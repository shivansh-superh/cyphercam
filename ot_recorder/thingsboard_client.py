"""
ThingsBoard MQTT client.

Connects outbound to ThingsBoard and handles server-side RPC:
  startRecording  { "surgery_id": "...", "scheduled_duration_minutes": 180 }
                  (alias: duration_minutes)
  stopRecording   { "surgery_id": "..." }

Publishes recorder status as device telemetry on connect and after each RPC.
"""

import json
import logging
import ssl
import threading
import time
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from .config import Config

logger = logging.getLogger(__name__)

RPC_SUBSCRIBE = "v1/devices/me/rpc/request/+"
RPC_RESPONSE_PREFIX = "v1/devices/me/rpc/response/"
TELEMETRY_TOPIC = "v1/devices/me/telemetry"

METHOD_START = "startRecording"
METHOD_STOP = "stopRecording"

RECONNECT_BASE_DELAY = 2
RECONNECT_MAX_DELAY = 60
CONNECT_TIMEOUT_SEC = 30


def _mqtt_failed(reason_code) -> bool:
    if reason_code is None:
        return False
    if hasattr(reason_code, "is_failure"):
        return reason_code.is_failure
    return int(reason_code) != 0


class ThingsBoardClient:
    def __init__(
        self,
        cfg: Config,
        on_start: Callable[[str, Optional[int]], None],
        on_stop: Callable[[str], None],
        get_status: Callable[[], dict],
    ):
        self.cfg = cfg
        self.on_start = on_start
        self.on_stop = on_stop
        self.get_status = get_status
        self._client: Optional[mqtt.Client] = None
        self._stop_event = threading.Event()
        self._connected = threading.Event()
        self._session_lost = threading.Event()

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="thingsboard-client",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"ThingsBoard client starting — {self.cfg.tb_host}:{self.cfg.tb_mqtt_port} "
            f"(tls={self.cfg.tb_mqtt_use_tls})"
        )

    def publish_status(self):
        """Push current recorder state to ThingsBoard telemetry (best-effort)."""
        self._publish_telemetry()

    def stop(self):
        self._stop_event.set()
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
        if hasattr(self, "_thread") and self._thread.is_alive():
            self._thread.join(timeout=5)

    def _run_loop(self):
        delay = RECONNECT_BASE_DELAY
        while not self._stop_event.is_set():
            try:
                self._connect_and_loop()
                delay = RECONNECT_BASE_DELAY
            except Exception:
                logger.exception("ThingsBoard MQTT session ended")
            if self._stop_event.is_set():
                break
            logger.info(f"Reconnecting to ThingsBoard in {delay}s...")
            if self._stop_event.wait(delay):
                break
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    def _configure_tls(self, client: mqtt.Client) -> None:
        ca_file = self.cfg.tb_mqtt_ca_file
        if not ca_file:
            try:
                import certifi

                ca_file = certifi.where()
            except ImportError:
                ca_file = None
        if ca_file:
            client.tls_set(ca_certs=ca_file, cert_reqs=ssl.CERT_REQUIRED)
        else:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    def _connect_and_loop(self):
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"ot-recorder-{self.cfg.ot_location_id}",
        )
        if self.cfg.tb_mqtt_use_tls:
            self._configure_tls(client)
        client.username_pw_set(self.cfg.tb_access_token)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect
        self._client = client
        self._connected.clear()
        self._session_lost.clear()
        self._connect_outcome = threading.Event()
        self._connect_error: str | None = None

        client.connect(self.cfg.tb_host, self.cfg.tb_mqtt_port, keepalive=60)
        client.loop_start()

        if not self._connect_outcome.wait(timeout=CONNECT_TIMEOUT_SEC):
            raise TimeoutError(
                f"ThingsBoard MQTT connect timed out after {CONNECT_TIMEOUT_SEC}s"
            )
        if self._connect_error:
            raise ConnectionError(self._connect_error)

        # paho-mqtt 2.x client.is_connected() is unreliable here; wait for on_disconnect.
        while not self._stop_event.is_set():
            if self._session_lost.wait(timeout=1.0):
                break

        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        try:
            if _mqtt_failed(reason_code):
                self._connect_error = f"CONNACK failed: {reason_code}"
                logger.error(f"ThingsBoard MQTT connect failed: {reason_code}")
                return
            self._connect_error = None
            logger.info("Connected to ThingsBoard")
            client.subscribe(RPC_SUBSCRIBE, qos=1)
            self._connected.set()
            self._publish_telemetry()
        finally:
            if hasattr(self, "_connect_outcome"):
                self._connect_outcome.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self._connected.clear()
        self._session_lost.set()
        if _mqtt_failed(reason_code) and not self._stop_event.is_set():
            logger.warning(f"Disconnected from ThingsBoard (rc={reason_code})")

    def _on_message(self, client, userdata, msg):
        if not msg.topic.startswith("v1/devices/me/rpc/request/"):
            return
        request_id = msg.topic.rsplit("/", 1)[-1]
        try:
            body = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Invalid RPC payload on {msg.topic}: {e}")
            self._rpc_response(request_id, {"ok": False, "error": "Invalid JSON payload"})
            return

        method = body.get("method")
        params = _parse_params(body.get("params"))
        logger.info(f"RPC {request_id}: method={method!r}")

        try:
            result = self._dispatch_rpc(method, params)
        except Exception as e:
            logger.exception(f"RPC {request_id} failed")
            result = {"ok": False, "error": str(e)}

        self._rpc_response(request_id, result)
        self._publish_telemetry()

    def _dispatch_rpc(self, method: Optional[str], params: dict) -> dict:
        if method == METHOD_START:
            return self._handle_start(params)
        if method == METHOD_STOP:
            return self._handle_stop(params)
        return {"ok": False, "error": f"Unknown method: {method!r}"}

    def _handle_start(self, params: dict) -> dict:
        surgery_id = params.get("surgery_id")
        if not surgery_id or not isinstance(surgery_id, str):
            return {"ok": False, "error": "surgery_id is required"}

        duration = params.get("scheduled_duration_minutes")
        if duration is None:
            duration = params.get("duration_minutes")
        if duration is not None:
            try:
                duration = int(duration)
            except (TypeError, ValueError):
                return {"ok": False, "error": "scheduled_duration_minutes must be an integer"}

        state = self.get_status()
        if state.get("status") == "recording":
            return {
                "ok": False,
                "error": f"Already recording surgery {state.get('surgery_id')}",
            }

        logger.info(f"Start RPC for surgery {surgery_id}")
        self.on_start(surgery_id, duration)
        return {
            "ok": True,
            "message": "Recording started",
            "surgery_id": surgery_id,
        }

    def _handle_stop(self, params: dict) -> dict:
        surgery_id = params.get("surgery_id")
        if not surgery_id or not isinstance(surgery_id, str):
            return {"ok": False, "error": "surgery_id is required"}

        state = self.get_status()
        if state.get("status") != "recording":
            return {"ok": False, "error": "Not currently recording"}
        if state.get("surgery_id") != surgery_id:
            return {
                "ok": False,
                "error": (
                    f"Recording in progress is for surgery {state.get('surgery_id')}, "
                    f"not {surgery_id}"
                ),
            }

        logger.info(f"Stop RPC for surgery {surgery_id}")
        self.on_stop(surgery_id)
        return {
            "ok": True,
            "message": "Recording stopped",
            "surgery_id": surgery_id,
        }

    def _rpc_response(self, request_id: str, payload: dict):
        if not self._client:
            return
        topic = f"{RPC_RESPONSE_PREFIX}{request_id}"
        self._client.publish(topic, json.dumps(payload), qos=1)

    def _publish_telemetry(self):
        if not self._client or not self._client.is_connected():
            return
        state = self.get_status()
        telemetry = {
            "status": state.get("status", "idle"),
            "surgery_id": state.get("surgery_id") or "",
            "ot_location_id": self.cfg.ot_location_id,
            "ot_location_name": self.cfg.ot_location_name,
        }
        self._client.publish(TELEMETRY_TOPIC, json.dumps(telemetry), qos=1)
        logger.debug(f"Telemetry published: {telemetry}")


def _parse_params(params: Any) -> dict:
    if params is None:
        return {}
    if isinstance(params, dict):
        return params
    if isinstance(params, str):
        if not params.strip():
            return {}
        parsed = json.loads(params)
        if not isinstance(parsed, dict):
            raise ValueError("RPC params must be a JSON object")
        return parsed
    raise ValueError("RPC params must be a JSON object or string")
