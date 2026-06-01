"""
FastAPI trigger server.

Listens for start/stop commands from HMS.
Runs in a background thread alongside the main recorder process.

Endpoints:
  POST /record/start   { "surgery_id": "...", "scheduled_duration_minutes": 180 }
  POST /record/stop    { "surgery_id": "..." }
  GET  /health         returns current recorder state
"""

import logging
import threading
from typing import Callable, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

from .config import Config

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# Request / response models
# -------------------------------------------------------------------------

class StartRequest(BaseModel):
    surgery_id: str
    scheduled_duration_minutes: Optional[int] = None


class StopRequest(BaseModel):
    surgery_id: str


class StatusResponse(BaseModel):
    status: str
    surgery_id: Optional[str]
    ot_location_id: str
    ot_location_name: str


# -------------------------------------------------------------------------
# Server
# -------------------------------------------------------------------------

class TriggerServer:
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
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None
        self._app = self._build_app()

    def _build_app(self) -> FastAPI:
        app = FastAPI(title="OT Recorder Trigger API", docs_url=None, redoc_url=None)
        cfg = self.cfg

        def _verify_api_key(x_api_key: str = Header(...)):
            if x_api_key != cfg.hms_api_key:
                raise HTTPException(status_code=401, detail="Invalid API key")

        @app.get("/health")
        def health():
            state = self.get_status()
            return StatusResponse(
                status=state.get("status", "idle"),
                surgery_id=state.get("surgery_id"),
                ot_location_id=cfg.ot_location_id,
                ot_location_name=cfg.ot_location_name,
            )

        @app.post("/record/start", status_code=202)
        def start(req: StartRequest, x_api_key: str = Header(...)):
            _verify_api_key(x_api_key)
            state = self.get_status()
            if state.get("status") == "recording":
                raise HTTPException(
                    status_code=409,
                    detail=f"Already recording surgery {state.get('surgery_id')}",
                )
            logger.info(f"Start command received for surgery {req.surgery_id}")
            try:
                self.on_start(req.surgery_id, req.scheduled_duration_minutes)
            except Exception as e:
                logger.exception("Failed to start recording")
                raise HTTPException(status_code=500, detail=str(e))
            return {"message": "Recording started", "surgery_id": req.surgery_id}

        @app.post("/record/stop", status_code=202)
        def stop(req: StopRequest, x_api_key: str = Header(...)):
            _verify_api_key(x_api_key)
            state = self.get_status()
            if state.get("status") != "recording":
                raise HTTPException(status_code=409, detail="Not currently recording")
            if state.get("surgery_id") != req.surgery_id:
                raise HTTPException(
                    status_code=409,
                    detail=f"Recording in progress is for surgery {state.get('surgery_id')}, not {req.surgery_id}",
                )
            logger.info(f"Stop command received for surgery {req.surgery_id}")
            try:
                self.on_stop(req.surgery_id)
            except Exception as e:
                logger.exception("Failed to stop recording")
                raise HTTPException(status_code=500, detail=str(e))
            return {"message": "Recording stopping", "surgery_id": req.surgery_id}

        return app

    def start(self):
        server_config = uvicorn.Config(
            app=self._app,
            host="0.0.0.0",
            port=self.cfg.trigger_port,
            log_level="warning",  # uvicorn logs go to warning, our app logs separately
        )
        self._server = uvicorn.Server(server_config)
        self._thread = threading.Thread(
            target=self._server.run,
            name="trigger-server",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"Trigger server listening on port {self.cfg.trigger_port}")

    def stop(self):
        if self._server:
            self._server.should_exit = True
