"""
Local SQLite manifest.

Tracks every chunk from the moment ffmpeg writes it to the moment
it's confirmed uploaded to S3. Survives crashes — on restart the recorder
checks here for anything left unfinished.

Chunk states:
  pending   — file exists locally, upload not started
  uploading — upload in progress (if we crash here, treat as pending on restart)
  complete  — S3 confirmed, local file deleted
  failed    — permanent failure after all retries (needs manual intervention)
"""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Manifest:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    surgery_id      TEXT PRIMARY KEY,
                    ot_location_id  TEXT NOT NULL,
                    hospital_id     TEXT NOT NULL,
                    started_at      TEXT NOT NULL,
                    stopped_at      TEXT,
                    status          TEXT NOT NULL DEFAULT 'recording'
                );

                CREATE TABLE IF NOT EXISTS chunks (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    surgery_id          TEXT NOT NULL,
                    chunk_sequence      INTEGER NOT NULL,
                    local_path          TEXT NOT NULL,
                    s3_key              TEXT,
                    status              TEXT NOT NULL DEFAULT 'pending',
                    recorded_at         TEXT,
                    uploaded_at         TEXT,
                    retry_count         INTEGER NOT NULL DEFAULT 0,
                    error               TEXT,
                    UNIQUE(surgery_id, chunk_sequence),
                    FOREIGN KEY(surgery_id) REFERENCES sessions(surgery_id)
                );

                CREATE INDEX IF NOT EXISTS idx_chunks_status
                    ON chunks(status);
                CREATE INDEX IF NOT EXISTS idx_chunks_surgery
                    ON chunks(surgery_id);
            """)

    # -------------------------------------------------------------------------
    # Sessions
    # -------------------------------------------------------------------------

    def start_session(self, surgery_id: str, ot_location_id: str, hospital_id: str):
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO sessions (surgery_id, ot_location_id, hospital_id, started_at, status)
                VALUES (?, ?, ?, ?, 'recording')
                ON CONFLICT(surgery_id) DO UPDATE SET
                    status='recording', stopped_at=NULL
                """,
                (surgery_id, ot_location_id, hospital_id, _now()),
            )
        logger.info(f"Session started: {surgery_id}")

    def stop_session(self, surgery_id: str):
        with self._conn() as conn:
            conn.execute(
                "UPDATE sessions SET stopped_at=?, status='stopped' WHERE surgery_id=?",
                (_now(), surgery_id),
            )
        logger.info(f"Session stopped: {surgery_id}")

    def get_active_session(self) -> Optional[dict]:
        """Returns the active recording session if one exists — used on crash recovery."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM sessions WHERE status='recording' ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    # -------------------------------------------------------------------------
    # Chunks
    # -------------------------------------------------------------------------

    def register_chunk(self, surgery_id: str, chunk_sequence: int, local_path: str, recorded_at: str):
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO chunks (surgery_id, chunk_sequence, local_path, recorded_at, status)
                VALUES (?, ?, ?, ?, 'pending')
                ON CONFLICT(surgery_id, chunk_sequence) DO NOTHING
                """,
                (surgery_id, chunk_sequence, local_path, recorded_at),
            )

    def mark_uploading(self, chunk_id: int):
        with self._conn() as conn:
            conn.execute(
                "UPDATE chunks SET status='uploading' WHERE id=?", (chunk_id,)
            )

    def mark_complete(self, chunk_id: int, s3_key: str):
        with self._conn() as conn:
            conn.execute(
                "UPDATE chunks SET status='complete', s3_key=?, uploaded_at=? WHERE id=?",
                (s3_key, _now(), chunk_id),
            )

    def migrate_legacy_uploaded(self) -> list[str]:
        """Upgrade legacy 'uploaded' rows to complete and return local paths to clean up."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT local_path FROM chunks WHERE status='uploaded'"
            ).fetchall()
            paths = [row["local_path"] for row in rows]
            if paths:
                conn.execute(
                    """
                    UPDATE chunks
                    SET status='complete', uploaded_at=COALESCE(uploaded_at, ?)
                    WHERE status='uploaded'
                    """,
                    (_now(),),
                )
                logger.info(f"Migrated {len(paths)} legacy uploaded chunks to complete")
            return paths

    def mark_failed(self, chunk_id: int, error: str):
        with self._conn() as conn:
            conn.execute(
                "UPDATE chunks SET status='failed', error=? WHERE id=?",
                (error, chunk_id),
            )

    def increment_retry(self, chunk_id: int):
        with self._conn() as conn:
            conn.execute(
                "UPDATE chunks SET retry_count = retry_count + 1 WHERE id=?",
                (chunk_id,),
            )

    def get_pending_chunks(self) -> list[dict]:
        """
        Returns chunks that need uploading.
        Treats 'uploading' as pending too — if we were mid-upload when we
        crashed, we restart the upload.
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM chunks
                WHERE status IN ('pending', 'uploading')
                ORDER BY chunk_sequence ASC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def get_chunks_for_surgery(self, surgery_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM chunks WHERE surgery_id=? ORDER BY chunk_sequence ASC",
                (surgery_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def has_incomplete_uploads(self, surgery_id: str) -> bool:
        with self._conn() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM chunks WHERE surgery_id=? AND status NOT IN ('complete', 'failed')",
                (surgery_id,),
            ).fetchone()[0]
            return count > 0
