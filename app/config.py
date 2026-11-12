"""Runtime configuration, overridable via environment variables."""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data")).resolve()
DB_PATH = Path(os.getenv("DB_PATH", str(DATA_DIR / "portal.db"))).resolve()
WORK_DIR = Path(os.getenv("WORK_DIR", str(DATA_DIR / "work"))).resolve()

# How long the rebuild worker waits between polling the pending queue.
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.5"))
# When true the API process also runs a rebuild worker in-process.
# The docker-compose deployment runs the worker as a separate container;
# the flag exists so a single-process demo/test run is also possible.
RUN_WORKER_IN_PROCESS = os.getenv("RUN_WORKER_IN_PROCESS", "false").lower() == "true"

# A session is broken when two consecutive events are more than this far apart,
# or an exit event is encountered.
SESSION_GAP_SECONDS = 30 * 60


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
