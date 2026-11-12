"""SQLite connection helpers and schema management.

The "official" database (``portal.db``) holds raw logs, the pending-rebuild
queue, version counters and the published sessions. It runs in WAL mode so
that the rebuild worker's reads never block the API's writes, and writes
only happen inside short explicit transactions.
"""
from __future__ import annotations

import aiosqlite

from .config import DB_PATH

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS raw_logs (
    log_id      TEXT PRIMARY KEY,
    visitor_id  TEXT NOT NULL,
    ts          INTEGER NOT NULL,           -- UTC, whole seconds
    action      TEXT NOT NULL CHECK (action IN ('visit', 'exit')),
    page_path   TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_visitor_ts ON raw_logs(visitor_id, ts, log_id);

-- Monotonic per-visitor version. Bumped on every accepted correction / backfill
-- (i.e. whenever the raw input of that visitor actually changes).
CREATE TABLE IF NOT EXISTS visitor_versions (
    visitor_id TEXT PRIMARY KEY,
    version    INTEGER NOT NULL
);

-- Durable rebuild queue. A row means "the published sessions of this visitor
-- are stale relative to raw input version ``target_version``".
CREATE TABLE IF NOT EXISTS pending_rebuilds (
    visitor_id      TEXT PRIMARY KEY,
    target_version  INTEGER NOT NULL,
    requested_at    INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing'))
);

-- Published sessions, whole-visitor replacement per version. The newest two
-- versions per visitor are retained so each query can read the complete old
-- and new generation in one read-only transaction.
CREATE TABLE IF NOT EXISTS sessions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    visitor_id     TEXT NOT NULL,
    version        INTEGER NOT NULL,
    session_index  INTEGER NOT NULL,
    start_ts       INTEGER NOT NULL,
    end_ts         INTEGER NOT NULL,
    page_paths     TEXT NOT NULL,           -- JSON array, visit order
    UNIQUE (visitor_id, version, session_index)
);
CREATE INDEX IF NOT EXISTS idx_sessions_visitor_version
    ON sessions(visitor_id, version);

-- One row per published generation, including empty ones (a visitor whose
-- events were all corrected away still publishes a version with zero
-- sessions). Drives generation retention and "current version" answers.
CREATE TABLE IF NOT EXISTS published_generations (
    visitor_id    TEXT NOT NULL,
    version       INTEGER NOT NULL,
    published_at  INTEGER NOT NULL,
    session_count INTEGER NOT NULL,
    PRIMARY KEY (visitor_id, version)
);
"""


async def connect(db_path=DB_PATH) -> aiosqlite.Connection:
    """Open an autocommit connection; transactions are managed explicitly."""
    conn = await aiosqlite.connect(str(db_path), isolation_level=None)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA busy_timeout = 5000")
    await conn.execute("PRAGMA synchronous = NORMAL")
    return conn


async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.executescript(SCHEMA)
