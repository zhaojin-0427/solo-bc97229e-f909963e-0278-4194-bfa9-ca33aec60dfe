"""Background session-rebuild worker.

Concurrency / consistency design
--------------------------------

* Rebuilding happens in this separate process (the ``worker`` container) and
  uses a throwaway SQLite database inside ``WORK_DIR``. The official database
  is only touched by:
    1. short read-only transactions (load one visitor's raw logs),
    2. one short write transaction when publishing a finished generation.
  So a long rebuild never holds a write transaction on the official DB; log
  ingestion and session reads continue uninterrupted.
* Every accepted correction bumps a per-visitor version and the pending row
  stores the ``target_version`` it was queued for. Publishing uses
  compare-and-swap on that version: if newer input arrived while the rebuild
  was running, the result is discarded and the visitor is re-queued. A late
  computation therefore can never overwrite a newer result.
* Pending rows and raw logs are durable. On restart the worker simply
  re-claims whatever is still pending; stale work files in WORK_DIR are
  removed at startup.
"""
from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiosqlite

from .config import DB_PATH, POLL_INTERVAL_SECONDS, WORK_DIR, ensure_dirs
from .db import connect, init_db
from .sessions import reconstruct_sessions


class VersionMismatchError(Exception):
    """The rebuild result is stale: the visitor got newer input meanwhile."""


class RebuildWorker:
    def __init__(self, db_path: Path = DB_PATH, work_dir: Path = WORK_DIR) -> None:
        self.db_path = db_path
        self.work_dir = work_dir
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._conn: aiosqlite.Connection | None = None

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        ensure_dirs()
        self._cleanup_stale_work_files()
        self._conn = await connect(self.db_path)
        await init_db(self._conn)
        # A crash between claiming and publishing would leave rows locked in
        # 'processing'; reclaim them on startup.
        await self._conn.execute(
            "UPDATE pending_rebuilds SET status = 'pending' WHERE status = 'processing'"
        )
        self._task = asyncio.create_task(self._run(), name="rebuild-worker")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except asyncio.TimeoutError:
                self._task.cancel()
        if self._conn is not None:
            await self._conn.close()

    def _cleanup_stale_work_files(self) -> None:
        for path in self.work_dir.glob("rebuild-*.db*"):
            try:
                path.unlink()
            except OSError:
                pass

    async def _run(self) -> None:
        assert self._conn is not None
        conn = self._conn
        while not self._stop.is_set():
            try:
                row = await conn.execute_fetchall(
                    "SELECT visitor_id, target_version FROM pending_rebuilds "
                    "WHERE status = 'pending' ORDER BY requested_at, visitor_id LIMIT 1"
                )
                if not row:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=POLL_INTERVAL_SECONDS)
                    except asyncio.TimeoutError:
                        pass
                    continue
                visitor_id, target_version = row[0][0], row[0][1]
                await self._process_one(visitor_id, target_version)
            except asyncio.CancelledError:
                raise
            except Exception:  # keep the loop alive; the row stays pending
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

    # ------------------------------------------------------------------ one job
    async def _process_one(self, visitor_id: str, target_version: int) -> None:
        assert self._conn is not None
        conn = self._conn

        # 1. Claim the job (single worker per queue row).
        claimed = await conn.execute(
            "UPDATE pending_rebuilds SET status = 'processing' "
            "WHERE visitor_id = ? AND status = 'pending'",
            (visitor_id,),
        )
        if claimed.rowcount == 0:
            return

        work_path = self.work_dir / f"rebuild-{visitor_id}-{target_version}-{time.time_ns()}.db"
        try:
            # 2. Read this visitor's raw input in a read-only transaction and
            #    stage it into the isolated work database.
            async with self._read_tx(conn) as ro:
                events = await ro.execute_fetchall(
                    "SELECT log_id, visitor_id, ts, action, page_path "
                    "FROM raw_logs WHERE visitor_id = ?",
                    (visitor_id,),
                )
                current_version = await ro.execute_fetchall(
                    "SELECT version FROM visitor_versions WHERE visitor_id = ?",
                    (visitor_id,),
                )
            current_version = current_version[0][0] if current_version else 0
            if current_version != target_version:
                # Newer corrections already landed; re-queue at the new version.
                await self._requeue(conn, visitor_id, current_version)
                return

            await self._stage_to_work_db(work_path, events)

            # 3. The (potentially heavy) computation touches only the work DB.
            sessions = await asyncio.to_thread(self._compute, work_path)

            # 4. Publish with compare-and-swap inside one short transaction.
            await self._publish(conn, visitor_id, target_version, sessions)
        except VersionMismatchError:
            latest = await conn.execute_fetchall(
                "SELECT version FROM visitor_versions WHERE visitor_id = ?",
                (visitor_id,),
            )
            await self._requeue(conn, visitor_id, latest[0][0] if latest else target_version)
        except Exception:
            # Unexpected failure: release the claim so the job is retried
            # instead of staying locked in 'processing' until restart.
            await conn.execute(
                "UPDATE pending_rebuilds SET status = 'pending' "
                "WHERE visitor_id = ? AND status = 'processing'",
                (visitor_id,),
            )
            raise
        finally:
            for suffix in ("", "-wal", "-shm"):
                p = Path(str(work_path) + suffix)
                try:
                    p.unlink()
                except OSError:
                    pass

    @asynccontextmanager
    async def _read_tx(self, conn: aiosqlite.Connection):
        await conn.execute("BEGIN")
        try:
            yield conn
        except Exception:
            await conn.execute("ROLLBACK")
            raise
        else:
            await conn.execute("COMMIT")

    async def _stage_to_work_db(
        self, work_path: Path, events: list[aiosqlite.Row]
    ) -> None:
        wconn = await aiosqlite.connect(str(work_path), isolation_level=None)
        try:
            await wconn.executescript(
                """
                CREATE TABLE raw_events (
                    log_id TEXT, ts INTEGER, action TEXT, page_path TEXT
                );
                """
            )
            await wconn.execute("BEGIN")
            await wconn.executemany(
                "INSERT INTO raw_events(log_id, ts, action, page_path) VALUES (?, ?, ?, ?)",
                [
                    (e["log_id"], e["ts"], e["action"], e["page_path"])
                    for e in events
                ],
            )
            await wconn.execute("COMMIT")
        finally:
            await wconn.close()

    @staticmethod
    def _compute(work_path: Path) -> list[dict[str, Any]]:
        # Runs in a worker thread; opens the work DB read-only.
        import sqlite3

        conn = sqlite3.connect(f"file:{work_path}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "SELECT log_id, ts, action, page_path FROM raw_events"
            ).fetchall()
        finally:
            conn.close()
        events = [
            {"log_id": r[0], "ts": r[1], "action": r[2], "page_path": r[3]}
            for r in rows
        ]
        return reconstruct_sessions(events)

    async def _publish(
        self,
        conn: aiosqlite.Connection,
        visitor_id: str,
        target_version: int,
        new_sessions: list[dict[str, Any]],
    ) -> None:
        """Atomically swap in the new generation, keeping the newest two."""
        await conn.execute("BEGIN IMMEDIATE")
        try:
            row = await conn.execute_fetchall(
                "SELECT version FROM visitor_versions WHERE visitor_id = ?",
                (visitor_id,),
            )
            current_version = row[0][0] if row else 0
            if current_version != target_version:
                raise VersionMismatchError()

            await conn.execute(
                "DELETE FROM pending_rebuilds WHERE visitor_id = ?", (visitor_id,)
            )
            await conn.executemany(
                "INSERT INTO sessions(visitor_id, version, session_index, "
                "start_ts, end_ts, page_paths) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        visitor_id,
                        target_version,
                        idx,
                        s["start_ts"],
                        s["end_ts"],
                        json.dumps(s["page_paths"], ensure_ascii=False),
                    )
                    for idx, s in enumerate(new_sessions)
                ],
            )
            await conn.execute(
                "INSERT INTO published_generations(visitor_id, version, "
                "published_at, session_count) VALUES (?, ?, ?, ?)",
                (visitor_id, target_version, int(time.time()), len(new_sessions)),
            )
            # Retain only the two newest generations for this visitor.
            await conn.execute(
                """
                DELETE FROM published_generations
                 WHERE visitor_id = ?
                   AND version NOT IN (
                        SELECT version FROM published_generations
                         WHERE visitor_id = ?
                         ORDER BY version DESC LIMIT 2
                   )
                """,
                (visitor_id, visitor_id),
            )
            await conn.execute(
                """
                DELETE FROM sessions
                 WHERE visitor_id = ?
                   AND version NOT IN (
                        SELECT version FROM published_generations
                         WHERE visitor_id = ?
                   )
                """,
                (visitor_id, visitor_id),
            )
            await conn.execute("COMMIT")
        except Exception:
            await conn.execute("ROLLBACK")
            raise

    async def _requeue(
        self, conn: aiosqlite.Connection, visitor_id: str, new_version: int
    ) -> None:
        # Single autocommit upsert: atomic, and avoids taking a second
        # immediate lock while another transaction may be queued.
        await conn.execute(
            "INSERT INTO pending_rebuilds(visitor_id, target_version, "
            "requested_at, status) VALUES (?, ?, ?, 'pending') "
            "ON CONFLICT(visitor_id) DO UPDATE SET "
            "target_version = excluded.target_version, "
            "requested_at = excluded.requested_at, status = 'pending'",
            (visitor_id, new_version, int(time.time())),
        )
