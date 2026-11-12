"""End-to-end tests: API + in-process worker against isolated SQLite files."""
from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from app import config
from app.main import create_app

T0 = 1_731_400_000


@pytest.fixture()
def env(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    work_dir = data_dir / "work"
    data_dir.mkdir()
    monkeypatch.setattr(config, "DATA_DIR", data_dir.resolve())
    monkeypatch.setattr(config, "DB_PATH", (data_dir / "portal.db").resolve())
    monkeypatch.setattr(config, "WORK_DIR", work_dir.resolve())
    monkeypatch.setattr(config, "POLL_INTERVAL_SECONDS", 0.05)
    return data_dir


@pytest.fixture()
def client(env, monkeypatch):
    # The worker runs as a task inside the application's own event loop.
    monkeypatch.setattr(config, "RUN_WORKER_IN_PROCESS", True)
    app = create_app()
    with TestClient(app) as c:
        yield c


def _ingest(c, logs):
    return c.post("/logs", json={"logs": logs}).raise_for_status().json()


def _wait_idle(c, visitor_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = c.get(f"/visitors/{visitor_id}/sessions").raise_for_status().json()
        if not resp["pending_rebuild"]:
            return resp
        time.sleep(0.05)
    raise AssertionError("rebuild did not finish in time")


def test_ingest_rebuild_and_query_flow(client):
    r = _ingest(client, [
        {"log_id": "l1", "visitor_id": "v1", "ts": T0, "action": "visit", "page_path": "/home"},
        {"log_id": "l2", "visitor_id": "v1", "ts": T0 + 60, "action": "visit", "page_path": "/news"},
        {"log_id": "l3", "visitor_id": "v1", "ts": T0 + 120, "action": "exit"},
        {"log_id": "l4", "visitor_id": "v1", "ts": T0 + 3000, "action": "visit", "page_path": "/later"},
    ])
    assert r["inserted"] == 4
    assert r["affected_visitors"] == ["v1"]

    # Immediately after ingestion the query must still work and flag pending.
    quick = client.get("/visitors/v1/sessions").json()
    assert quick["pending_rebuild"] is True
    assert quick["generations"] == []  # nothing published yet, reads still work

    result = _wait_idle(client, "v1")
    assert result["pending_rebuild"] is False
    assert result["current_version"] == 1
    gens = result["generations"]
    assert len(gens) == 1
    sessions = gens[0]["sessions"]
    assert len(sessions) == 2
    assert sessions[0]["page_paths"] == ["/home", "/news"]  # exit excluded
    assert sessions[1]["page_paths"] == ["/later"]
    assert sessions[0]["start_time"].endswith("Z")


def test_correction_rebuilds_and_keeps_two_generations(client):
    _ingest(client, [
        {"log_id": "l1", "visitor_id": "v1", "ts": T0, "action": "visit", "page_path": "/a"},
        {"log_id": "l2", "visitor_id": "v1", "ts": T0 + 10, "action": "visit", "page_path": "/b"},
    ])
    _wait_idle(client, "v1")

    # Correct l1: same visitor, different time -> splits into two sessions.
    r = _ingest(client, [
        {"log_id": "l1", "visitor_id": "v1", "ts": T0 + 5000, "action": "visit", "page_path": "/a"}
    ])
    assert r["corrected"] == 1
    result = _wait_idle(client, "v1")
    assert result["current_version"] == 2
    assert len(result["generations"]) == 2  # old + new generation retained
    new_gen, old_gen = result["generations"]
    assert len(new_gen["sessions"]) == 2
    assert len(old_gen["sessions"]) == 1

    # A third generation pushes the oldest one out (only two kept).
    _ingest(client, [
        {"log_id": "l2", "visitor_id": "v1", "ts": T0 + 5000 + 5, "action": "visit", "page_path": "/b"}
    ])
    result = _wait_idle(client, "v1")
    assert result["current_version"] == 3
    assert [g["version"] for g in result["generations"]] == [3, 2]


def test_reassigning_visitor_fixes_both_sides(client):
    _ingest(client, [
        {"log_id": "l1", "visitor_id": "v1", "ts": T0, "action": "visit", "page_path": "/a"},
        {"log_id": "l2", "visitor_id": "v2", "ts": T0, "action": "visit", "page_path": "/x"},
    ])
    _wait_idle(client, "v1")
    _wait_idle(client, "v2")

    r = _ingest(client, [
        {"log_id": "l1", "visitor_id": "v2", "ts": T0, "action": "visit", "page_path": "/a"}
    ])
    assert r["corrected"] == 1
    assert sorted(r["affected_visitors"]) == ["v1", "v2"]

    v1 = _wait_idle(client, "v1")
    v2 = _wait_idle(client, "v2")
    # v1 lost its only event -> empty session set, but version still published.
    assert v1["generations"][0]["version"] == 2
    assert v1["generations"][0]["sessions"] == []
    # v2 now contains both events in visit order (log_id tiebreak).
    assert v2["generations"][0]["sessions"][0]["page_paths"] == ["/a", "/x"]


def test_unchanged_resubmit_does_not_bump_version(client):
    entry = {"log_id": "l1", "visitor_id": "v1", "ts": T0, "action": "visit", "page_path": "/a"}
    _ingest(client, [entry])
    _wait_idle(client, "v1")
    r = _ingest(client, [dict(entry)])
    assert r["unchanged"] == 1
    assert r["affected_visitors"] == []
    assert r["pending_rebuild"] is False
    result = client.get("/visitors/v1/sessions").json()
    assert result["current_version"] == 1


def test_validation_rules(client):
    bad_visit = client.post("/logs", json={"logs": [
        {"log_id": "x", "visitor_id": "v", "ts": T0, "action": "visit"}
    ]})
    assert bad_visit.status_code == 422

    bad_exit = client.post("/logs", json={"logs": [
        {"log_id": "x", "visitor_id": "v", "ts": T0, "action": "exit", "page_path": "/p"}
    ]})
    assert bad_exit.status_code == 422

    bad_ts = client.post("/logs", json={"logs": [
        {"log_id": "x", "visitor_id": "v", "ts": -1, "action": "visit", "page_path": "/p"}
    ]})
    assert bad_ts.status_code == 422


def test_pending_state_and_raw_logs_survive_restart(env):
    # Seed a durable queue without any worker running, then start the app.
    conn = sqlite3.connect(str(config.DB_PATH))
    conn.executescript(
        """
        CREATE TABLE raw_logs (
            log_id TEXT PRIMARY KEY, visitor_id TEXT NOT NULL, ts INTEGER NOT NULL,
            action TEXT NOT NULL, page_path TEXT
        );
        CREATE TABLE visitor_versions (visitor_id TEXT PRIMARY KEY, version INTEGER NOT NULL);
        CREATE TABLE pending_rebuilds (
            visitor_id TEXT PRIMARY KEY, target_version INTEGER NOT NULL,
            requested_at INTEGER NOT NULL, status TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO raw_logs VALUES ('l1', 'v9', ?, 'visit', '/a')", (T0,)
    )
    conn.execute("INSERT INTO visitor_versions VALUES ('v9', 1)")
    conn.execute("INSERT INTO pending_rebuilds VALUES ('v9', 1, ?, 'pending')", (T0,))
    conn.commit()
    conn.close()

    config.RUN_WORKER_IN_PROCESS = True
    try:
        with TestClient(create_app()) as c:
            result = _wait_idle(c, "v9")
            assert result["current_version"] == 1
            assert result["generations"][0]["sessions"][0]["page_paths"] == ["/a"]
    finally:
        config.RUN_WORKER_IN_PROCESS = False


def test_stale_rebuild_cannot_overwrite_newer_result(client, monkeypatch):
    _ingest(client, [
        {"log_id": "l1", "visitor_id": "v1", "ts": T0, "action": "visit", "page_path": "/a"}
    ])
    _wait_idle(client, "v1")

    # Slow the compute down so corrections land while a rebuild is running.
    from app import worker as worker_mod

    original = worker_mod.RebuildWorker._compute

    def slow_compute(self, work_path):
        time.sleep(0.4)
        return original(self, work_path)

    monkeypatch.setattr(worker_mod.RebuildWorker, "_compute", slow_compute)

    # Queue a rebuild at v2, then correct twice more before it finishes, so
    # the in-flight computation is guaranteed stale.
    client.post("/admin/visitors/v1/rebuild").raise_for_status()
    _ingest(client, [
        {"log_id": "l1", "visitor_id": "v1", "ts": T0, "action": "visit", "page_path": "/b"}
    ])
    _ingest(client, [
        {"log_id": "l2", "visitor_id": "v1", "ts": T0 + 10, "action": "visit", "page_path": "/c"}
    ])

    result = _wait_idle(client, "v1", timeout=15)
    assert result["current_version"] == 3
    latest = result["generations"][0]
    # The published generation must reflect the newest input, never the stale
    # intermediate computation.
    assert latest["sessions"][0]["page_paths"] == ["/b", "/c"]
