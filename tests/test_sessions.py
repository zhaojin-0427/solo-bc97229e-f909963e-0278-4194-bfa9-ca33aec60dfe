"""Unit tests for the pure session-reconstruction rules."""
from __future__ import annotations

from app.sessions import reconstruct_sessions

T0 = 1_731_400_000  # arbitrary UTC epoch second


def _evt(log_id: str, ts: int, action: str, page_path: str | None = None):
    return {"log_id": log_id, "ts": ts, "action": action, "page_path": page_path}


def test_single_visit_session_keeps_order_and_repeats():
    events = [
        _evt("a", T0, "visit", "/a"),
        _evt("b", T0 + 10, "visit", "/b"),
        _evt("c", T0 + 20, "visit", "/a"),  # repeated path retained
    ]
    sessions = reconstruct_sessions(events)
    assert len(sessions) == 1
    assert sessions[0]["start_ts"] == T0
    assert sessions[0]["end_ts"] == T0 + 20
    assert sessions[0]["page_paths"] == ["/a", "/b", "/a"]


def test_gap_greater_than_30_minutes_splits_sessions():
    events = [
        _evt("a", T0, "visit", "/a"),
        _evt("b", T0 + 1800, "visit", "/b"),  # exactly 30 min -> same session
        _evt("c", T0 + 1800 + 1801, "visit", "/c"),  # >30 min -> new session
    ]
    sessions = reconstruct_sessions(events)
    assert len(sessions) == 2
    assert sessions[0]["page_paths"] == ["/a", "/b"]
    assert sessions[1]["page_paths"] == ["/c"]
    assert sessions[0]["end_ts"] == T0 + 1800
    assert sessions[1]["start_ts"] == T0 + 3601


def test_exit_closes_session_and_is_not_a_page():
    events = [
        _evt("a", T0, "visit", "/a"),
        _evt("b", T0 + 5, "visit", "/b"),
        _evt("x", T0 + 9, "exit"),
        _evt("c", T0 + 12, "visit", "/c"),
    ]
    sessions = reconstruct_sessions(events)
    assert len(sessions) == 2
    assert sessions[0]["page_paths"] == ["/a", "/b"]
    assert sessions[0]["end_ts"] == T0 + 5
    assert sessions[1]["page_paths"] == ["/c"]


def test_lone_exit_produces_no_session():
    assert reconstruct_sessions([_evt("x", T0, "exit")]) == []
    # exit followed, within 30 min, by a visit: only one session, no exit page
    sessions = reconstruct_sessions(
        [_evt("x", T0, "exit"), _evt("a", T0 + 3, "visit", "/a")]
    )
    assert sessions == [
        {"start_ts": T0 + 3, "end_ts": T0 + 3, "page_paths": ["/a"]}
    ]


def test_sorting_by_ts_then_log_id_merges_interleaved_tabs():
    events = [
        _evt("z", T0 + 30, "visit", "/tab2"),
        _evt("a", T0 + 30, "visit", "/tab1"),  # same ts -> log_id tiebreak
        _evt("m", T0 + 10, "visit", "/home"),
    ]
    sessions = reconstruct_sessions(events)
    assert sessions[0]["page_paths"] == ["/home", "/tab1", "/tab2"]


def test_exit_then_gap_then_visit():
    events = [
        _evt("a", T0, "visit", "/a"),
        _evt("x", T0 + 5, "exit"),
        _evt("b", T0 + 4000, "visit", "/b"),  # both exit and gap apply
    ]
    sessions = reconstruct_sessions(events)
    assert len(sessions) == 2
    assert [s["page_paths"] for s in sessions] == [["/a"], ["/b"]]
