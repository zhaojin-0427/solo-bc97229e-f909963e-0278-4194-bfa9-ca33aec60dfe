"""Pure session-reconstruction logic.

Rules (from the data-engineering spec):

* Events of a visitor are ordered by ``(ts, log_id)``.
* Two consecutive events more than ``SESSION_GAP_SECONDS`` (30 minutes) apart
  break the session.
* An ``exit`` event also breaks the session and is never included in the page
  sequence (exits carry no page path).
* Interleaved multi-tab visits are simply merged by the same ordering rule;
  repeated visits to the same path are retained.
* A session containing no visit (e.g. a lone exit) is not emitted.
"""
from __future__ import annotations

from typing import Any, Iterable

from .config import SESSION_GAP_SECONDS


def reconstruct_sessions(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build ordered sessions from raw event dicts.

    Each event needs ``ts`` (int), ``action`` ('visit'/'exit') and
    ``page_path`` (str or None).
    """
    ordered = sorted(events, key=lambda e: (e["ts"], e["log_id"]))

    sessions: list[dict[str, Any]] = []
    current_pages: list[str] = []
    current_start: int | None = None
    current_end: int | None = None
    last_ts: int | None = None

    def flush() -> None:
        # Empty sessions (no visit) are dropped.
        if current_pages:
            sessions.append(
                {
                    "start_ts": current_start,
                    "end_ts": current_end,
                    "page_paths": list(current_pages),
                }
            )

    for event in ordered:
        ts = int(event["ts"])
        gap_break = last_ts is not None and (ts - last_ts) > SESSION_GAP_SECONDS

        if gap_break:
            flush()
            current_pages = []
            current_start = None
            current_end = None

        if event["action"] == "exit":
            # Exit closes the current session but is not part of the sequence.
            flush()
            current_pages = []
            current_start = None
            current_end = None
            last_ts = ts
            continue

        # visit
        if current_start is None:
            current_start = ts
        current_pages.append(event["page_path"])
        current_end = ts
        last_ts = ts

    flush()
    return sessions
