"""End-to-end demo against a running service (stdlib only).

Walks through: batch submission, correction within the same visitor,
cross-visitor reassignment (both sides fixed), and querying the newest two
session generations with the pending-rebuild flag.

Usage:
    python scripts/demo.py                 # default http://localhost:8000
    BASE_URL=http://host:8000 python scripts/demo.py
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

BASE = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")
T0 = 1_731_400_000  # fixed UTC epoch second so output is reproducible


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} on {method} {path}: {exc.read().decode()}")


def wait_published(visitor_id: str) -> dict:
    deadline = time.time() + 15
    while time.time() < deadline:
        result = call("GET", f"/visitors/{visitor_id}/sessions")
        if not result["pending_rebuild"]:
            return result
        time.sleep(0.2)
    raise SystemExit(f"rebuild for {visitor_id} never finished")


def show_sessions(title: str, result: dict) -> None:
    print(f"\n=== {title} ===")
    print(f"visitor={result['visitor_id']} pending={result['pending_rebuild']} "
          f"input_version={result['input_version']} current={result['current_version']}")
    for gen in result["generations"]:
        print(f"  generation v{gen['version']}: {len(gen['sessions'])} session(s)")
        for s in gen["sessions"]:
            print(f"    #{s['session_index']} {s['start_time']} -> {s['end_time']} "
                  f"pages={s['page_paths']}")
    if not result["generations"]:
        print("  (no published generation yet)")


def main() -> None:
    print(f"target: {BASE}")
    print("health:", call("GET", "/health"))

    # 1. Batch submission: a tab-interleaved visit, an exit, and a later visit.
    batch = [
        {"log_id": "evt-01", "visitor_id": "visitor-7", "ts": T0,
         "action": "visit", "page_path": "/home"},
        {"log_id": "evt-02", "visitor_id": "visitor-7", "ts": T0 + 20,
         "action": "visit", "page_path": "/products/42"},
        {"log_id": "evt-03", "visitor_id": "visitor-7", "ts": T0 + 40,
         "action": "visit", "page_path": "/products/42"},  # repeated visit kept
        {"log_id": "evt-04", "visitor_id": "visitor-7", "ts": T0 + 60,
         "action": "exit"},
        {"log_id": "evt-05", "visitor_id": "visitor-7", "ts": T0 + 2400,
         "action": "visit", "page_path": "/blog"},
        {"log_id": "evt-06", "visitor_id": "visitor-9", "ts": T0 + 5,
         "action": "visit", "page_path": "/pricing"},
    ]
    print("\nsubmit batch:", json.dumps(call("POST", "/logs", {"logs": batch}),
                                        ensure_ascii=False))

    # While rebuilding, reads stay available and explicitly flag pending.
    print("immediate read (may still be pending):",
          json.dumps(call("GET", "/visitors/visitor-7/sessions"), ensure_ascii=False))

    show_sessions("after first publish", wait_published("visitor-7"))
    wait_published("visitor-9")

    # 2. Correction within the same visitor: move evt-05 earlier so it joins
    #    the first session (gap no longer exceeds 30 minutes).
    correction = [{"log_id": "evt-05", "visitor_id": "visitor-7", "ts": T0 + 80,
                   "action": "visit", "page_path": "/blog"}]
    print("\ncorrection result:",
          json.dumps(call("POST", "/logs", {"logs": correction}), ensure_ascii=False))
    show_sessions("after correction (old + new generation retained)",
                  wait_published("visitor-7"))

    # 3. Reassign evt-06 to visitor-7: both visitors must be rebuilt.
    reassign = [{"log_id": "evt-06", "visitor_id": "visitor-7", "ts": T0 + 5,
                 "action": "visit", "page_path": "/pricing"}]
    print("\nreassign result:",
          json.dumps(call("POST", "/logs", {"logs": reassign}), ensure_ascii=False))
    show_sessions("visitor-7 gains the event, merged by (ts, log_id)",
                  wait_published("visitor-7"))
    show_sessions("visitor-9 loses its only event -> empty generation published",
                  wait_published("visitor-9"))

    # 4. Idempotent resubmission changes nothing.
    print("\nunchanged resubmission:",
          json.dumps(call("POST", "/logs", {"logs": [batch[0]]}), ensure_ascii=False))


if __name__ == "__main__":
    main()
