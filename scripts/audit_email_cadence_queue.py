from __future__ import annotations

import os
from collections import Counter
from datetime import datetime

from email_cadence.engine import (
    DAILY_LIMIT,
    STOP_STATUSES,
    due_from_missing_next_send,
    get_next_send,
    load,
    load_events,
    parse_dt,
)


def is_today(value) -> bool:
    parsed = parse_dt(value)
    return bool(parsed and parsed.date() == datetime.now().date())


def row_step(row) -> int:
    try:
        return max(1, min(6, int(row.get("step") or 1)))
    except Exception:
        return 1


def main() -> int:
    rows = load()
    events = load_events()
    current = datetime.now()
    limit = int(os.getenv("EMAIL_DAILY_LIMIT", str(DAILY_LIMIT)) or DAILY_LIMIT)
    counts = Counter()
    due_examples = []
    by_step = Counter()

    sent_today = sum(1 for event in events if is_today(event.get("sent_at")) and str(event.get("type") or event.get("status") or "sent") in {"sent", "email_sent"})
    if not sent_today:
        sent_today = sum(1 for row in rows if is_today(row.get("last_sent_at")))

    for row in rows:
        counts["total"] += 1
        status = str(row.get("status") or "").strip().lower()
        step = row_step(row)
        by_step[step] += 1
        if status == "pending":
            counts["pending"] += 1
        if status in STOP_STATUSES or status != "pending":
            continue

        next_send = get_next_send(row)
        if not next_send:
            counts["missing_next_send"] += 1
            is_due, computed = due_from_missing_next_send(row, step, current)
            if not is_due:
                counts["blocked_future"] += 1
                continue
        else:
            parsed = parse_dt(next_send)
            if parsed and parsed > current:
                counts["blocked_future"] += 1
                continue
            if not parsed:
                counts["missing_next_send"] += 1

        counts["due_now"] += 1
        if len(due_examples) < 20:
            due_examples.append({
                "deal_id": row.get("deal_id"),
                "email": row.get("email"),
                "step": step,
                "last_sent_at": row.get("last_sent_at"),
                "next_send": row.get("next_send"),
                "next_send_at": row.get("next_send_at"),
            })

    remaining_capacity = max(0, limit - sent_today)
    estimated_sendable = min(int(counts["due_now"]), remaining_capacity)

    print(f"total={counts['total']}")
    print(f"pending={counts['pending']}")
    print(f"due_now={counts['due_now']}")
    print(f"blocked_future={counts['blocked_future']}")
    print(f"missing_next_send={counts['missing_next_send']}")
    print(f"sent_today={sent_today}")
    print(f"EMAIL_DAILY_LIMIT={limit}")
    print(f"estimated_sendable_today={estimated_sendable}")
    print("por_step:")
    for step in range(1, 7):
        print(f"  step_{step}={by_step[step]}")
    print("due_now_examples:")
    for item in due_examples:
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
