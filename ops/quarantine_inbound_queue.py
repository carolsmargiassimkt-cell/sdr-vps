from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path("/root/sdr-vps/app/inbound_queue.sqlite3")


def main() -> int:
    if not DB_PATH.exists():
        print("[INBOUND_QUEUE_QUARANTINE] db=missing updated=0")
        return 0
    with sqlite3.connect(DB_PATH) as conn:
        updated = conn.execute(
            """
            UPDATE inbound_messages
            SET status = 'done', processing_started_at = NULL
            WHERE status != 'done'
            """
        ).rowcount or 0
        conn.commit()
    print(f"[INBOUND_QUEUE_QUARANTINE] db={DB_PATH} updated={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
