from __future__ import annotations

from collections import Counter
from pathlib import Path

from linkedin.bot_linkedin import EVENTS_FILE, LinkedInBot
from utils.safe_json import safe_read_json


def main() -> int:
    bot = LinkedInBot()
    added = int(bot.collect_live_events(limit=10))
    payload = safe_read_json(Path(EVENTS_FILE))
    events = payload if isinstance(payload, list) else []
    pending = [item for item in events if isinstance(item, dict) and str(item.get("status", "")).strip() == "pending"]
    by_source = Counter(str(item.get("source", "unknown")).strip() for item in pending)

    print(f"LINKEDIN_CAPTURED_ADDED={added}")
    print(f"LINKEDIN_PENDING_TOTAL={len(pending)}")
    for source, total in sorted(by_source.items()):
        print(f"SOURCE_{source.upper()}={total}")

    for item in pending[:5]:
        nome = str(item.get("nome", "contato")).strip()
        source = str(item.get("source", "linkedin")).strip()
        message = str(item.get("message", "")).strip().replace("\n", " ")
        print(f"- {source} | {nome} | {message[:120]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
