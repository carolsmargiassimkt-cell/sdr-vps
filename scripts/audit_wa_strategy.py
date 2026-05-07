#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter

import supervisor
from core.wa_strategy_state import has_sent_step, load_strategy_state


def main() -> int:
    state = load_strategy_state()
    blocked = supervisor.blocklist_values()
    deals = supervisor.load_open_deals()
    counts = Counter()
    duplicates = 0

    for deal in deals:
        deal_id = int((deal or {}).get("id") or 0)
        phone = supervisor.phone_from_deal(deal)
        reason = supervisor.hunter_skip_reason(deal, state, blocked)
        if reason:
            counts[f"blocked_{reason}"] += 1
        else:
            counts["eligible_hunter"] += 1

        if reason == "impeditive_tag":
            counts["blocked_by_tag"] += 1
        if reason == "blocklist":
            counts["blocked_by_blocklist"] += 1
        hunter_record = supervisor.get_record(state, deal_id, phone, "wa_hunter")
        if has_sent_step(state, deal_id, phone, "wa_hunter", "hunter_1") or int(hunter_record.get("hunter_step") or 0) >= 1:
            counts["already_received_opening"] += 1
        if supervisor.is_warm_deal(deal, state, phone):
            counts["became_warm"] += 1

    seen = set()
    for key, record in list(state.items()):
        if not isinstance(record, dict):
            continue
        deal_id = record.get("deal_id")
        phone = record.get("phone")
        strategy = record.get("strategy")
        for step in list(record.get("sent_steps") or []):
            identity = (deal_id, phone, strategy, step)
            if identity in seen:
                duplicates += 1
            seen.add(identity)

    print(f"eligible_hunter={counts['eligible_hunter']}")
    print(f"blocked_by_tag={counts['blocked_by_tag']}")
    print(f"blocked_by_blocklist={counts['blocked_by_blocklist']}")
    print(f"already_received_opening={counts['already_received_opening']}")
    print(f"became_warm={counts['became_warm']}")
    print(f"duplicados_deal_phone_step={duplicates}")
    for key in sorted(counts):
        if key.startswith("blocked_") and key not in {"blocked_by_tag", "blocked_by_blocklist"}:
            print(f"{key}={counts[key]}")
    return 1 if duplicates else 0


if __name__ == "__main__":
    raise SystemExit(main())
