#!/usr/bin/env bash
cd /root/sdr-vps || exit 1

MIN_AGE="${1:-60}"
MAX_AGE="${2:-48}"

python3 - <<PY
from core.hot_lead_queue import build_hot_queue, recovery_message
queue = build_hot_queue(min_age_minutes=int("$MIN_AGE"), max_age_hours=int("$MAX_AGE"))
print(f"HOT_LEADS={len(queue)}")
for i, item in enumerate(queue[:50], 1):
    print("-" * 80)
    print(f"{i}. phone={item.get('phone')} deal={item.get('deal_id')} state={item.get('state')} intent={item.get('last_intent')} age={item.get('age_minutes')}min priority={item.get('priority')}")
    print(f"summary={item.get('summary')}")
    print(f"suggested={recovery_message(item)}")
PY
