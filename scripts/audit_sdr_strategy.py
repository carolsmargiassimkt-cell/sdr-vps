import json
from collections import Counter
from pathlib import Path
from sdr_core.strategy import resolve_sdr_strategy

candidates = []
for p in [
    Path("data/deals_cache.json"),
    Path("data/daily_flow_plan.json"),
    Path("data/whatsapp_warm_cadence.json"),
]:
    if p.exists():
        try:
            data = json.load(open(p, encoding="utf-8"))
            if isinstance(data, dict):
                data = data.get("deals") or data.get("items") or list(data.values())
            if isinstance(data, list):
                candidates.extend([x for x in data if isinstance(x, dict)])
        except Exception:
            pass

counter = Counter()
reasons = Counter()

for d in candidates:
    strategy, rs = resolve_sdr_strategy(d)
    counter[strategy] += 1
    for r in rs:
        reasons[r] += 1

print("TOTAL_AVALIADOS:", len(candidates))
print("ESTRATEGIAS:")
for k, v in counter.most_common():
    print(k, v)

print("\nMOTIVOS:")
for k, v in reasons.most_common():
    print(k, v)

state_path = Path("data/wa_strategy_state.json")
if state_path.exists():
    st = json.load(open(state_path, encoding="utf-8"))
    seen = Counter()
    for key, val in st.items():
        for step in val.get("sent_steps", []):
            seen[(key, step)] += 1
    dup = [x for x, n in seen.items() if n > 1]
    print("\nDUPLICADOS deal+phone+step:", len(dup))
else:
    print("\nwa_strategy_state.json ainda não existe")
