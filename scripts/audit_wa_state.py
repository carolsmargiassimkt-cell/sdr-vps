import json
from pathlib import Path
from collections import Counter

p=Path("data/wa_strategy_state.json")
if not p.exists():
    print("WA_STATE_EMPTY")
    raise SystemExit

d=json.load(open(p,encoding="utf-8"))
print("TOTAL_STATE:", len(d))
print("STATUS:", Counter(v.get("status") for v in d.values()))
print("STRATEGY:", Counter(v.get("strategy") for v in d.values()))

dups=0
for k,v in d.items():
    steps=v.get("sent_steps",[])
    if len(steps)!=len(set(steps)):
        dups+=1
print("DUPLICADOS_STEPS:", dups)
