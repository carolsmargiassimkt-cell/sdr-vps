import json, os
from datetime import datetime
from pathlib import Path

STATE_PATH = Path("data/wa_strategy_state.json")

def load_state():
    if not STATE_PATH.exists():
        return {}
    try:
        return json.load(open(STATE_PATH, encoding="utf-8"))
    except Exception:
        return {}

def save_state(state):
    STATE_PATH.parent.mkdir(exist_ok=True)
    tmp = str(STATE_PATH) + ".tmp"
    json.dump(state, open(tmp, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)

def key(deal_id, phone):
    return f"{deal_id}:{phone}"

def already_sent(deal_id, phone, step):
    st = load_state()
    return step in st.get(key(deal_id, phone), {}).get("sent_steps", [])

def mark_sent(deal_id, phone, strategy, step):
    st = load_state()
    k = key(deal_id, phone)
    rec = st.setdefault(k, {
        "strategy": strategy,
        "hunter_step": 0,
        "sent_steps": [],
        "status": "active",
        "reason": ""
    })
    rec["strategy"] = strategy
    rec["last_sent_at"] = datetime.now().isoformat()
    rec["status"] = "active"
    if step not in rec["sent_steps"]:
        rec["sent_steps"].append(step)
    if step.startswith("hunter_"):
        try:
            rec["hunter_step"] = max(int(step.split("_")[1]), int(rec.get("hunter_step") or 0))
        except Exception:
            pass
    save_state(st)

def stop_hunter(deal_id, phone, reason="warm"):
    st = load_state()
    k = key(deal_id, phone)
    rec = st.setdefault(k, {"sent_steps": []})
    rec["status"] = "warm" if reason == "warm" else "blocked"
    rec["reason"] = reason
    rec["last_update_at"] = datetime.now().isoformat()
    save_state(st)
