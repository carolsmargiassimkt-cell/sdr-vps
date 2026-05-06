import json
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
STATE_FILE = BASE / "data" / "sdr_state.json"
EVENTS_FILE = BASE / "logs" / "sdr_events.log"

def _now():
    return datetime.now().isoformat(timespec="seconds")

def _load():
    if not STATE_FILE.exists():
        return {"phones": {}, "deals": {}}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {"phones": {}, "deals": {}}

def _save(data):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))

def log_event(event, **data):
    EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    row = {"ts": _now(), "event": event, **data}
    with EVENTS_FILE.open("a") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return True

def mark_inbound(phone=None, deal_id=None, message=None, intent=None, **kw):
    data = _load()
    key = str(phone or deal_id or "unknown")
    data.setdefault("phones", {}).setdefault(key, {})
    data["phones"][key].update({
        "last_inbound_at": _now(),
        "last_message": message,
        "last_intent": intent,
        **kw
    })
    _save(data)
    log_event("INBOUND_MARKED", phone=phone, deal_id=deal_id, intent=intent)
    return True

def stop_automation(phone=None, deal_id=None, reason="stop", **kw):
    data = _load()
    key = str(phone or deal_id or "unknown")
    data.setdefault("phones", {}).setdefault(key, {})
    data["phones"][key].update({
        "automation_stopped": True,
        "stop_reason": reason,
        "stopped_at": _now(),
        **kw
    })
    _save(data)
    log_event("AUTOMATION_STOPPED", phone=phone, deal_id=deal_id, reason=reason)
    return True

def mark_warm(*args, **kwargs):
    log_event("MARK_WARM", args=str(args), **kwargs)
    return True

def update_score(*args, **kwargs):
    log_event("UPDATE_SCORE", args=str(args), **kwargs)
    return True
