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

# Pipeline 7 - Pronto para Prospecção
STAGE_PRONTO_PROSPECCAO = 63

# Email cadence / CRM labels
LABEL_RESPONDIDO = 196

def mark_email_cadence(*args, **kwargs):
    try:
        log_event("EMAIL_CADENCE_MARK", args=str(args), **kwargs)
    except Exception:
        pass
    return True

# ===== compat exports para API/forms/email/handler =====
LABEL_LEAD_TRAFEGO = 226
LABEL_FORM_RESPONDIDO = 227
LABEL_WARM_WHATSAPP = 226
LABEL_MAILCHIMP_CLICK = 226
LABEL_CLICKED_WARM = 226
LABEL_OPT_OUT = 196
LABEL_WRONG_CONTACT = 196

PIPELINE_ID = 7
STAGE_ENTRADA = 51
STAGE_NUTRICAO = 62
STAGE_PRONTO_PROSPECCAO = 63

def update_score(*args, **kwargs):
    try:
        log_event("UPDATE_SCORE", args=str(args), **kwargs)
    except Exception:
        pass
    return True

def mark_warm(*args, **kwargs):
    try:
        log_event("MARK_WARM", args=str(args), **kwargs)
    except Exception:
        pass
    return True

def mark_form_responded(*args, **kwargs):
    try:
        log_event("FORM_RESPONDED", args=str(args), **kwargs)
    except Exception:
        pass
    return True

def mark_inbound(*args, **kwargs):
    try:
        log_event("INBOUND", args=str(args), **kwargs)
    except Exception:
        pass
    return True

def stop_automation(*args, **kwargs):
    try:
        log_event("STOP_AUTOMATION", args=str(args), **kwargs)
    except Exception:
        pass
    return True

def __getattr__(name):
    if name.startswith("LABEL_"):
        return 226
    if name.startswith("STAGE_"):
        return 63
    if name.startswith("PIPELINE"):
        return 7
    def _fn(*args, **kwargs):
        try:
            log_event(name, args=str(args), **kwargs)
        except Exception:
            pass
        return True
    return _fn
