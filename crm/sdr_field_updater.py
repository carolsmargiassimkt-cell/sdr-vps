import os
import json
import requests
from pathlib import Path
from datetime import datetime

BASE = "https://api.pipedrive.com/v1"
MAP_PATH = Path("data/pipedrive_sdr_fields.json")
IDEMPOTENCY_PATH = Path("data/sdr_field_events.json")

FIELD_ALIASES = {
    "WhatsApp Status": "whatsapp_status",
    "tentativas_contato": "tentativas_contato",
    "ultimo_contato": "ultimo_contato",
    "canal_ultimo_contato": "canal_ultimo_contato",
    "Origem Oficial": "origem_oficial",
    "Status SDR": "status_sdr",
    "Etapa Cadencia": "etapa_cadencia",
    "lead_score": "lead_score",
}

def _token():
    return os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN")

def _now():
    return datetime.now().isoformat(timespec="seconds")

def _load_json(path, default):
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return default

def _save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

def load_field_map():
    m = _load_json(MAP_PATH, {})
    return m if isinstance(m, dict) else {}

def get_deal(deal_id):
    r = requests.get(
        f"{BASE}/deals/{deal_id}",
        params={"api_token": _token()},
        timeout=30,
    )
    data = r.json()
    return data.get("data") or {}

def update_deal(deal_id, payload):
    if not payload:
        return {"ok": True, "skip": "empty_payload"}

    r = requests.put(
        f"{BASE}/deals/{deal_id}",
        params={"api_token": _token()},
        json=payload,
        timeout=30,
    )
    return {"ok": r.ok, "status": r.status_code, "body": r.text[:300]}

def already_processed(event_id):
    if not event_id:
        return False
    data = _load_json(IDEMPOTENCY_PATH, {})
    return bool(data.get(str(event_id)))

def mark_processed(event_id):
    if not event_id:
        return
    data = _load_json(IDEMPOTENCY_PATH, {})
    data[str(event_id)] = _now()
    _save_json(IDEMPOTENCY_PATH, data)

def _key(field_map, name):
    return field_map.get(name)

def update_sdr_fields(deal_id, event):
    """
    event exemplos:
    {
      "event_id": "email_sent:123:2",
      "type": "email_sent",
      "channel": "email",
      "step": 2,
      "source": "email_cadence",
      "intent": "interesse",
      "phase": "outbound_engaged",
      "warm_source": "email_click",
      "automation_status": "pending",
      "meeting_status": "scheduled",
      "contamination_status": "ok",
      "increment_attempt": true
    }
    """
    event = event or {}
    event_id = event.get("event_id")

    if already_processed(event_id):
        return {"ok": True, "skip": "already_processed", "event_id": event_id}

    fmap = load_field_map()
    deal = get_deal(deal_id)
    payload = {}

    def set_field(logical_name, value):
        k = _key(fmap, logical_name)
        if k and value not in [None, ""]:
            payload[k] = value

    # novos sdr_*
    set_field("sdr_lead_source", event.get("lead_source") or event.get("source"))
    set_field("sdr_warm_source", event.get("warm_source"))
    set_field("sdr_last_intent", event.get("intent"))
    set_field("sdr_last_touch_channel", event.get("channel"))
    set_field("sdr_automation_status", event.get("automation_status") or event.get("status"))
    set_field("sdr_conversation_phase", event.get("phase"))
    set_field("sdr_email_clicked", event.get("email_clicked"))
    set_field("sdr_clicked_step", str(event.get("clicked_step") or event.get("step") or "") if event.get("email_clicked") else None)
    set_field("sdr_meeting_status", event.get("meeting_status"))
    set_field("sdr_contamination_status", event.get("contamination_status"))
    set_field("sdr_last_event_at", event.get("event_at") or _now())

    # campos existentes/operacionais
    for logical, value in {
        "WhatsApp Status": event.get("whatsapp_status"),
        "ultimo_contato": event.get("last_contact_at") or _now(),
        "canal_ultimo_contato": event.get("channel"),
        "Origem Oficial": event.get("origin"),
        "Status SDR": event.get("status_sdr"),
        "Etapa Cadencia": str(event.get("cadence_step") or event.get("step") or "") if event.get("step") else None,
        "lead_score": event.get("lead_score"),
    }.items():
        k = _key(fmap, logical) or _key(fmap, logical.lower())
        if k and value not in [None, ""]:
            payload[k] = value

    # tentativas_contato += 1 idempotente
    if event.get("increment_attempt"):
        k = _key(fmap, "tentativas_contato")
        if k:
            current = deal.get(k) or 0
            try:
                current = int(float(current))
            except Exception:
                current = 0
            payload[k] = current + 1

    result = update_deal(deal_id, payload)

    if result.get("ok"):
        mark_processed(event_id)
        print("[SDR_FIELDS_UPDATED]", deal_id, event.get("type"), payload)
    else:
        print("[SDR_FIELDS_UPDATE_FAIL]", deal_id, event.get("type"), result)

    return result
