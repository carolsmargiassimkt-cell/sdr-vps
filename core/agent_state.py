import json, os, time
from pathlib import Path

STATE_FILE = Path(__file__).resolve().parent.parent / "data" / "agent_state.json"

BLOCKED_STATES = {
    "sem_interesse",
    "numero_errado",
    "encerrado",
    "agendado",
}

CONVERSATION_STATES = {
    "conversando",
    "respondido",
    "aguardando_agendamento",
    "indicacao",
}

def _load():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8") or "{}")
    except Exception:
        return {}

def _save(data):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_FILE)

def key_for(phone=None, deal_id=None):
    if deal_id:
        return f"deal:{deal_id}"
    return f"phone:{str(phone or '').strip()}"

def get_state(phone=None, deal_id=None):
    data = _load()
    return data.get(key_for(phone, deal_id), {})

def set_state(phone=None, deal_id=None, state=None, intent=None, summary=None, extra=None):
    data = _load()
    key = key_for(phone, deal_id)
    current = data.get(key, {})
    current.update({
        "phone": str(phone or current.get("phone") or "").strip(),
        "deal_id": deal_id or current.get("deal_id"),
        "state": state or current.get("state") or "novo",
        "last_intent": intent or current.get("last_intent"),
        "last_seen_at": int(time.time()),
    })
    if summary:
        current["summary"] = str(summary)[-1000:]
    if isinstance(extra, dict):
        current.update(extra)
    data[key] = current
    _save(data)
    return current

def should_block_cadence(phone=None, deal_id=None):
    st = get_state(phone, deal_id)
    state = str(st.get("state") or "").lower()
    return state in BLOCKED_STATES or state in CONVERSATION_STATES

def route_intent(intent):
    i = str(intent or "").lower()

    if i in {"nao_interessado_hard", "sem_interesse", "negative_stop", "stop"}:
        return {"state": "sem_interesse", "action": "close_lost"}

    if i in {"numero_errado", "wrong_contact"}:
        return {"state": "numero_errado", "action": "block_number"}

    if i in {"indicacao", "referral"}:
        return {"state": "indicacao", "action": "create_referral"}

    if i in {"agendamento", "schedule", "meeting"}:
        return {"state": "agendado", "action": "schedule"}

    if i in {"interesse", "positive", "positivo"}:
        return {"state": "conversando", "action": "reply_pitch"}

    if i in {"neutro", "duvida", "question", "neutral"}:
        return {"state": "conversando", "action": "reply_context"}

    return {"state": "conversando", "action": "reply_context"}
