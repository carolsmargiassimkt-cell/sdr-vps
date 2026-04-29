import json, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / "data" / "agent_state.json"
HOT_QUEUE_FILE = ROOT / "data" / "hot_lead_queue.json"

HOT_STATES = {"conversando", "respondido", "aguardando_agendamento"}
BLOCKED_STATES = {"sem_interesse", "numero_errado", "encerrado", "agendado", "indicacao"}

def _load_json(path, default):
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8") or json.dumps(default))
    except Exception:
        return default

def _save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def build_hot_queue(min_age_minutes=60, max_age_hours=48):
    now = int(time.time())
    state = _load_json(STATE_FILE, {})
    queue = []

    for key, item in state.items():
        if not isinstance(item, dict):
            continue

        current_state = str(item.get("state") or "").lower()
        if current_state in BLOCKED_STATES:
            continue
        if current_state not in HOT_STATES:
            continue

        last_seen = int(item.get("last_seen_at") or 0)
        if last_seen <= 0:
            continue

        age_sec = now - last_seen
        if age_sec < min_age_minutes * 60:
            continue
        if age_sec > max_age_hours * 3600:
            continue

        queue.append({
            "key": key,
            "phone": item.get("phone"),
            "deal_id": item.get("deal_id"),
            "state": current_state,
            "last_intent": item.get("last_intent"),
            "last_action": item.get("last_action"),
            "last_seen_at": last_seen,
            "age_minutes": age_sec // 60,
            "summary": item.get("summary", ""),
            "priority": _priority(current_state, item.get("last_intent")),
        })

    queue.sort(key=lambda x: (-int(x["priority"]), int(x["age_minutes"])))
    _save_json(HOT_QUEUE_FILE, {"updated_at": now, "items": queue})
    return queue

def _priority(state, intent):
    s = str(state or "").lower()
    i = str(intent or "").lower()
    if "agendamento" in i or s == "aguardando_agendamento":
        return 100
    if i in {"interesse", "positive", "positivo"}:
        return 90
    if s == "conversando":
        return 70
    if s == "respondido":
        return 60
    return 40

def recovery_message(item):
    summary = str(item.get("summary") or "").strip()
    if len(summary) > 140:
        summary = summary[-140:]

    if item.get("last_intent") in {"interesse", "positive", "positivo"}:
        return "Oi! Retomando nossa conversa: vi que fazia sentido entender melhor. Quer que eu te mostre rapidamente como funcionaria para vocês?"

    if "agendamento" in str(item.get("last_intent") or "").lower():
        return "Oi! Só retomando para fecharmos o melhor horário. Qual período fica melhor para você?"

    return "Oi! Só retomando nossa conversa por aqui. Ainda faz sentido eu te explicar rapidinho como funcionaria para vocês?"
