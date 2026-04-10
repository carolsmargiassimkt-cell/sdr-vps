import datetime
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from utils.safe_json import safe_read_json, safe_write_json


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEAD_CARDS_FILE = os.path.join(BASE_DIR, "data", "lead_cards.json")
LEAD_CARDS_LOCK_FILE = f"{LEAD_CARDS_FILE}.lock"
LOG_FILE = os.path.join(BASE_DIR, "logs", "carol_logs.txt")
VALID_ACTIVITY_TYPES = {
    "WHATSAPP_REPLY",
    "LINKEDIN_INMAIL",
    "LINKEDIN_MESSAGE",
    "CALL_BACK",
    "FOLLOW_UP",
}
MAX_HISTORY_ENTRIES = 200


def _append_carol_log(message: str) -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as file_obj:
        file_obj.write(f"[{now}] {message}\n")


@contextmanager
def _lead_cards_lock(timeout_seconds: float = 5.0):
    os.makedirs(os.path.dirname(LEAD_CARDS_FILE), exist_ok=True)
    with open(LEAD_CARDS_LOCK_FILE, "a+", encoding="utf-8") as lock_file:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write("0")
            lock_file.flush()

        start = time.time()
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if (time.time() - start) >= timeout_seconds:
                    raise TimeoutError("timeout ao adquirir lock dos lead cards")
                time.sleep(0.05)

        try:
            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt

                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass


def _load_cards_unlocked() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(LEAD_CARDS_FILE):
        return {}
    try:
        data = safe_read_json(LEAD_CARDS_FILE)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        _append_carol_log(f"[LEAD_CARD] falha ao carregar cards: {exc}")
        return {}


def _save_cards_unlocked(cards: Dict[str, Dict[str, Any]]) -> None:
    try:
        safe_write_json(LEAD_CARDS_FILE, cards)
    finally:
        try:
            tmp_file = f"{LEAD_CARDS_FILE}.tmp"
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
        except OSError:
            pass


def _now_iso() -> str:
    return datetime.datetime.now().isoformat()


def _normalize_datetime(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.datetime.fromisoformat(raw).isoformat()
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.datetime.strptime(raw, fmt).isoformat()
            except ValueError:
                continue
    return raw


def _normalize_list(values: Any) -> List[str]:
    if isinstance(values, list):
        items = values
    elif values:
        items = [values]
    else:
        items = []

    normalized = []
    for item in items:
        clean = str(item or "").strip()
        if clean and clean not in normalized:
            normalized.append(clean)
    return normalized


def _default_card(lead_id: str) -> Dict[str, Any]:
    return {
        "lead_id": str(lead_id),
        "company_name": "",
        "pipeline_stage": "",
        "decision_maker_name": "",
        "phones": [],
        "email": "",
        "linkedin_profile": "",
        "notes": "",
        "source": "",
        "sources": [],
        "cadencia": "",
        "bot_envio": "",
        "bot_matriz": "",
        "data_ultimo_envio": "",
        "last_activity": "",
        "next_activity_type": "",
        "next_activity_datetime": "",
        "activity_history": [],
        "updated_at": "",
    }


def _normalize_history(history: Any) -> List[Dict[str, str]]:
    items = history if isinstance(history, list) else []
    normalized: List[Dict[str, str]] = []
    for entry in items[-MAX_HISTORY_ENTRIES:]:
        if isinstance(entry, dict):
            timestamp = _normalize_datetime(entry.get("timestamp"))
            description = str(entry.get("description") or entry.get("atividade") or "").strip()
            kind = str(entry.get("kind") or "").strip()
        else:
            timestamp = _now_iso()
            description = str(entry).strip()
            kind = ""
        if description:
            normalized.append({"timestamp": timestamp or _now_iso(), "description": description, "kind": kind})
    normalized.sort(key=lambda item: item.get("timestamp", ""))
    return normalized[-MAX_HISTORY_ENTRIES:]


def _normalize_card(card: Optional[Dict[str, Any]], lead_id: str) -> Dict[str, Any]:
    base = _default_card(lead_id)
    merged = dict(base)
    for key, value in (card or {}).items():
        merged[key] = value

    merged["lead_id"] = str(lead_id)
    merged["company_name"] = str(merged.get("company_name") or merged.get("empresa") or "").strip()
    merged["pipeline_stage"] = str(merged.get("pipeline_stage") or merged.get("status") or "").strip()
    merged["decision_maker_name"] = str(merged.get("decision_maker_name") or merged.get("nome") or "").strip()
    merged["phones"] = _normalize_list(merged.get("phones") or merged.get("telefone"))
    merged["email"] = str(merged.get("email") or "").strip()
    merged["linkedin_profile"] = str(merged.get("linkedin_profile") or merged.get("linkedin") or "").strip()
    merged["notes"] = str(merged.get("notes") or merged.get("observacao") or "").strip()
    merged["source"] = str(merged.get("source") or "").strip().lower()
    raw_sources = merged.get("sources") if isinstance(merged.get("sources"), list) else [merged.get("sources")]
    sources = [str(item or "").strip().lower() for item in raw_sources if str(item or "").strip()]
    if merged["source"] and merged["source"] not in sources:
        sources.append(merged["source"])
    merged["sources"] = sorted(set(sources))
    merged["cadencia"] = str(merged.get("cadencia") or merged.get("Cadencia") or "").strip()
    merged["bot_envio"] = str(merged.get("bot_envio") or merged.get("BotEnvio") or "").strip()
    merged["bot_matriz"] = str(merged.get("bot_matriz") or merged.get("BotMatriz") or "").strip()
    merged["data_ultimo_envio"] = str(merged.get("data_ultimo_envio") or merged.get("DataUltimoEnvio") or "").strip()
    merged["last_activity"] = str(merged.get("last_activity") or "").strip()
    next_type = str(merged.get("next_activity_type") or "").strip().upper()
    merged["next_activity_type"] = next_type if next_type in VALID_ACTIVITY_TYPES else ""
    merged["next_activity_datetime"] = _normalize_datetime(merged.get("next_activity_datetime"))
    merged["activity_history"] = _normalize_history(merged.get("activity_history"))
    merged["updated_at"] = _normalize_datetime(merged.get("updated_at")) or _now_iso()
    return merged


def load_all_lead_cards() -> Dict[str, Dict[str, Any]]:
    with _lead_cards_lock():
        cards = _load_cards_unlocked()
    return {lead_id: _normalize_card(card, lead_id) for lead_id, card in cards.items()}


def get_lead_card(lead_id: str) -> Dict[str, Any]:
    key = str(lead_id)
    with _lead_cards_lock():
        cards = _load_cards_unlocked()
        return _normalize_card(cards.get(key, {}), key)


def _sync_card(card: Dict[str, Any], changed_fields: List[str], history_entry: str = "") -> None:
    try:
        from core.pipedrive_sync import sync_lead_card

        sync_lead_card(card, changed_fields=changed_fields, history_entry=history_entry)
    except Exception as exc:
        _append_carol_log(f"[LEAD_CARD] falha ao sincronizar {card.get('lead_id')}: {exc}")


def update_lead_card(
    lead_id: str,
    updates: Optional[Dict[str, Any]] = None,
    sync_pipedrive: bool = True,
    history_entry: str = "",
) -> Dict[str, Any]:
    key = str(lead_id)
    payload = dict(updates or {})

    with _lead_cards_lock():
        cards = _load_cards_unlocked()
        current = _normalize_card(cards.get(key, {}), key)
        merged = dict(current)
        merged.update(payload)
        normalized = _normalize_card(merged, key)
        cards[key] = normalized
        _save_cards_unlocked(cards)

    changed_fields = sorted({field for field in payload.keys() if current.get(field) != normalized.get(field)})
    if sync_pipedrive and changed_fields:
        _sync_card(normalized, changed_fields=changed_fields, history_entry=history_entry)
    return normalized


def append_activity_history(
    lead_id: str,
    description: str,
    timestamp: str = "",
    kind: str = "",
    sync_pipedrive: bool = False,
) -> Dict[str, Any]:
    card = get_lead_card(lead_id)
    entry = {
        "timestamp": _normalize_datetime(timestamp) or _now_iso(),
        "description": str(description or "").strip(),
        "kind": str(kind or "").strip(),
    }
    history = list(card.get("activity_history", []))
    if entry["description"]:
        history.append(entry)
    updates = {
        "last_activity": entry["description"],
        "activity_history": history[-MAX_HISTORY_ENTRIES:],
    }
    return update_lead_card(
        lead_id,
        updates=updates,
        sync_pipedrive=sync_pipedrive,
        history_entry=entry["description"],
    )


def add_note(lead_id: str, note_text: str) -> Dict[str, Any]:
    clean_note = str(note_text or "").strip()
    if not clean_note:
        return get_lead_card(lead_id)

    card = get_lead_card(lead_id)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    combined_notes = card.get("notes", "").strip()
    note_block = f"[{timestamp}] {clean_note}"
    new_notes = f"{combined_notes}\n{note_block}".strip() if combined_notes else note_block
    updated = update_lead_card(
        lead_id,
        updates={"notes": new_notes},
        sync_pipedrive=True,
        history_entry=f"Nota adicionada: {clean_note}",
    )
    return append_activity_history(lead_id, f"Nota adicionada: {clean_note}", kind="NOTE")


def schedule_next_activity(lead_id: str, activity_type: str, when: str) -> Dict[str, Any]:
    normalized_type = str(activity_type or "").strip().upper()
    if normalized_type not in VALID_ACTIVITY_TYPES:
        raise ValueError(f"tipo de atividade invalido: {activity_type}")

    normalized_when = _normalize_datetime(when)
    if not normalized_when:
        raise ValueError("next_activity_datetime obrigatorio")

    updates = {
        "next_activity_type": normalized_type,
        "next_activity_datetime": normalized_when,
    }
    update_lead_card(
        lead_id,
        updates=updates,
        sync_pipedrive=True,
        history_entry=f"Proxima atividade agendada: {normalized_type}",
    )
    card = append_activity_history(
        lead_id,
        f"Proxima atividade agendada: {normalized_type} em {normalized_when}",
        kind="SCHEDULED_ACTIVITY",
    )
    try:
        from core.priority_recovery import rebuild_daily_priority_queue

        rebuild_daily_priority_queue()
    except Exception as exc:
        _append_carol_log(f"[LEAD_CARD] falha ao atualizar fila prioritaria: {exc}")
    return card


def clear_next_activity(lead_id: str, reason: str = "") -> Dict[str, Any]:
    card = update_lead_card(
        lead_id,
        updates={"next_activity_type": "", "next_activity_datetime": ""},
        sync_pipedrive=False,
    )
    try:
        from core.priority_recovery import rebuild_daily_priority_queue

        rebuild_daily_priority_queue()
    except Exception as exc:
        _append_carol_log(f"[LEAD_CARD] falha ao atualizar fila prioritaria: {exc}")
    if reason:
        return append_activity_history(lead_id, reason, kind="ACTIVITY_CLEARED")
    return card


def sync_imported_leads(leads: List[Dict[str, Any]]) -> int:
    imported = 0
    for index, lead in enumerate(leads or []):
        lead_id = str(
            lead.get("id")
            or lead.get("lead_id")
            or lead.get("DealID")
            or lead.get("telefone")
            or lead.get("email")
            or lead.get("empresa")
            or f"lead_{index}"
        ).strip()
        if not lead_id:
            continue
        phones = []
        if lead.get("telefone"):
            phones.append(lead.get("telefone"))
        if lead.get("phones"):
            phones.extend(list(lead.get("phones")))
        update_lead_card(
            lead_id,
            updates={
                "lead_id": lead_id,
                "company_name": lead.get("empresa") or lead.get("Empresa") or "",
                "pipeline_stage": lead.get("pipeline_stage") or lead.get("status") or lead.get("Status") or "ENTRADA",
                "decision_maker_name": lead.get("nome") or lead.get("Nome") or "",
                "phones": phones,
                "email": lead.get("email") or lead.get("Email") or "",
                "linkedin_profile": lead.get("linkedin") or lead.get("LinkedIn") or "",
                "notes": lead.get("observacao") or lead.get("Observacao") or "",
                "source": "spreadsheet_import",
                "sources": ["spreadsheet_import"],
                "cadencia": lead.get("cadencia") or lead.get("Cadencia") or "",
                "bot_envio": lead.get("bot_envio") or lead.get("BotEnvio") or "",
                "bot_matriz": lead.get("bot_matriz") or lead.get("BotMatriz") or "",
                "data_ultimo_envio": lead.get("data_ultimo_envio") or lead.get("DataUltimoEnvio") or "",
            },
            sync_pipedrive=False,
        )
        imported += 1
    if imported:
        _append_carol_log(f"[LEAD_CARD] cards sincronizados da planilha: {imported}")
    return imported


def get_pending_activities(reference_time: Optional[datetime.datetime] = None) -> List[Dict[str, Any]]:
    now = reference_time or datetime.datetime.now()
    pending = []
    for card in load_all_lead_cards().values():
        activity_type = str(card.get("next_activity_type") or "").strip()
        activity_when = str(card.get("next_activity_datetime") or "").strip()
        if not activity_type or not activity_when:
            continue
        try:
            scheduled_at = datetime.datetime.fromisoformat(activity_when)
        except ValueError:
            scheduled_at = now
        item = dict(card)
        item["scheduled_at"] = scheduled_at.isoformat()
        item["is_overdue"] = scheduled_at <= now
        pending.append(item)
    pending.sort(key=lambda card: str(card.get("next_activity_datetime") or ""))
    return pending
