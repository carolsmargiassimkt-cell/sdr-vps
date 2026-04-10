import csv
import datetime
import hashlib
import os
import random
import re
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from config import settings
from core.lead_cards import load_all_lead_cards
from utils.safe_json import safe_read_json, safe_write_json


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_FILE = os.path.join(BASE_DIR, "logs", "carol_logs.txt")
CRM_WHITELIST_FILE = os.path.join(DATA_DIR, "crm_whitelist.json")
CRM_WHITELIST_LOCK_FILE = f"{CRM_WHITELIST_FILE}.lock"
SENT_MESSAGES_REGISTRY_FILE = os.path.join(DATA_DIR, "sent_messages_registry.json")
SENT_MESSAGES_REGISTRY_LOCK_FILE = f"{SENT_MESSAGES_REGISTRY_FILE}.lock"
LINKEDIN_SAFETY_FILE = os.path.join(DATA_DIR, "linkedin_safety_state.json")
LINKEDIN_SAFETY_LOCK_FILE = f"{LINKEDIN_SAFETY_FILE}.lock"
SIMULATION_FLAG = "CENTRAL_MAND_SIMULATION"

ALLOWED_WHITELIST_SOURCES = {"spreadsheet_import", "pipedrive_leads"}
ALLOWED_INBOUND_CHANNELS = {"whatsapp", "linkedin"}
LINKEDIN_LIMITS = {
    "connections_per_day": int(os.getenv("LINKEDIN_CONNECTIONS_PER_DAY", "80")),
    "messages_per_day": int(os.getenv("LINKEDIN_MESSAGES_PER_DAY", "120")),
    "inmails_per_day": int(os.getenv("LINKEDIN_INMAILS_PER_DAY", "40")),
    "actions_per_minute": int(os.getenv("LINKEDIN_ACTIONS_PER_MINUTE", "5")),
    "delay_min_seconds": int(os.getenv("LINKEDIN_DELAY_MIN_SECONDS", "30")),
    "delay_max_seconds": int(os.getenv("LINKEDIN_DELAY_MAX_SECONDS", "120")),
    "cooldown_every_actions": int(os.getenv("LINKEDIN_COOLDOWN_EVERY_ACTIONS", "10")),
    "cooldown_seconds": int(os.getenv("LINKEDIN_COOLDOWN_SECONDS", "600")),
}


def _append_carol_log(message: str) -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as file_obj:
        file_obj.write(f"[{now}] {message}\n")


@contextmanager
def _locked_file(path: str, timeout_seconds: float = 5.0):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a+", encoding="utf-8") as lock_file:
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
                    raise TimeoutError(f"timeout ao adquirir lock: {path}")
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


def _normalize_phone(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def _normalize_linkedin(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    return normalized.rstrip("/")


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_message_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _is_simulation() -> bool:
    return bool(str(os.getenv(SIMULATION_FLAG, "")).strip())


def _sleep_if_needed(seconds: int, simulate: bool = False) -> None:
    if seconds <= 0:
        return
    if simulate or _is_simulation():
        return
    time.sleep(int(seconds))


def _safe_load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        data = safe_read_json(path)
        return data
    except Exception:
        return default


def _safe_save_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        safe_write_json(path, data)
    finally:
        try:
            tmp_file = f"{path}.tmp"
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
        except OSError:
            pass


def _empty_whitelist() -> Dict[str, Any]:
    return {"generated_at": "", "leads": []}


def _lead_payload(lead_id: str) -> Dict[str, Any]:
    return {
        "lead_id": str(lead_id or "").strip(),
        "phones": [],
        "linkedin_profiles": [],
        "emails": [],
        "sources": [],
        "company_name": "",
    }


def _upsert_whitelist_lead(
    items: Dict[str, Dict[str, Any]],
    lead_id: str,
    phone: Any = "",
    linkedin: Any = "",
    email: Any = "",
    source: str = "",
    company_name: str = "",
) -> None:
    key = str(lead_id or "").strip()
    if not key:
        return
    current = items.setdefault(key, _lead_payload(key))
    normalized_phone = _normalize_phone(phone)
    normalized_linkedin = _normalize_linkedin(linkedin)
    normalized_email = _normalize_email(email)
    normalized_source = str(source or "").strip().lower()
    normalized_company = str(company_name or "").strip()

    if normalized_phone and normalized_phone not in current["phones"]:
        current["phones"].append(normalized_phone)
    if normalized_linkedin and normalized_linkedin not in current["linkedin_profiles"]:
        current["linkedin_profiles"].append(normalized_linkedin)
    if normalized_email and normalized_email not in current["emails"]:
        current["emails"].append(normalized_email)
    if normalized_source and normalized_source not in current["sources"]:
        current["sources"].append(normalized_source)
    if normalized_company and not current["company_name"]:
        current["company_name"] = normalized_company


def rebuild_crm_whitelist(spreadsheet_path: Optional[str] = None) -> Dict[str, Any]:
    whitelist_items: Dict[str, Dict[str, Any]] = {}
    csv_path = spreadsheet_path or settings.get_setting("leads_csv_fallback", "data/leads.csv")
    absolute_csv_path = csv_path if os.path.isabs(csv_path) else os.path.join(BASE_DIR, csv_path)

    if os.path.exists(absolute_csv_path):
        try:
            with open(absolute_csv_path, "r", encoding="utf-8-sig", newline="") as file_obj:
                for index, row in enumerate(csv.DictReader(file_obj)):
                    lead_id = (
                        row.get("DealID")
                        or row.get("id")
                        or row.get("lead_id")
                        or row.get("Telefone")
                        or row.get("Email")
                        or row.get("Empresa")
                        or f"spreadsheet_{index}"
                    )
                    _upsert_whitelist_lead(
                        whitelist_items,
                        lead_id=lead_id,
                        phone=row.get("Telefone") or row.get("telefone"),
                        linkedin=row.get("LinkedIn") or row.get("linkedin"),
                        email=row.get("Email") or row.get("email"),
                        source="spreadsheet_import",
                        company_name=row.get("Empresa") or row.get("empresa") or "",
                    )
        except Exception as exc:
            _append_carol_log(f"[SECURITY] falha ao reconstruir whitelist da planilha: {exc}")

    try:
        for lead_id, card in load_all_lead_cards().items():
            sources = list(card.get("sources") or [])
            default_source = str(card.get("source") or "").strip().lower() or "pipedrive_leads"
            if default_source not in sources:
                sources.append(default_source)
            for phone in card.get("phones") or []:
                _upsert_whitelist_lead(
                    whitelist_items,
                    lead_id=lead_id,
                    phone=phone,
                    linkedin=card.get("linkedin_profile"),
                    email=card.get("email"),
                    source=default_source,
                    company_name=card.get("company_name"),
                )
            if not (card.get("phones") or []):
                _upsert_whitelist_lead(
                    whitelist_items,
                    lead_id=lead_id,
                    linkedin=card.get("linkedin_profile"),
                    email=card.get("email"),
                    source=default_source,
                    company_name=card.get("company_name"),
                )
            for source in sources:
                _upsert_whitelist_lead(
                    whitelist_items,
                    lead_id=lead_id,
                    source=source,
                    company_name=card.get("company_name"),
                )
    except Exception as exc:
        _append_carol_log(f"[SECURITY] falha ao reconstruir whitelist do CRM: {exc}")

    payload = {
        "generated_at": datetime.datetime.now().isoformat(),
        "leads": sorted(whitelist_items.values(), key=lambda item: item.get("lead_id", "")),
    }
    with _locked_file(CRM_WHITELIST_LOCK_FILE):
        _safe_save_json(CRM_WHITELIST_FILE, payload)
    return payload


def load_crm_whitelist() -> Dict[str, Any]:
    if not os.path.exists(CRM_WHITELIST_FILE):
        return rebuild_crm_whitelist()
    with _locked_file(CRM_WHITELIST_LOCK_FILE):
        data = _safe_load_json(CRM_WHITELIST_FILE, _empty_whitelist())
    return data if isinstance(data, dict) else _empty_whitelist()


def find_crm_lead(
    lead_id: Any = "",
    phone: Any = "",
    linkedin_profile: Any = "",
    email: Any = "",
    whitelist: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    normalized_lead_id = str(lead_id or "").strip()
    normalized_phone = _normalize_phone(phone)
    normalized_linkedin = _normalize_linkedin(linkedin_profile)
    normalized_email = _normalize_email(email)

    whitelist = whitelist or load_crm_whitelist()
    for item in whitelist.get("leads", []):
        if not ALLOWED_WHITELIST_SOURCES.intersection(set(item.get("sources") or [])):
            continue
        if normalized_lead_id and normalized_lead_id == str(item.get("lead_id") or "").strip():
            return item
        if normalized_phone and normalized_phone in set(item.get("phones") or []):
            return item
        if normalized_linkedin and normalized_linkedin in set(item.get("linkedin_profiles") or []):
            return item
        if normalized_email and normalized_email in set(item.get("emails") or []):
            return item
    return None


def task_contact_identity(task: Dict[str, Any]) -> Dict[str, str]:
    dados = dict(task.get("dados") or {})
    return {
        "lead_id": str(task.get("lead_id") or dados.get("lead_id") or dados.get("DealID") or "").strip(),
        "phone": _normalize_phone(dados.get("telefone") or dados.get("Telefone") or ""),
        "linkedin_profile": _normalize_linkedin(
            dados.get("linkedin_profile") or dados.get("linkedin") or dados.get("LinkedIn") or ""
        ),
        "email": _normalize_email(dados.get("email") or dados.get("Email") or ""),
        "channel": str(task.get("canal") or dados.get("channel") or "").strip().lower(),
        "message_text": str(
            dados.get("mensagem")
            or dados.get("msg")
            or dados.get("message_text")
            or dados.get("corpo")
            or ""
        ).strip(),
    }


def validate_task_whitelist(task: Dict[str, Any], log_block: bool = True) -> Tuple[bool, Optional[Dict[str, Any]]]:
    identity = task_contact_identity(task)
    channel = identity["channel"]
    crm_lead = find_crm_lead(
        lead_id=identity["lead_id"],
        phone=identity["phone"],
        linkedin_profile=identity["linkedin_profile"],
        email=identity["email"],
    )
    if not crm_lead and identity["lead_id"]:
        refreshed = rebuild_crm_whitelist()
        crm_lead = find_crm_lead(
            lead_id=identity["lead_id"],
            phone=identity["phone"],
            linkedin_profile=identity["linkedin_profile"],
            email=identity["email"],
            whitelist=refreshed,
        )

    if channel == "linkedin" and (not crm_lead or not list(crm_lead.get("linkedin_profiles") or [])):
        if log_block:
            _append_carol_log("[SECURITY] linkedin profile not in CRM")
        return False, None
    if channel in {"whatsapp", "voice_bot"} and (not crm_lead or not list(crm_lead.get("phones") or [])):
        if log_block:
            _append_carol_log("[SECURITY] sender not in CRM whitelist")
        return False, None
    if not crm_lead:
        if log_block:
            _append_carol_log("[SECURITY] sender not in CRM whitelist")
        return False, None
    return True, crm_lead


def validate_incoming_identity(
    channel: str,
    lead_id: Any = "",
    phone: Any = "",
    linkedin_profile: Any = "",
    email: Any = "",
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    normalized_channel = str(channel or "").strip().lower()
    if normalized_channel not in ALLOWED_INBOUND_CHANNELS:
        _append_carol_log("[SECURITY] message ignored (not CRM lead)")
        return False, None
    crm_lead = find_crm_lead(
        lead_id=lead_id,
        phone=phone,
        linkedin_profile=linkedin_profile,
        email=email,
    )
    if normalized_channel == "linkedin" and (not crm_lead or not list(crm_lead.get("linkedin_profiles") or [])):
        _append_carol_log("[SECURITY] message ignored (not CRM lead)")
        return False, None
    if normalized_channel == "whatsapp" and (not crm_lead or not list(crm_lead.get("phones") or [])):
        _append_carol_log("[SECURITY] message ignored (not CRM lead)")
        return False, None
    if not crm_lead:
        _append_carol_log("[SECURITY] message ignored (not CRM lead)")
        return False, None
    return True, crm_lead


def _load_message_registry() -> List[Dict[str, Any]]:
    with _locked_file(SENT_MESSAGES_REGISTRY_LOCK_FILE):
        data = _safe_load_json(SENT_MESSAGES_REGISTRY_FILE, [])
        return data if isinstance(data, list) else []


def _save_message_registry(entries: List[Dict[str, Any]]) -> None:
    with _locked_file(SENT_MESSAGES_REGISTRY_LOCK_FILE):
        _safe_save_json(SENT_MESSAGES_REGISTRY_FILE, entries)


def hash_message(message_text: Any) -> str:
    normalized = _normalize_message_text(message_text).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def check_duplicate_whatsapp_message(lead_id: Any, message_text: Any) -> bool:
    normalized_lead_id = str(lead_id or "").strip()
    message_hash = hash_message(message_text)
    for entry in _load_message_registry():
        if (
            str(entry.get("lead_id") or "").strip() == normalized_lead_id
            and str(entry.get("channel") or "").strip().lower() == "whatsapp"
            and str(entry.get("message_hash") or "") == message_hash
        ):
            _append_carol_log("[DUPLICATION BLOCKED] whatsapp message already sent")
            return True
    return False


def register_whatsapp_message(lead_id: Any, message_text: Any, timestamp: str = "") -> Dict[str, Any]:
    normalized_lead_id = str(lead_id or "").strip()
    entry = {
        "lead_id": normalized_lead_id,
        "channel": "whatsapp",
        "message_hash": hash_message(message_text),
        "timestamp": timestamp or datetime.datetime.now().isoformat(),
    }
    entries = _load_message_registry()
    entries.append(entry)
    _save_message_registry(entries[-5000:])
    return entry


def enforce_whatsapp_duplication_guard(task: Dict[str, Any]) -> bool:
    identity = task_contact_identity(task)
    if identity["channel"] != "whatsapp":
        return True
    if check_duplicate_whatsapp_message(identity["lead_id"], identity["message_text"]):
        return False
    return True


def confirm_whatsapp_dispatch(task: Dict[str, Any]) -> None:
    identity = task_contact_identity(task)
    if identity["channel"] != "whatsapp":
        return
    if identity["message_text"]:
        entries = _load_message_registry()
        message_hash = hash_message(identity["message_text"])
        for entry in reversed(entries[-20:]):
            if (
                str(entry.get("lead_id") or "").strip() == identity["lead_id"]
                and str(entry.get("channel") or "").strip().lower() == "whatsapp"
                and str(entry.get("message_hash") or "") == message_hash
            ):
                return
        register_whatsapp_message(identity["lead_id"], identity["message_text"])


def _empty_linkedin_state() -> Dict[str, Any]:
    return {
        "date": "",
        "actions": [],
        "connections": 0,
        "messages": 0,
        "inmails": 0,
    }


def _load_linkedin_state() -> Dict[str, Any]:
    with _locked_file(LINKEDIN_SAFETY_LOCK_FILE):
        data = _safe_load_json(LINKEDIN_SAFETY_FILE, _empty_linkedin_state())
    return data if isinstance(data, dict) else _empty_linkedin_state()


def _save_linkedin_state(data: Dict[str, Any]) -> None:
    with _locked_file(LINKEDIN_SAFETY_LOCK_FILE):
        _safe_save_json(LINKEDIN_SAFETY_FILE, data)


def _reset_linkedin_state_if_needed(data: Dict[str, Any]) -> Dict[str, Any]:
    today_key = datetime.date.today().isoformat()
    if data.get("date") != today_key:
        return {
            "date": today_key,
            "actions": [],
            "connections": 0,
            "messages": 0,
            "inmails": 0,
        }
    return data


def _linkedin_action_key(action_type: str) -> str:
    normalized = str(action_type or "").strip().lower()
    if normalized in {"follow", "connection", "connections"}:
        return "connections"
    if normalized in {"inmail", "inmails"}:
        return "inmails"
    return "messages"


def _linkedin_limit_for(action_key: str) -> int:
    if action_key == "connections":
        return int(LINKEDIN_LIMITS["connections_per_day"])
    if action_key == "inmails":
        return int(LINKEDIN_LIMITS["inmails_per_day"])
    return int(LINKEDIN_LIMITS["messages_per_day"])


def _prune_recent_actions(actions: List[float], now_ts: float) -> List[float]:
    return [float(item) for item in actions if float(item) >= now_ts - 86400]


def apply_linkedin_safety(action_type: str, simulate: bool = False) -> bool:
    state = _reset_linkedin_state_if_needed(_load_linkedin_state())
    now_ts = time.time()
    action_key = _linkedin_action_key(action_type)
    daily_limit = _linkedin_limit_for(action_key)
    state["actions"] = _prune_recent_actions(list(state.get("actions") or []), now_ts)

    if int(state.get(action_key, 0) or 0) >= daily_limit:
        _append_carol_log("[SAFETY] linkedin daily limit reached")
        _save_linkedin_state(state)
        return False

    recent_minute = [entry for entry in state["actions"] if float(entry) >= now_ts - 60]
    if len(recent_minute) >= int(LINKEDIN_LIMITS["actions_per_minute"]):
        wait_seconds = max(1, int(min(recent_minute) + 60 - now_ts) + 1)
        _append_carol_log("[SAFETY] linkedin cooldown triggered")
        _sleep_if_needed(wait_seconds, simulate=simulate)
        now_ts = time.time()
        state = _reset_linkedin_state_if_needed(_load_linkedin_state())
        state["actions"] = _prune_recent_actions(list(state.get("actions") or []), now_ts)

    total_actions = len(state["actions"])
    cooldown_every = int(LINKEDIN_LIMITS["cooldown_every_actions"])
    if total_actions > 0 and total_actions % cooldown_every == 0:
        _append_carol_log("[SAFETY] linkedin cooldown triggered")
        _sleep_if_needed(int(LINKEDIN_LIMITS["cooldown_seconds"]), simulate=simulate)
        now_ts = time.time()
        state = _reset_linkedin_state_if_needed(_load_linkedin_state())
        state["actions"] = _prune_recent_actions(list(state.get("actions") or []), now_ts)

    delay_seconds = random.randint(
        int(LINKEDIN_LIMITS["delay_min_seconds"]),
        int(LINKEDIN_LIMITS["delay_max_seconds"]),
    )
    _append_carol_log("[SAFETY] linkedin delay applied")
    _sleep_if_needed(delay_seconds, simulate=simulate)
    return True


def register_linkedin_action(action_type: str, timestamp: str = "") -> Dict[str, Any]:
    state = _reset_linkedin_state_if_needed(_load_linkedin_state())
    action_key = _linkedin_action_key(action_type)
    now_ts = time.time()
    state["actions"] = _prune_recent_actions(list(state.get("actions") or []), now_ts)
    state["actions"].append(now_ts)
    state[action_key] = int(state.get(action_key, 0) or 0) + 1
    state["last_action_at"] = timestamp or datetime.datetime.now().isoformat()
    _save_linkedin_state(state)
    return state


class ProductionSafety:
    validate_incoming_identity = staticmethod(validate_incoming_identity)
    validate_task_whitelist = staticmethod(validate_task_whitelist)
    find_crm_lead = staticmethod(find_crm_lead)
    enforce_whatsapp_duplication_guard = staticmethod(enforce_whatsapp_duplication_guard)
    confirm_whatsapp_dispatch = staticmethod(confirm_whatsapp_dispatch)
    apply_linkedin_safety = staticmethod(apply_linkedin_safety)
    register_linkedin_action = staticmethod(register_linkedin_action)
