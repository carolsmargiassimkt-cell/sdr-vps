
def _session_guard_allows_inbound(data):
    try:
        import json, os, time
        guard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "whatsapp_session_guard.json")
        if not os.path.exists(guard_path):
            return True
        with open(guard_path, "r", encoding="utf-8") as f:
            guard = json.load(f) or {}
        started_at = int(float(guard.get("session_started_at") or 0))
        if started_at <= 0:
            return True
        ts = data.get("timestamp") or data.get("messageTimestamp") or data.get("ts")
        try:
            msg_ts = int(float(ts))
            if msg_ts > 1000000000000:
                msg_ts = msg_ts // 1000
        except Exception:
            print("[SESSION_GUARD_NO_VALID_TIMESTAMP] bloqueando inbound sem timestamp confiavel")
            return False
        return msg_ts >= (started_at - 300)
    except Exception as e:
        print(f"[SESSION_GUARD_CHECK_ERROR] {e}")
        return True


def _force_positive_intent(text):
    t = (text or "").lower()
    kws = ["ok", "tá bom", "ta bom", "sim", "beleza", "pode ser"]
    return any(k in t for k in kws)
import hashlib
import json
import logging
import os
import re
import sys
import time
import unicodedata
import uuid
from datetime import datetime, timedelta
from threading import Lock
from core.sdr_state import mark_inbound as central_mark_inbound, stop_automation as central_stop_automation, log_event as central_log_event

import requests
from core.agent_router import register_inbound
from flask import Flask, jsonify, request

from crm.pipedrive_client import PipedriveClient
from logic.whatsapp_pitch_engine import WhatsAppPitchEngine
from services.whatsapp_service import WhatsAppService

try:
    from config.config_loader import get_config_value
except Exception:
    def get_config_value(_key, default=""):
        return default


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

pitch = WhatsAppPitchEngine(config=None)
whatsapp = WhatsAppService()
crm = PipedriveClient()
crm.ensure_deal_labels(
    [
        "respondido",
        *[f"WHATSAPP_CAD{i}" for i in range(1, 7)],
        *[f"EMAIL_CAD{i}" for i in range(1, 7)],
    ]
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
BLOCKLIST_FILE = os.path.join(LOGS_DIR, "whatsapp_manual_blocklist.json")
PROCESSED_FILE = os.path.join(BASE_DIR, "inbound_processed.json")
PROCESSED_LOCK_FILE = os.path.join(BASE_DIR, "inbound_processed.json.lock")
HISTORY_FILE = os.path.join(LOGS_DIR, "whatsapp_message_history.json")
WHATSAPP_CONVERSATION_STATE_FILE = os.path.join(DATA_DIR, "whatsapp_conversation_state.json")
EMAIL_PENDING_CONFIRMATIONS_FILE = os.path.join(DATA_DIR, "email_pending_confirmations.json")
EMAIL_QUEUE_FILE = os.path.join(DATA_DIR, "email_handoff_queue.json")
AGENT_DECIDE_UPSTREAM_URL = str(
    os.getenv("AGENT_DECIDE_UPSTREAM_URL")
    or get_config_value("agent_decide_upstream_url", "")
    or ""
).strip()
AGENT_DECIDE_TIMEOUT_SEC = max(
    1.0,
    min(
        15.0,
        float(
            os.getenv("AGENT_DECIDE_TIMEOUT_SEC")
            or get_config_value("agent_decide_timeout_sec", 4)
            or 4
        ),
    ),
)
BOTPRESS_BASE_URL = str(
    os.getenv("BOTPRESS_BASE_URL")
    or get_config_value("botpress_base_url", "http://127.0.0.1:3100")
    or "http://127.0.0.1:3100"
).rstrip("/")
BOTPRESS_BOT_ID = str(
    os.getenv("BOTPRESS_BOT_ID")
    or get_config_value("botpress_bot_id", "default")
    or "default"
).strip() or "default"
BOTPRESS_TIMEOUT_SEC = max(
    1.0,
    min(
        15.0,
        float(
            os.getenv("BOTPRESS_TIMEOUT_SEC")
            or get_config_value("botpress_timeout_sec", 8)
            or 8
        ),
    ),
)


def is_agent_decide_enabled():
    return str(os.getenv("AGENT_DECIDE_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}


def is_auto_reply_router_enabled():
    if str(os.getenv("INBOX_HANDLER_ALLOW_SEND", "0") or "0").strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    return str(os.getenv("AUTO_REPLY_ROUTER_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}


def send_reply(phone, text):
    print(f"[INBOX_SEND_REPLY_BLOCKED_EMISSOR_UNICO] telefone={phone} texto={str(text or '')[:120]}")
    return jsonify({"ok": True, "confirmed": True, "auto_reply_blocked": True, "reason": "single_whatsapp_emitter"})


def blocked_whatsapp_send(phone, text, *args, **kwargs):
    print(f"[INBOX_SEND_BLOCKED_EMISSOR_UNICO] telefone={phone} texto={str(text or '')[:120]}")
    return False


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def is_hard_negative_message(message):
    txt = str(message or "").strip().lower()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    patterns = [
        r"\bnao\s+temos\s+interesse\b",
        r"\bnao\s+tenho\s+interesse\b",
        r"\bnao\s+tenho\s+interes+\b",
        r"\bsem\s+interesse\b",
        r"\bnao\s+queremos\b",
        r"\bnao\s+quero\b",
        r"\bnao\s+me\s+interessa\b",
        r"\bnao\s+gostaria\b",
        r"\bnao\s+preciso\b",
        r"\bnao\s+tenho\s+interesse\s+no\s+momento\b",
        r"\bpode\s+me\s+remover\b",
        r"\btira(r)?\s+meu\s+contato\b",
        r"\bretira(r)?\s+meu\s+numero\b",
        r"\bnao\s+chama(r)?\s+mais\b",
        r"\bpare\b",
        r"\bstop\b",
        r"\bnao\s+tenho\s+interesse,\b",
        r"\bagradeco\b",
        r"\bagradeco\s+mas\s+nao\b",
        r"\bobrigad[oa],?\s+mas\s+nao\b",
        r"\bremove\b",
        r"\bnao\s+me\s+chame\b",
        r"\bnao\s+autorizei\b",
        r"\bsem\s+interesse\b",
        r"\bnao\s+faz\s+sentido\b",
        r"\bnao\s+preciso\b",
    ]
    return any(re.search(p, txt) for p in patterns)

def is_test_whitelist_phone(phone):
    return str(phone) in TEST_WHITELIST

TEST_WHITELIST = {"5535920002020", "35920002020", "5511998804191", "11998804191"}
CRM_CACHE_TTL_SECONDS = 24 * 60 * 60
REPLY_WINDOW_SECONDS = 30 * 60
LEAD_TRAFEGO_LABEL_ID = 193
RESPONDIDO_LABEL_ID = 196
WARM_WHATSAPP_LABEL_ID = 226
GEMINI_MODEL = str(
    os.getenv("GEMINI_MODEL")
    or get_config_value("gemini_model", "gemini-1.5-flash")
    or "gemini-1.5-flash"
).strip() or "gemini-1.5-flash"
lead_cache = {}
reply_guard = {}
reply_guard_lock = Lock()
FORA_HORARIO_STATUS = "aguardando_horario"
FORA_HORARIO_MENSAGEM = "Oi, aqui é a Carol da Mand. Não estou disponível no momento, mas responderei assim que possível 😊"


def dentro_do_horario():
    agora = datetime.now()
    if agora.weekday() <= 4:
        return 9 <= agora.hour < 18
    return False


def _blocklist_entry_phone(item):
    if not isinstance(item, dict):
        return ""
    for field_name in ("telefone", "phone", "number"):
        normalized = whatsapp.normalize_phone(item.get(field_name))
        if normalized:
            return normalized
    return ""


def append_to_blocklist(phone, reason):
    try:
        os.makedirs(LOGS_DIR, exist_ok=True)
        data = []
        if os.path.exists(BLOCKLIST_FILE):
            with open(BLOCKLIST_FILE, "r", encoding="utf-8-sig") as f:
                loaded = json.load(f)
                if isinstance(loaded, list):
                    data = loaded
        normalized_phone = whatsapp.normalize_phone(phone)
        if not normalized_phone:
            return False
        if not any(_blocklist_entry_phone(item) == normalized_phone for item in data if isinstance(item, dict)):
            data.append({
                "telefone": normalized_phone,
                "reason": reason,
                "tag": "blocked_automatic",
                "created_at": datetime.now().isoformat(),
            })
            with open(BLOCKLIST_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"[BLOCKLIST_ADICIONADO] telefone={normalized_phone} motivo={reason}")
            return True
    except Exception as e:
        print(f"[ERRO_BLOCKLIST] {e}")
    return False


def acquire_lock(lock_file, timeout=10):
    started_at = time.time()
    while time.time() - started_at < timeout:
        try:
            return os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            time.sleep(0.05)
    raise TimeoutError(lock_file)


def release_lock(fd, lock_file):
    try:
        os.close(fd)
    except Exception:
        pass
    try:
        os.remove(lock_file)
    except Exception:
        pass


def load_processed():
    if os.path.exists(PROCESSED_FILE):
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, list):
                    return set(loaded)
        except Exception:
            return set()
    try:
        with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)
    except Exception:
        pass
    return set()


def save_processed(items):
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(items), f)


def commit_processed_key(key):
    if not str(key or "").strip():
        return
    fd = None
    try:
        fd = acquire_lock(PROCESSED_LOCK_FILE)
        refreshed = load_processed()
        if key not in refreshed:
            refreshed.add(key)
            save_processed(refreshed)
        processed.clear()
        processed.update(refreshed)
    finally:
        if fd is not None:
            release_lock(fd, PROCESSED_LOCK_FILE)


processed = load_processed()


def load_manual_blocklist():
    try:
        if os.path.exists(BLOCKLIST_FILE):
            with open(BLOCKLIST_FILE, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
                blocked = set()
                for item in list(data or []):
                    normalized = _blocklist_entry_phone(item)
                    if not normalized:
                        continue
                    blocked.update(whatsapp.phone_variants(normalized))
                return blocked
    except Exception:
        pass
    return set()


def load_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    return loaded
    except Exception:
        pass
    return {}


def load_json_file(path, fallback):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, type(fallback)):
                    return loaded
    except Exception:
        pass
    return fallback


def save_json_file(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_email_pending_confirmations():
    payload = load_json_file(EMAIL_PENDING_CONFIRMATIONS_FILE, {})
    return payload if isinstance(payload, dict) else {}


def save_email_pending_confirmations(payload):
    save_json_file(EMAIL_PENDING_CONFIRMATIONS_FILE, payload if isinstance(payload, dict) else {})


def load_email_queue():
    payload = load_json_file(EMAIL_QUEUE_FILE, [])
    return payload if isinstance(payload, list) else []


def save_email_queue(payload):
    save_json_file(EMAIL_QUEUE_FILE, payload if isinstance(payload, list) else [])


def load_whatsapp_conversation_state():
    payload = load_json_file(WHATSAPP_CONVERSATION_STATE_FILE, {})
    return payload if isinstance(payload, dict) else {}


def save_whatsapp_conversation_state(payload):
    save_json_file(WHATSAPP_CONVERSATION_STATE_FILE, payload if isinstance(payload, dict) else {})


def whatsapp_conversation_key(deal_id, phone):
    return f"{int(deal_id or 0)}:{whatsapp.normalize_phone(phone)}"


def _label_token_set(deal):
    try:
        return {str(token or "").strip().upper() for token in crm.resolve_label_tokens((deal or {}).get("label"), crm.get_deal_labels()) if str(token or "").strip()}
    except Exception:
        raw = (deal or {}).get("label")
        if isinstance(raw, list):
            return {str(item.get("id") if isinstance(item, dict) else item).strip().upper() for item in raw if str(item or "").strip()}
        return {item.strip().upper() for item in str(raw or "").split(",") if item.strip()}


def infer_whatsapp_origin(deal, previous_origin=""):
    if str(previous_origin or "").strip().lower() in {"lead_trafego", "warm_email"}:
        return str(previous_origin).strip().lower()
    tokens = _label_token_set(deal)
    if str(LEAD_TRAFEGO_LABEL_ID) in tokens or "LEAD_TRAFEGO" in tokens or "LEAD_TRÁFEGO" in tokens:
        return "lead_trafego"
    if str(WARM_WHATSAPP_LABEL_ID) in tokens or "WARM_WHATSAPP" in tokens or "CLICKED_WARM" in tokens:
        return "warm_email"
    return "warm_email"


def classify_conversation_intent(message):
    text = normalize_intent_text(message)
    if is_opt_out(message) or is_hard_negative_message(message):
        return "opt_out"
    closing_key = detect_closing_intent(message)
    nonlead_intent = str(pitch.detect_nonlead_intent(message) or "").strip().lower()
    if closing_key in {"wrong_number", "unknown_person"} or nonlead_intent == "numero_errado":
        return "wrong_contact"
    if nonlead_intent in {"mensagem_automatica", "menu_ura", "mensagem_institucional", "empresa_incompativel", "setor_errado"}:
        return "automation_blocked"
    if detect_referral_intent(message):
        return "referred"
    if detect_pause_not_now_intent(message):
        return "pause_not_now"
    if any(token in text for token in ("case", "exemplo", "modelo", "como funciona", "manda", "envia")):
        return "asked_case"
    if detect_positive_intent(message) or detect_scheduling_intent(message):
        return "interested"
    if detect_neutral_intent(message):
        return "replied"
    return "replied"


def update_whatsapp_conversation_state_for_lead(lead_state, phone, message="", intent="", last_bot_reply=""):
    normalized_phone = whatsapp.normalize_phone(phone)
    if not normalized_phone:
        return {}
    state = load_whatsapp_conversation_state()
    deals = list(related_deals_from_state(lead_state) or [])
    if not deals:
        deals = [{"id": 0, "label": ""}]
    now = datetime.now().isoformat(timespec="seconds")
    resolved_intent = str(intent or classify_conversation_intent(message) or "replied").strip().lower()
    updated = {}
    for deal in deals:
        deal_id = extract_entity_id((deal or {}).get("id"))
        key = whatsapp_conversation_key(deal_id, normalized_phone)
        previous = state.get(key) if isinstance(state.get(key), dict) else {}
        origin = infer_whatsapp_origin(deal, previous.get("origin"))
        record = {
            "deal_id": int(deal_id),
            "phone": normalized_phone,
            "origin": origin,
            "wa1_sent": bool(previous.get("wa1_sent", False)),
            "replied": bool(previous.get("replied", False) or message),
            "asked_case": bool(previous.get("asked_case", False) or resolved_intent == "asked_case"),
            "interested": bool(previous.get("interested", False) or resolved_intent == "interested"),
            "opt_out": bool(previous.get("opt_out", False) or resolved_intent == "opt_out"),
            "wrong_contact": bool(previous.get("wrong_contact", False) or resolved_intent == "wrong_contact"),
            "referred": bool(previous.get("referred", False) or resolved_intent == "referred"),
            "pause_not_now": bool(previous.get("pause_not_now", False) or resolved_intent == "pause_not_now"),
            "stopped": bool(previous.get("stopped", False) or resolved_intent in {"opt_out", "wrong_contact", "referred", "automation_blocked"}),
            "last_intent": resolved_intent,
            "last_bot_reply": str(last_bot_reply or previous.get("last_bot_reply") or ""),
            "last_lead_reply": str(message or previous.get("last_lead_reply") or ""),
            "updated_at": now,
        }
        if previous.get("wa1_sent_at"):
            record["wa1_sent_at"] = previous.get("wa1_sent_at")
        state[key] = record
        updated[key] = record
        try:
            central_mark_inbound(deal_id, normalized_phone, message, intent=resolved_intent, origin=origin)
        except Exception as exc:
            central_log_event("STATE_TRANSITION", deal_id=deal_id, phone=normalized_phone, error=f"central_state_fail:{exc}")
    save_whatsapp_conversation_state(state)
    return updated


def detect_pause_not_now_intent(message):
    return bool(pitch.detect_pause_not_now_intent(message))


def get_whatsapp_conversation_records_for_lead(lead_state, phone):
    normalized_phone = whatsapp.normalize_phone(phone)
    state = load_whatsapp_conversation_state()
    records = []
    for deal in list(related_deals_from_state(lead_state) or []):
        key = whatsapp_conversation_key(extract_entity_id((deal or {}).get("id")), normalized_phone)
        record = state.get(key)
        if isinstance(record, dict):
            records.append(record)
    return records


def has_stopped_whatsapp_conversation(lead_state, phone):
    return any(bool(record.get("stopped")) for record in get_whatsapp_conversation_records_for_lead(lead_state, phone))


def enrich_lead_with_whatsapp_state(lead, lead_state, phone):
    records = get_whatsapp_conversation_records_for_lead(lead_state, phone)
    if not records:
        return lead
    merged = dict(lead or {})
    active = records[0]
    for record in records:
        if record.get("wa1_sent") or record.get("replied"):
            active = record
            break
    merged.update({
        "conversation_origin": active.get("origin"),
        "conversation_state": active,
        "wa1_sent": bool(active.get("wa1_sent")),
        "last_bot_reply": active.get("last_bot_reply") or "",
        "last_lead_reply": active.get("last_lead_reply") or "",
        "has_whatsapp_history": bool(active.get("wa1_sent") or active.get("replied")),
    })
    return merged


def save_history(history):
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f)


def append_history(phone, direction, message, step=0, source=""):
    history = load_history()
    items = history.get(phone, [])
    items.append({
        "direction": direction,
        "message": str(message or "").strip(),
        "step": int(step or 0),
        "created_at": datetime.now().isoformat(),
        "source": str(source or "").strip().lower(),
    })
    history[phone] = items[-30:]
    save_history(history)
    return history[phone]


def get_history_items(phone):
    normalized_phone = whatsapp.normalize_phone(phone)
    if not normalized_phone:
        return []
    history = load_history()
    items = history.get(normalized_phone, [])
    return items if isinstance(items, list) else []


def has_recent_history_entry(phone, direction, message, within_seconds=600):
    history = load_history().get(phone, [])
    target_direction = str(direction or "").strip().lower()
    target_message = str(message or "").strip()
    if not target_direction or not target_message:
        return False

    now = datetime.now()
    for item in reversed(history):
        if str(item.get("direction") or "").strip().lower() != target_direction:
            continue
        if str(item.get("message") or "").strip() != target_message:
            continue
        created_at = str(item.get("created_at") or "").strip()
        if not created_at:
            return True
        try:
            created = datetime.fromisoformat(created_at)
        except Exception:
            return True
        if (now - created).total_seconds() <= within_seconds:
            return True
    return False


def has_recent_confirmed_outbound_entry(phone, message, within_seconds=600):
    history = load_history().get(phone, [])
    target_message = str(message or "").strip()
    if not target_message:
        return False
    now = datetime.now()
    for item in reversed(history):
        if str(item.get("direction") or "").strip().lower() != "out":
            continue
        if str(item.get("message") or "").strip() != target_message:
            continue
        if str(item.get("source") or "").strip().lower() not in {"sync", "outbound_sync", "webhook_sync", "manual_sync"}:
            continue
        created_at = str(item.get("created_at") or "").strip()
        if not created_at:
            return True
        try:
            created = datetime.fromisoformat(created_at)
        except Exception:
            return True
        if (now - created).total_seconds() <= within_seconds:
            return True
    return False


def can_emit_reply(phone, message, within_seconds=1800):
    normalized_phone = whatsapp.normalize_phone(phone)
    normalized_message = str(message or "").strip()
    if not normalized_phone or not normalized_message:
        return False
    now = time.time()
    with reply_guard_lock:
        # We only block if the EXACT SAME message was sent to the same phone recently
        guard_key = f"{normalized_phone}:{normalized_message}"
        if guard_key in reply_guard:
            last_sent = float(reply_guard[guard_key] or 0)
            if now - last_sent < within_seconds:
                return False

        # Clean up old entries
        expired = [k for k, t in reply_guard.items() if now - float(t or 0) > within_seconds]
        for k in expired:
            reply_guard.pop(k, None)

        reply_guard[guard_key] = now
        return True


def release_reply_guard(phone):
    normalized_phone = whatsapp.normalize_phone(phone)
    if not normalized_phone:
        return
    with reply_guard_lock:
        reply_guard.pop(normalized_phone, None)


def infer_step_from_history(history):
    max_step = 0
    for item in history:
        if str(item.get("direction") or "").strip().lower() != "out":
            continue
        try:
            max_step = max(max_step, int(item.get("step") or 0))
        except Exception:
            continue
    return max(1, min(10, max_step + 1 if max_step else 1))


def infer_step_from_deals(deals):
    if not deals:
        return 1
    options = crm.get_deal_labels()
    max_step = 0
    for deal in deals:
        for token in crm.resolve_label_tokens(deal.get("label"), options):
            match = re.fullmatch(r"CAD(\d+)", str(token or "").strip().upper())
            if match:
                max_step = max(max_step, int(match.group(1)))
    return max(1, min(10, max_step + 1 if max_step else 1))


def resolve_contextual_reply_step(history=None, deals=None, fallback=2):
    history_items = list(history or [])
    structured_history = [item for item in history_items if isinstance(item, dict)]
    step = max(infer_step_from_history(structured_history), infer_step_from_deals(deals or []))
    has_inbound = False
    for item in history_items:
        if isinstance(item, dict):
            direction = str(item.get("direction") or "").strip().lower()
            has_inbound = has_inbound or direction == "in"
        else:
            has_inbound = has_inbound or bool(str(item or "").strip())
    if step <= 1 and has_inbound:
        step = int(fallback or 2)
    return max(2, min(10, int(step or fallback or 2)))


def is_opt_out(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    return any(
        token in normalized
        for token in (
            "pare",
            "não quero",
            "nao quero",
            "não queremos",
            "nao queremos",
            "não temos interesse",
            "nao temos interesse",
            "não tenho interesse",
            "nao tenho interesse",
            "sem interesse",
            "agradeço",
            "agradeco",
            "sair",
            "remove",
            "não me chame",
            "nao me chame",
            "não me procure",
            "nao me procure",
            "não autorizei",
            "nao autorizei",
            "não faz sentido",
            "nao faz sentido",
            "não preciso",
            "nao preciso",
            "obrigado, mas não",
            "obrigado mas nao",
        )
    )


def normalize_intent_text(message):
    text = str(message or "").strip().lower()
    decomposed = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", ascii_text).strip()


def _gemini_api_keys():
    keys = []
    for candidate in (
        os.getenv("GEMINI_API_KEY_1", ""),
        os.getenv("GEMINI_API_KEY_2", ""),
        os.getenv("GEMINI_API_KEY", ""),
        get_config_value("gemini_api_key_primary", ""),
        get_config_value("gemini_api_key_fallback", ""),
        get_config_value("gemini_api_key", ""),
    ):
        token = str(candidate or "").strip()
        if token and token not in keys:
            keys.append(token)
    return keys


def _extract_json_payload(raw_text):
    text = str(raw_text or "").strip()
    if not text:
        return {}
    cleaned = re.sub(r"^```(?:json)?|```$", "", text, flags=re.IGNORECASE).strip()
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        pass
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(0))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _gemini_generate_text(prompt, *, max_output_tokens=120, temperature=0):
    prompt_text = str(prompt or "").strip()
    if not prompt_text:
        return "", "empty_prompt"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": {
            "temperature": float(temperature or 0),
            "maxOutputTokens": max(1, int(max_output_tokens or 120)),
        },
    }
    for api_key in _gemini_api_keys():
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
            response = requests.post(url, params={"key": api_key}, json=payload, timeout=8)
            if int(response.status_code or 0) >= 400:
                continue
            body = response.json() if response.content else {}
            candidates = list((body or {}).get("candidates") or [])
            if not candidates:
                continue
            parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
            for part in parts:
                maybe_text = str((part or {}).get("text") or "").strip()
                if maybe_text:
                    return maybe_text, "gemini"
        except Exception:
            continue
    return "", "missing_key_or_error"


def detect_bot_menu_rule(message):
    raw = str(message or "").strip()
    normalized = normalize_intent_text(raw)
    if not normalized:
        return False, ""
    bot_tokens = (
        "assistente virtual",
        "assistente da",
        "assistente do",
        "atendimento automatico",
        "mensagem automatica",
        "resposta automatica",
        "resposta automatizada",
        "sou um bot",
        "sou uma assistente virtual",
        "estou aqui para ajudar",
        "como podemos ajudar",
        "como posso ajudar",
        "encaminhar para o departamento",
        "encaminhar para um departamento",
        "encaminhar para o setor",
        "departamento responsavel",
        "qual departamento",
        "setor responsavel",
        "falar com nosso time",
        "time de atendimento",
        "bem-vindo",
        "bem vindo",
        "obrigado por entrar em contato",
        "agradecemos seu contato",
        "em breve responderemos",
        "em instantes responderemos",
        "horario de atendimento",
        "fora do horario",
        "menu de atendimento",
        "menu principal",
        "escolha uma opcao",
        "digite uma opcao",
        "digite a opcao",
        "selecione uma opcao",
        "pressione",
        "para falar com",
        "protocolo de atendimento",
        "central de atendimento",
        "chatbot",
    )
    if any(token in normalized for token in bot_tokens):
        return True, "keyword"
    if re.match(r"^bem[- ]vindo(a)?\s+(a|ao|à|a\s+)?", normalized):
        return True, "welcome_auto"
    if re.search(r"\bdigite\s+(?:\d{1,2}|[#*])\b", normalized):
        return True, "digite_option"
    if re.search(r"\b(?:opcao|escolha)\s+(?:\d{1,2}|[#*])\b", normalized):
        return True, "option_keyword"
    option_lines = re.findall(r"(?m)^\s*(?:\d{1,2}|[#*])\s*[\).:\-–]\s+\S+", raw)
    if len(option_lines) >= 2:
        return True, "numbered_menu"
    if len(raw) > 260 and raw.count("\n") >= 3 and re.search(r"(?m)^\s*\d{1,2}\s*[\).:\-–]", raw):
        return True, "long_menu"
    return False, ""


def classify_inbound_intent_gemini(message):
    prompt = (
        "Classifique a mensagem recebida no WhatsApp. Responda APENAS JSON puro "
        "{\"intent\":\"bot_menu|human|unknown\",\"reason\":\"...\"}.\n"
        "Use `bot_menu` para resposta automatica, menu numerado, URA, assistente virtual ou "
        "atendimento automatico em que o SDR nao deve responder livremente.\n"
        "Use `human` para pessoa respondendo livremente.\n"
        "Mensagem:\n"
        f"{str(message or '').strip()[:2000]}"
    )
    raw_text, reason = _gemini_generate_text(prompt, max_output_tokens=80, temperature=0)
    if not raw_text:
        return "unknown", reason
    parsed = _extract_json_payload(raw_text)
    intent = str(parsed.get("intent") or "").strip().lower()
    detail = str(parsed.get("reason") or "").strip() or reason
    if intent in {"bot_menu", "human", "unknown"}:
        return intent, detail
    return "unknown", "invalid_payload"


def choose_bot_menu_option_gemini(message):
    prompt = (
        "Voce recebe um menu/URA de WhatsApp. Escolha a melhor resposta curta e EXATA para navegar "
        "ate um humano, vendas, comercial ou compras. Responda APENAS JSON puro "
        "{\"reply\":\"token_exato\",\"reason\":\"...\"}.\n"
        "Se o menu pedir um numero, devolva so o numero.\n"
        "Se o menu pedir uma palavra, devolva so a palavra exata do menu.\n"
        "Priorize nesta ordem: humano/atendente, comercial/vendas, compras, representante/consultor, sac/atendimento.\n"
        "Nao invente resposta fora do menu.\n"
        "Menu:\n"
        f"{str(message or '').strip()[:2500]}"
    )
    raw_text, reason = _gemini_generate_text(prompt, max_output_tokens=80, temperature=0)
    if not raw_text:
        return "", reason
    parsed = _extract_json_payload(raw_text)
    reply = str(parsed.get("reply") or "").strip()
    detail = str(parsed.get("reason") or "").strip() or reason
    if re.fullmatch(r"[0-9]{1,2}|[#*]|[A-Za-z0-9][A-Za-z0-9 _./-]{0,40}", reply, flags=re.IGNORECASE):
        return reply, detail
    return "", "invalid_payload"


def extract_bot_menu_options(message):
    raw = str(message or "")
    options = []
    seen = set()
    patterns = [
        re.compile(r"(?im)^\s*([0-9]{1,2}|[#*])\s*[\).:\-–]\s*(.+?)\s*$"),
        re.compile(r"(?i)\bdigite\s+([0-9]{1,2}|[#*])\s+para\s+(.+?)(?:[.;\n]|$)"),
        re.compile(r"(?i)\bop(?:cao|ção)\s+([0-9]{1,2}|[#*])\s*[:\-–]?\s*(.+?)(?:[.;\n]|$)"),
        re.compile(r"(?i)\b(?:para|pra)\s+falar\s+com\s+(.+?),?\s*(?:digite|tecle|pressione|responda)\s+[\"']?([0-9]{1,2}|[#*]|[A-Za-z][A-Za-z0-9 _./-]{1,30})[\"']?(?:[.;\n]|$)"),
        re.compile(r"(?i)\b(?:responda|digite)\s+[\"']?([A-Za-z][A-Za-z0-9 _./-]{1,30})[\"']?\s+(?:para|pra)\s+(.+?)(?:[.;\n]|$)"),
    ]
    for pattern in patterns:
        for match in pattern.finditer(raw):
            if "falar\\s+com" in pattern.pattern:
                label = str(match.group(1) or "").strip()
                key = str(match.group(2) or "").strip()
            else:
                key = str(match.group(1) or "").strip()
                label = str(match.group(2) or "").strip()
            normalized_key = f"{key}|{label.lower()}"
            if not key or not label or normalized_key in seen:
                continue
            seen.add(normalized_key)
            options.append({"reply": key, "label": label})
    for keyword in re.findall(r"(?i)\b(?:responda|digite)\s+[\"']?([A-Za-z][A-Za-z0-9 _./-]{1,30})[\"']?", raw):
        token = str(keyword or "").strip()
        normalized_key = f"{token}|keyword"
        if not token or normalized_key in seen:
            continue
        seen.add(normalized_key)
        options.append({"reply": token, "label": token})
    return options


def choose_bot_menu_option(message):
    options = extract_bot_menu_options(message)
    priority_groups = [
        ("atendente", "humano", "pessoa", "representante", "consultor"),
        ("comercial", "vendas", "vendedor", "orcamento", "orçamento", "marketing"),
        ("compras", "comprador"),
        ("sac", "atendimento"),
        ("financeiro",),
    ]
    if options:
        for keywords in priority_groups:
            for option in options:
                label_norm = normalize_intent_text(option.get("label"))
                if any(token in label_norm for token in keywords):
                    return str(option.get("reply") or "").strip(), f"keyword:{'/'.join(keywords)}"

        first_numeric = next((str(option.get("reply") or "").strip() for option in options if re.fullmatch(r"[0-9]{1,2}|[#*]", str(option.get("reply") or "").strip())), "")
        if first_numeric:
            return first_numeric, "fallback:first_option"

        first_keyword = next((str(option.get("reply") or "").strip() for option in options if re.fullmatch(r"[A-Za-z][A-Za-z0-9 _./-]{1,30}", str(option.get("reply") or "").strip())), "")
        if first_keyword:
            return first_keyword, "fallback:first_keyword"

    reply, reason = choose_bot_menu_option_gemini(message)
    if reply:
        return reply, f"gemini:{reason}"

    normalized = normalize_intent_text(message)
    keyword_fallbacks = [
        ("compras", ("compras", "comprador")),
        ("comercial", ("comercial", "vendas", "vendedor", "orcamento", "orçamento")),
        ("atendente", ("atendente", "humano", "pessoa", "consultor", "representante")),
    ]
    for reply_token, keywords in keyword_fallbacks:
        if any(token in normalized for token in keywords):
            return reply_token, f"rule:{reply_token}"
    return "", ""


def handle_bot_menu_navigation(phone, message, lead_state=None):
    reply, reason = choose_bot_menu_option(message)
    append_history(phone, "in", message, step=0)
    if not reply:
        print(f"[BOT_MENU_SEM_ROTA] {phone}")
        return False
    if lead_state is None:
        try:
            lead_state = validate_lead_with_cache(phone)
        except Exception:
            lead_state = None
    if lead_state:
        if has_stopped_whatsapp_conversation(lead_state, phone):
            print(f"[BOT_MENU_STOPPED_NO_REPLY] telefone={phone}")
            return True
        update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="replied")
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")
    print(f"[BOT_MENU_NAVEGACAO] telefone={phone} resposta={reply} motivo={reason}")
    if False and has_recent_history_entry(phone, "out", reply, within_seconds=1800):
        print(f"[BOT_MENU_DUPLICADO] {phone} resposta={reply}")
        return True
    if not can_emit_reply(phone, reply, within_seconds=REPLY_WINDOW_SECONDS):
        print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
        return False
    ok = blocked_whatsapp_send(phone, reply)
    if ok:
        append_history(phone, "out", reply, step=0)
        if lead_state:
            update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="replied", last_bot_reply=reply)
        print(f"[RESPOSTA_ENVIADA] {phone}")
        print(f"[BOT_MENU_RESPOSTA_ENVIADA] {phone} resposta={reply}")
        return True
    else:
        release_reply_guard(phone)
        print(f"[BOT_MENU_FALHA_ENVIO] {phone} resposta={reply}")
        return False


def should_ignore_bot_menu(message):
    if extract_phone_candidates(message, current_phone="") or extract_email_candidates(message):
        return False, "contact_candidate"
    by_rule, rule_reason = detect_bot_menu_rule(message)
    if by_rule:
        return True, f"rule:{rule_reason}"
    normalized = normalize_intent_text(message)
    needs_ai_check = (
        "menu" in normalized
        or "opcao" in normalized
        or "atendimento" in normalized
        or "virtual" in normalized
        or str(message or "").count("\n") >= 2
        or bool(re.search(r"(?m)^\s*\d{1,2}\s*[\).:\-–]", str(message or "")))
    )
    if not needs_ai_check:
        return False, "rule_clean"
    intent, reason = classify_inbound_intent_gemini(message)
    print(f"[INTENT_GEMINI] intent={intent} reason={reason}")
    return intent == "bot_menu", f"gemini:{reason}"


def detect_closing_intent(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    if not normalized:
        return ""

    no_interest_tokens = (
        "sem interesse",
        "sem interesse nisso",
        "não temos interesse",
        "nao temos interesse",
        "não tenho interesse",
        "nao tenho interesse",
        "não me interessa",
        "nao me interessa",
        "não queremos",
        "nao queremos",
        "não quero",
        "nao quero",
        "nao queroo",
        "nao tenhoo interesse",
        "não tenho interesse nisso",
        "nao tenho interesse nisso",
        "não é interesse",
        "nao e interesse",
        "não faz sentido",
        "nao faz sentido",
        "não vamos fazer campanha",
        "nao vamos fazer campanha",
        "não vamos fazer campanhas",
        "nao vamos fazer campanhas",
        "não faremos campanha",
        "nao faremos campanha",
        "não vamos rodar campanha",
        "nao vamos rodar campanha",
        "pode encerrar",
        "pode parar",
        "não precisa",
        "nao precisa",
        "agradeço",
        "agradeco",
        "remove",
        "pare",
        "não me chame",
        "nao me chame",
        "não autorizei",
        "nao autorizei",
        "obrigado, mas não",
        "obrigado mas nao",
    )
    if any(token in normalized for token in no_interest_tokens):
        return "no_interest"

    wrong_number_tokens = (
        "numero errado",
        "número errado",
        "telefone errado",
        "ligou errado",
        "falou com a pessoa errada",
        "não sou dessa empresa",
        "nao sou dessa empresa",
        "não trabalho aí",
        "nao trabalho ai",
        "não conheço essa empresa",
        "nao conheco essa empresa",
        "empresa errada",
        "não é daqui",
        "nao e daqui",
        "não faço parte",
        "nao faco parte",
        "sou ex funcionário",
        "sou ex funcionario",
        "não tenho vínculo",
        "nao tenho vinculo",
        "desconheço",
        "desconheco",
    )
    if any(token in normalized for token in wrong_number_tokens):
        return "wrong_number"

    unknown_person_tokens = (
        "não conheço",
        "nao conheco",
        "não sei quem é",
        "nao sei quem e",
        "não é comigo",
        "nao e comigo",
        "não sou eu",
        "nao sou eu",
    )
    if any(token in normalized for token in unknown_person_tokens):
        return "unknown_person"

    return ""


def detect_positive_intent(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    if not normalized:
        return False
    positive_tokens = (
        "sou eu",
        "sou sim",
        "eu sou",
        "sim sou eu",
        "sim, sou eu",
        "sim sou",
        "pode falar comigo",
        "pode falar",
        "pode sim",
        "fala comigo",
        "pode me falar",
        "eu cuido",
        "eu respondo",
        "sou o responsavel",
        "sou a responsavel",
        "sou o responsável",
        "sou a responsável",
        "cuido do marketing",
        "cuido de marketing",
        "cuido do comercial",
        "cuido de vendas",
        "respondo pelo marketing",
        "respondo por marketing",
        "respondo pelo comercial",
        "respondo por vendas",
        "tenho interesse",
        "tenho sim interesse",
        "quero ver",
        "pode me explicar",
        "pode explicar",
        "me explica",
        "quero entender",
        "como funciona",
        "gostaria de entender",
        "manda mais informacao",
        "manda mais informação",
        "quero saber mais",
        "sim",
        "pode",
        "quero",
        "quero saber",
        "pode me mandar",
        "faz sentido",
        "podemos conversar",
        "ok",
        "tá bom",
        "ta bom",
        "beleza",
        "pode ser",
        "com certeza",
        "claro",
    )
    return any(token in normalized for token in positive_tokens)


def detect_neutral_intent(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    if not normalized:
        return False
    neutral_tokens = (
        "quem é",
        "quem e",
        "quem fala",
        "do que se trata",
        "como assim",
        "sobre o que é",
        "sobre o que e",
        "qual assunto",
        "do que se trata isso",
        "obrigado",
        "obrigada",
        "agradeço",
        "agradeco",
        "tenha um bom dia",
        "tenha um ótimo dia",
        "tenha um otimo dia",
        "boa semana",
        "bom final de semana",
        "ótimo final de semana",
        "otimo final de semana",
        "bom fds",
        "vlw",
        "valeu",
    )
    return any(token in normalized for token in neutral_tokens)


def normalize_stage_text(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text).strip().lower()
    return re.sub(r"\s+", " ", text)


def detect_scheduling_intent(message):
    normalized = normalize_stage_text(message)
    if not normalized:
        return False
    tokens = (
        "pode agendar",
        "vamos agendar",
        "marcar reuniao",
        "marcar uma reuniao",
        "agenda",
        "agendar",
        "call",
        "reuniao",
        "horario",
        "convite",
        "meu email",
        "meu e mail",
    )
    return any(token in normalized for token in tokens)


def classify_business_intent_gemini(message):
    prompt = (
        "Classifique a mensagem comercial em exatamente um rotulo: "
        "nao_interessado, numero_errado, empresa_incompativel, agendamento, interesse, duvida, neutro. "
        "Responda somente com o rotulo.\n"
        "Regras:\n"
        "- Se houver qualquer sinal claro de sem interesse, escolha `nao_interessado`.\n"
        "- Se houver indicio claro de numero/pessoa errada, escolha `numero_errado`.\n"
        "- Se a empresa/setor for incompatível, escolha `empresa_incompativel`.\n"
        "- Se houver proposta/aceite de horario, dia, reuniao ou call, escolha `agendamento`.\n"
        "- Se houver abertura para conversar, escolha `interesse`.\n"
        "- Se a pessoa estiver pedindo explicacao, escolha `duvida`.\n"
        "- Se nada estiver claro, escolha `neutro`.\n\n"
        f"Mensagem: {str(message or '').strip()[:2000]}"
    )
    raw_text, reason = _gemini_generate_text(prompt, max_output_tokens=24, temperature=0)
    label = normalize_intent_text(raw_text)
    if "nao_interessado" in label or "sem interesse" in label:
        return "nao_interessado", reason
    if "numero_errado" in label or "numero errado" in label:
        return "numero_errado", reason
    if "empresa_incompativel" in label or "empresa incompativel" in label:
        return "empresa_incompativel", reason
    if "agendamento" in label:
        return "agendamento", reason
    if "interesse" in label:
        return "interesse", reason
    if "duvida" in label:
        return "duvida", reason
    if "neutro" in label:
        return "neutro", reason
    return "", reason


def build_agent_decision(data):
    payload = dict(data or {})
    text = str(payload.get("text") or payload.get("message") or "").strip()
    channel = str(payload.get("channel") or payload.get("canal") or "whatsapp").strip().lower() or "whatsapp"
    phone = whatsapp.normalize_phone(payload.get("phone") or payload.get("telefone") or "")
    history = payload.get("history") if isinstance(payload.get("history"), list) else []
    lead_name = str(payload.get("name") or payload.get("nome") or "").strip()
    lead_company = str(payload.get("company") or payload.get("empresa") or "").strip()
    generic_names = {"", "cliente", "lead", "contato", "responsavel", "responsável"}
    lead = {"name": lead_name, "nome": lead_name, "empresa": lead_company, "phone": phone, "telefone": phone}
    reason = "rule_fallback"
    confidence = 0.75
    reply = ""
    action = "reply_with_pitch"
    intent = "neutro"

    closing_key = detect_closing_intent(text)
    nonlead_intent = str(pitch.detect_nonlead_intent(text) or "").strip().lower()
    stateful_intent = str(pitch.detect_stateful_intent(text) or "").strip().lower()

    if is_hard_negative_message(text) or is_opt_out(text) or closing_key == "no_interest" or nonlead_intent == "nao_interessado":
        intent = "nao_interessado"
        action = "close_and_block"
        reply = "Entendi, obrigado por avisar. Vou retirar você daqui para não incomodar."
        reason = "hard_negative"
        confidence = 0.99
    elif closing_key in {"wrong_number", "unknown_person"} or nonlead_intent == "numero_errado":
        intent = "numero_errado"
        action = "close_and_block"
        reply = "Entendi, desculpa pelo contato. Vou retirar você daqui para não incomodar."
        reason = f"closing:{closing_key}" if closing_key else f"nonlead:{nonlead_intent}"
        confidence = 0.99
    elif detect_referral_intent(text):
        intent = "indicacao"
        action = "close_and_block"
        reply = "Perfeito, obrigado pela indicação. Vou seguir por esse contato e não te incomodo mais por aqui."
        reason = "referral_rule"
        confidence = 0.98
    elif detect_pause_not_now_intent(text):
        intent = "pause_not_now"
        action = "pause_automation"
        reply = ""
        reason = "pause_not_now_rule"
        confidence = 0.97
        print(f"[PAUSE_NOT_NOW_CLASSIFIED] source=agent_decide texto={text}")
    elif nonlead_intent in {"mensagem_automatica", "menu_ura", "mensagem_institucional", "empresa_incompativel", "setor_errado"}:
        intent = str(nonlead_intent or "empresa_incompativel")
        action = "pause_automation"
        reason = f"nonlead:{nonlead_intent}"
        confidence = 0.98
    elif detect_scheduling_intent(text) or stateful_intent.startswith("agendamento"):
        intent = "agendamento"
        action = "schedule"
        reply = "Perfeito. Me fala só o melhor horário para eu organizar isso direitinho."
        reason = "schedule_rule"
        confidence = 0.95
    elif detect_positive_intent(text):
        intent = "interesse"
        action = "continue_conversation"
        if normalize_intent_text(text) in {"pode sim", "sou eu", "sim", "claro", "pode falar", "pode", "fala", "pode perguntar"} and normalize_intent_text(lead_name) in generic_names:
            reply = "Perfeito. Como posso te chamar?"
        else:
            inbound_messages = [str(item or "") for item in history if str(item or "").strip()] or [text]
            reply = pitch.build_reply(lead, inbound_messages, current_step=resolve_contextual_reply_step(history), allow_opening=False)
        reason = "positive_rule"
        confidence = 0.93
    elif detect_neutral_intent(text):
        intent = "duvida"
        action = "hold"
        reply = ""
        reason = "neutral_rule"
        confidence = 0.9
    else:
        gemini_intent, gemini_reason = classify_business_intent_gemini(text)
        if gemini_intent:
            intent = gemini_intent
            reason = f"gemini:{gemini_reason}"
            confidence = 0.72
        
        if intent == "nao_interessado":
            action = "close_and_block"
            reply = "Entendi, obrigado por avisar. Vou retirar você daqui para não incomodar."
        elif intent == "numero_errado":
            action = "close_and_block"
            reply = "Entendi, desculpa pelo contato. Vou retirar você daqui para não incomodar."
        elif intent == "indicacao":
            action = "close_and_block"
            reply = "Perfeito, obrigado pela indicação. Vou seguir por esse contato e não te incomodo mais por aqui."
        elif intent == "empresa_incompativel":
            action = "pause_automation"
            reply = ""
        elif intent == "agendamento":
            action = "schedule"
            reply = "Perfeito. Me fala só o melhor horário para eu organizar isso direitinho."
        elif intent == "interesse":
            action = "continue_conversation"
            inbound_messages = [str(item or "") for item in history if str(item or "").strip()] or [text]
            reply = pitch.build_reply(lead, inbound_messages, current_step=resolve_contextual_reply_step(history), allow_opening=False)
        else:
            action = "hold"
            reply = ""

    decision = {
        "intent": intent,
        "action": action,
        "reply": str(reply or "").strip(),
        "confidence": float(confidence),
        "reason": reason,
        "channel": channel,
        "phone": phone,
        "deal_id": int(payload.get("deal_id") or 0),
    }
    print(
        f"[AGENT_DECIDE] canal={channel} deal={decision['deal_id']} "
        f"intent={intent} action={action} confidence={confidence:.2f} reason={reason}"
    )
    return decision


def build_local_agent_decision(data):
    payload = dict(data or {})
    text = str(payload.get("message_text") or payload.get("text") or payload.get("message") or "").strip()
    channel = str(payload.get("channel") or payload.get("canal") or "whatsapp").strip().lower() or "whatsapp"
    phone = whatsapp.normalize_phone(payload.get("phone") or payload.get("telefone") or "")
    email = str(payload.get("email") or "").strip().lower()
    history = payload.get("conversation_history") if isinstance(payload.get("conversation_history"), list) else []
    if not history and isinstance(payload.get("history"), list):
        history = payload.get("history")
    lead_name = str(payload.get("name") or payload.get("nome") or "").strip()
    lead_company = str(payload.get("company") or payload.get("empresa") or "").strip()
    source = str(payload.get("source") or "").strip().lower()
    cadence_step = max(0, int(payload.get("cadence_step") or 0))
    current_tags = {str(item or "").strip().upper() for item in list(payload.get("current_tags") or []) if str(item or "").strip()}
    status_bot = normalize_intent_text(payload.get("status_bot") or "")
    generic_names = {"", "cliente", "lead", "contato", "responsavel", "responsÃ¡vel"}
    lead = {"name": lead_name, "nome": lead_name, "empresa": lead_company, "phone": phone, "telefone": phone}
    reason = "rule_fallback"
    confidence = 0.75
    reply = ""
    action = "reply_with_pitch"
    intent = "neutro"
    should_send = channel == "whatsapp"
    should_blocklist = False
    should_pause_automation = False
    should_move_stage = False
    should_create_activity = False

    blocked_current_tags = {
        "RESPONDIDO",
        "CONVERSANDO",
        "SEM_INTERESSE",
        "SEM INTERESSE",
        "NUMERO_ERRADO",
        "NÚMERO_ERRADO",
        "NUMERO ERRADO",
        "CONTATO_INDICADO",
        "CONTATO INDICADO",
        "LEAD_ENCERRADO",
        "LEAD ENCERRADO",
    }
    if (current_tags & blocked_current_tags) or status_bot in {"encerrado", "lead_sem_interesse", "numero_nao_corresponde", "stop"}:
        print(f"[AUTO_REPLY_BLOQUEADO_TAG_CRM] telefone={phone} tags={sorted(current_tags & blocked_current_tags)} status_bot={status_bot}")
        intent = "lead_encerrado"
        action = "hold"
        reason = "lead_closed"
        confidence = 0.99
        should_send = False
        should_pause_automation = True
    elif not text and source.startswith("email_outbound"):
        intent = "cadencia_email"
        action = "continue_cadence"
        reason = "email_outbound_preflight"
        confidence = 0.84
        should_send = True
    elif not text and cadence_step > 0:
        intent = "cadencia_aberta"
        action = "continue_cadence"
        reason = "cadence_context_only"
        confidence = 0.7
        should_send = channel == "email"
    else:
        closing_key = detect_closing_intent(text)
        nonlead_intent = str(pitch.detect_nonlead_intent(text) or "").strip().lower()
        stateful_intent = str(pitch.detect_stateful_intent(text) or "").strip().lower()

        if is_hard_negative_message(text) or is_opt_out(text) or closing_key == "no_interest" or nonlead_intent == "nao_interessado":
            intent = "nao_interessado"
            action = "close_and_block"
            reply = "Entendi, obrigado por avisar. Vou retirar você daqui para não incomodar."
            reason = "hard_negative"
            confidence = 0.99
            should_blocklist = True
            should_pause_automation = True
            should_move_stage = True
            should_send = bool(reply)
        elif closing_key in {"wrong_number", "unknown_person"} or nonlead_intent == "numero_errado":
            intent = "numero_errado"
            action = "close_and_block"
            reply = "Entendi, desculpa pelo contato. Vou retirar você daqui para não incomodar."
            reason = f"closing:{closing_key}" if closing_key else f"nonlead:{nonlead_intent}"
            confidence = 0.99
            should_blocklist = True
            should_pause_automation = True
            should_move_stage = True
            should_send = bool(reply)
        elif detect_referral_intent(text):
            intent = "indicacao"
            action = "close_and_block"
            reply = "Perfeito, obrigado pela indicação. Vou seguir por esse contato e não te incomodo mais por aqui."
            reason = "referral_rule"
            confidence = 0.98
            should_blocklist = True
            should_pause_automation = True
            should_move_stage = True
            should_send = bool(reply)
        elif detect_pause_not_now_intent(text):
            intent = "pause_not_now"
            action = "pause_automation"
            reply = ""
            reason = "pause_not_now_rule"
            confidence = 0.97
            should_pause_automation = True
            should_create_activity = True
            should_send = False
            print(f"[PAUSE_NOT_NOW_CLASSIFIED] source=build_agent_decision texto={text}")
        elif nonlead_intent in {"mensagem_automatica", "menu_ura", "mensagem_institucional", "empresa_incompativel", "setor_errado"}:
            intent = str(nonlead_intent or "empresa_incompativel")
            action = "pause_automation"
            reason = f"nonlead:{nonlead_intent}"
            confidence = 0.98
            should_pause_automation = True
            should_send = False
        elif detect_scheduling_intent(text) or stateful_intent.startswith("agendamento"):
            intent = "agendamento"
            action = "schedule"
            reply = "Perfeito. Me fala só o melhor horário para eu organizar isso direitinho."
            reason = "schedule_rule"
            confidence = 0.95
            should_move_stage = True
            should_create_activity = True
            should_send = bool(reply)
        elif detect_positive_intent(text):
            intent = "interesse"
            action = "continue_conversation"
            if normalize_intent_text(text) in {"pode sim", "sou eu", "sim", "claro", "pode falar", "pode", "fala", "pode perguntar"} and normalize_intent_text(lead_name) in generic_names:
                reply = "Perfeito. Como posso te chamar?"
            else:
                inbound_messages = [str((item or {}).get("message") or item or "").strip() for item in history if str((item or {}).get("message") or item or "").strip()] or [text]
                reply = pitch.build_reply(lead, inbound_messages, current_step=resolve_contextual_reply_step(history), allow_opening=False)
            reason = "positive_rule"
            confidence = 0.93
            should_pause_automation = True
            should_send = bool(reply)
        elif detect_neutral_intent(text):
            intent = "duvida"
            action = "hold"
            reply = ""
            reason = "neutral_rule"
            confidence = 0.9
            should_pause_automation = True
            should_send = False
        else:
            gemini_intent, gemini_reason = classify_business_intent_gemini(text)
            if gemini_intent:
                intent = gemini_intent
                reason = f"gemini:{gemini_reason}"
                confidence = 0.72
            
            if intent == "nao_interessado":
                action = "close_and_block"
                reply = "Entendi, obrigado por avisar. Vou retirar você daqui para não incomodar."
                should_blocklist = True
                should_pause_automation = True
                should_move_stage = True
                should_send = bool(reply)
            elif intent == "numero_errado":
                action = "close_and_block"
                reply = "Entendi, desculpa pelo contato. Vou retirar você daqui para não incomodar."
                should_blocklist = True
                should_pause_automation = True
                should_move_stage = True
                should_send = bool(reply)
            elif intent == "indicacao":
                action = "close_and_block"
                reply = "Perfeito, obrigado pela indicação. Vou seguir por esse contato e não te incomodo mais por aqui."
                should_blocklist = True
                should_pause_automation = True
                should_move_stage = True
                should_send = bool(reply)
            elif intent == "pause_not_now":
                action = "pause_automation"
                reply = ""
                should_pause_automation = True
                should_create_activity = True
                should_send = False
            elif intent == "empresa_incompativel":
                action = "pause_automation"
                should_pause_automation = True
                should_send = False
            elif intent == "agendamento":
                action = "schedule"
                reply = "Perfeito. Me fala só o melhor horário para eu organizar isso direitinho."
                should_move_stage = True
                should_create_activity = True
                should_send = bool(reply)
            elif intent == "interesse":
                action = "continue_conversation"
                inbound_messages = [str((item or {}).get("message") or item or "").strip() for item in history if str((item or {}).get("message") or item or "").strip()] or [text]
                reply = pitch.build_reply(lead, inbound_messages, current_step=resolve_contextual_reply_step(history), allow_opening=False)
                should_pause_automation = True
                should_send = bool(reply)
            else:
                action = "hold"
                reply = ""
                should_pause_automation = False
                should_send = False

    return {
        "intent": intent,
        "confidence": float(confidence),
        "action": action,
        "reply": str(reply or "").strip(),
        "should_send": bool(should_send),
        "should_blocklist": bool(should_blocklist),
        "should_pause_automation": bool(should_pause_automation),
        "should_move_stage": bool(should_move_stage),
        "should_create_activity": bool(should_create_activity),
        "reason": reason,
        "channel": channel,
        "phone": phone,
        "email": email,
        "deal_id": int(payload.get("deal_id") or 0),
        "person_id": int(payload.get("person_id") or 0),
        "org_id": int(payload.get("org_id") or 0),
        "message_text": text,
        "source": source,
    }


def _normalize_agent_decision_payload(raw_payload, fallback):
    payload = dict(raw_payload or {})
    normalized = dict(fallback or {})
    normalized["intent"] = str(payload.get("intent") or fallback.get("intent") or "neutro").strip().lower() or "neutro"
    normalized["confidence"] = float(payload.get("confidence") or fallback.get("confidence") or 0.0)
    normalized["action"] = str(payload.get("action") or fallback.get("action") or "hold").strip().lower() or "hold"
    normalized["reply"] = str(payload.get("reply") or fallback.get("reply") or "").strip()
    normalized["should_send"] = _as_bool(payload.get("should_send", fallback.get("should_send")))
    normalized["should_blocklist"] = _as_bool(payload.get("should_blocklist", fallback.get("should_blocklist")))
    normalized["should_pause_automation"] = _as_bool(payload.get("should_pause_automation", fallback.get("should_pause_automation")))
    normalized["should_move_stage"] = _as_bool(payload.get("should_move_stage", fallback.get("should_move_stage")))
    normalized["should_create_activity"] = _as_bool(payload.get("should_create_activity", fallback.get("should_create_activity")))
    normalized["reason"] = str(payload.get("reason") or fallback.get("reason") or "").strip()
    return normalized


def _extract_botpress_texts(body):
    texts = []
    responses = list((body or {}).get("responses") or [])
    for item in responses:
        if not isinstance(item, dict):
            continue
        for field_name in ("text", "reply", "message"):
            value = str(item.get(field_name) or "").strip()
            if value:
                texts.append(value)
                break
        payload = item.get("payload")
        if isinstance(payload, dict):
            value = str(payload.get("text") or payload.get("reply") or "").strip()
            if value:
                texts.append(value)
    return texts


def build_botpress_agent_decision(data):
    payload = dict(data or {})
    fallback = build_local_agent_decision(payload)
    message_text = str(payload.get("message_text") or payload.get("text") or payload.get("message") or "").strip()
    channel = str(payload.get("channel") or "whatsapp").strip().lower() or "whatsapp"
    phone = whatsapp.normalize_phone(payload.get("phone") or payload.get("telefone") or "")
    email = str(payload.get("email") or "").strip().lower()
    identity = phone or email or str(payload.get("deal_id") or payload.get("person_id") or "lead")
    botpress_payload = {
        "userId": identity,
        "message": message_text or f"[{channel}]",
        "name": str(payload.get("name") or payload.get("nome") or "").strip(),
        "channel": channel,
        "deal_id": int(payload.get("deal_id") or 0),
        "person_id": int(payload.get("person_id") or 0),
        "org_id": int(payload.get("org_id") or 0),
        "source": str(payload.get("source") or "").strip(),
        "metadata": {
            "conversation_history": payload.get("conversation_history") or [],
            "current_tags": payload.get("current_tags") or [],
            "status_bot": payload.get("status_bot") or "",
            "cadence_step": int(payload.get("cadence_step") or 0),
        },
    }
    url = f"{BOTPRESS_BASE_URL}/api/v1/bots/{BOTPRESS_BOT_ID}/converse"
    print(
        f"[BOTPRESS_AGENT_REQUEST] channel={channel} deal={int(payload.get('deal_id') or 0)} "
        f"person={int(payload.get('person_id') or 0)} text_len={len(message_text)} url={url}"
    )
    response = requests.post(url, json=botpress_payload, timeout=BOTPRESS_TIMEOUT_SEC)
    if int(response.status_code or 0) != 200:
        raise RuntimeError(f"botpress_status_{int(response.status_code or 0)}")
    body = response.json() if response.content else {}
    texts = _extract_botpress_texts(body)
    parsed = {}
    for candidate in texts:
        parsed = _extract_json_payload(candidate)
        if parsed:
            break
    if parsed:
        decision = _normalize_agent_decision_payload(parsed, fallback)
        print(
            f"[BOTPRESS_AGENT_RESPONSE] channel={channel} deal={decision.get('deal_id') or 0} "
            f"intent={decision.get('intent')} action={decision.get('action')} mode=json"
        )
        return decision
    reply = "\n".join(texts).strip()
    decision = dict(fallback)
    if reply:
        decision["reply"] = reply
        decision["should_send"] = bool(reply) if channel == "whatsapp" else bool(decision.get("should_send"))
        decision["reason"] = "botpress_text_reply"
    print(
        f"[BOTPRESS_AGENT_RESPONSE] channel={channel} deal={decision.get('deal_id') or 0} "
        f"intent={decision.get('intent')} action={decision.get('action')} mode=text"
    )
    return decision


def resolve_agent_decision(data, *, caller="unknown"):
    payload = dict(data or {})
    fallback = build_local_agent_decision(payload)
    channel = str(payload.get("channel") or "whatsapp").strip().lower() or "whatsapp"
    message_text = str(payload.get("message_text") or payload.get("message") or payload.get("text") or "").strip()
    print(
        f"[AGENT_DECIDE_REQUEST] caller={caller} channel={channel} deal={int(payload.get('deal_id') or 0)} "
        f"person={int(payload.get('person_id') or 0)} source={str(payload.get('source') or '-').strip() or '-'} "
        f"text_len={len(message_text)} enabled={1 if is_agent_decide_enabled() else 0}"
    )

    upstream_url = AGENT_DECIDE_UPSTREAM_URL.rstrip("/")
    if not is_agent_decide_enabled() or not upstream_url:
        decision = dict(fallback)
        print(
            f"[AGENT_DECIDE_RESPONSE] caller={caller} channel={channel} deal={decision.get('deal_id') or 0} "
            f"intent={decision.get('intent')} action={decision.get('action')} "
            f"should_send={1 if decision.get('should_send') else 0} source=local"
        )
        return decision

    if upstream_url in {"http://127.0.0.1:5001/agent/decide", "http://localhost:5001/agent/decide"}:
        print(f"[AGENT_DECIDE_FALLBACK] caller={caller} motivo=self_targeted_upstream")
        decision = dict(fallback)
        print(
            f"[AGENT_DECIDE_RESPONSE] caller={caller} channel={channel} deal={decision.get('deal_id') or 0} "
            f"intent={decision.get('intent')} action={decision.get('action')} "
            f"should_send={1 if decision.get('should_send') else 0} source=local"
        )
        return decision

    try:
        response = requests.post(upstream_url, json=payload, timeout=AGENT_DECIDE_TIMEOUT_SEC)
        if int(response.status_code or 0) != 200:
            raise RuntimeError(f"status_{int(response.status_code or 0)}")
        body = response.json() if response.content else {}
        candidate = body.get("decision") if isinstance(body, dict) and isinstance(body.get("decision"), dict) else body
        decision = _normalize_agent_decision_payload(candidate, fallback)
        print(
            f"[AGENT_DECIDE_RESPONSE] caller={caller} channel={channel} deal={decision.get('deal_id') or 0} "
            f"intent={decision.get('intent')} action={decision.get('action')} "
            f"should_send={1 if decision.get('should_send') else 0} source=upstream"
        )
        return decision
    except Exception as exc:
        print(f"[AGENT_DECIDE_FALLBACK] caller={caller} motivo={exc}")
        decision = dict(fallback)
        print(
            f"[AGENT_DECIDE_RESPONSE] caller={caller} channel={channel} deal={decision.get('deal_id') or 0} "
            f"intent={decision.get('intent')} action={decision.get('action')} "
            f"should_send={1 if decision.get('should_send') else 0} source=fallback_local"
        )
        return decision


def build_agent_decision(data):
    return resolve_agent_decision(data, caller="route")


def resolve_stage_by_keywords(*keyword_groups):
    try:
        stages = crm.get_stages()
    except Exception:
        stages = []
    for stage in list(stages or []):
        if not isinstance(stage, dict):
            continue
        try:
            pipeline_id = int(stage.get("pipeline_id") or 0)
        except Exception:
            pipeline_id = 0
        if pipeline_id != 2:
            continue
        stage_name = normalize_stage_text(stage.get("name"))
        for keywords in keyword_groups:
            if all(str(keyword or "").strip().lower() in stage_name for keyword in keywords):
                return int(stage.get("id") or 0)
    return 0


def resolve_terminal_stage_id():
    return (
        resolve_stage_by_keywords(("perdid",),)
        or resolve_stage_by_keywords(("arquiv",),)
        or resolve_stage_by_keywords(("falta",),)
    )


def move_related_deals_to_terminal_stage(lead_state, reason=""):
    stage_id = resolve_terminal_stage_id()
    if not stage_id:
        print(f"[STAGE_PERDIDO_FALHA] motivo=stage_nao_encontrado reason={reason or '-'}")
        return False
    moved = False
    for deal in related_deals_from_state(lead_state):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = bool(crm.update_stage(deal_id=deal_id, stage_id=stage_id))
        except Exception as exc:
            print(f"[STAGE_PERDIDO_FALHA] deal={deal_id} erro={exc}")
            ok = False
        if ok:
            moved = True
            print(f"[STAGE_PERDIDO_OK] deal={deal_id} stage={stage_id} reason={reason or '-'}")
    return moved


def move_related_deals_to_scheduled(lead_state):
    stage_id = resolve_stage_by_keywords(("agend",), ("reuniao",), ("reuni",))
    if not stage_id:
        print("[STAGE_AGENDADO_FALHA] motivo=stage_nao_encontrado")
        return False
    moved = False
    for deal in related_deals_from_state(lead_state):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = bool(crm.update_stage(deal_id=deal_id, stage_id=stage_id))
        except Exception as exc:
            print(f"[STAGE_AGENDADO_FALHA] deal={deal_id} erro={exc}")
            ok = False
        if ok:
            moved = True
            print(f"[STAGE_AGENDADO_OK] deal={deal_id} stage={stage_id}")
    return moved


def create_scheduling_activity_for_lead(lead_state, summary):
    current_person = dict((lead_state or {}).get("person") or {})
    person_id = extract_entity_id(current_person.get("id"))
    due_date = datetime.now().date().isoformat()
    note_text = str(summary or "").strip() or "Lead pediu agendamento."
    created = False
    for deal in related_deals_from_state(lead_state):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = bool(
                crm.create_activity(
                    subject="Reuniao solicitada por inbound",
                    type="meeting",
                    deal_id=deal_id,
                    person_id=person_id,
                    note=note_text,
                    due_date=due_date,
                    done=0,
                )
            )
        except Exception as exc:
            print(f"[ATIVIDADE_AGENDAMENTO_FALHA] deal={deal_id} erro={exc}")
            ok = False
        if ok:
            created = True
            print(f"[ATIVIDADE_AGENDAMENTO_OK] deal={deal_id} person={person_id}")
    return created


def create_soft_negative_followup_for_lead(lead_state, summary, days=7):
    current_person = dict((lead_state or {}).get("person") or {})
    person_id = extract_entity_id(current_person.get("id"))
    due_date = (datetime.now().date() + timedelta(days=max(1, int(days or 7)))).isoformat()
    note_text = str(summary or "").strip() or "Lead pediu contato em outro momento."
    created = False
    for deal in related_deals_from_state(lead_state):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = bool(
                crm.create_activity(
                    subject="Follow-up leve: lead pediu contato depois",
                    type="task",
                    deal_id=deal_id,
                    person_id=person_id,
                    note=note_text,
                    due_date=due_date,
                    done=0,
                )
            )
        except Exception as exc:
            print(f"[ATIVIDADE_PAUSE_NOT_NOW_FALHA] deal={deal_id} erro={exc}")
            ok = False
        if ok:
            created = True
            print(f"[ATIVIDADE_PAUSE_NOT_NOW_OK] deal={deal_id} person={person_id} due={due_date}")
    return created


def related_deals_from_state(lead_state):
    current_person = dict((lead_state or {}).get("person") or {})
    current_person_id = extract_entity_id(current_person.get("id"))
    current_org_id = extract_entity_id(current_person.get("org_id"))
    if current_person_id > 0 or current_org_id > 0:
        return crm.find_open_deals_by_person_or_org(person_id=current_person_id, org_id=current_org_id)
    return []


def clear_email_runtime_state(*, deal_ids=None, person_id=0, email="", phone=""):
    target_deal_ids = {int(item or 0) for item in list(deal_ids or []) if int(item or 0) > 0}
    target_person_id = int(person_id or 0)
    target_email = str(email or "").strip().lower()
    target_phone = whatsapp.normalize_phone(phone)
    removed_pending = 0
    removed_queue = 0

    pending = load_email_pending_confirmations()
    filtered_pending = {}
    for event_id, item in dict(pending or {}).items():
        payload = dict(item or {})
        item_deal_id = int(payload.get("deal_id") or 0)
        item_person_id = int(payload.get("person_id") or 0)
        item_email = str(payload.get("email") or "").strip().lower()
        item_phone = whatsapp.normalize_phone(payload.get("phone") or payload.get("telefone") or "")
        matches = (
            (target_deal_ids and item_deal_id in target_deal_ids)
            or (target_person_id > 0 and item_person_id == target_person_id)
            or (target_email and item_email == target_email)
            or (target_phone and item_phone == target_phone)
        )
        if matches:
            removed_pending += 1
            continue
        filtered_pending[str(event_id)] = payload
    if removed_pending:
        save_email_pending_confirmations(filtered_pending)

    queue = load_email_queue()
    filtered_queue = []
    for item in list(queue or []):
        payload = dict(item or {})
        item_deal_id = int(payload.get("id") or payload.get("deal_id") or 0)
        item_person_id = int(payload.get("person_id") or 0)
        item_email = str(payload.get("email") or "").strip().lower()
        item_phone = whatsapp.normalize_phone(payload.get("phone") or payload.get("telefone") or "")
        matches = (
            (target_deal_ids and item_deal_id in target_deal_ids)
            or (target_person_id > 0 and item_person_id == target_person_id)
            or (target_email and item_email == target_email)
            or (target_phone and item_phone == target_phone)
        )
        if matches:
            removed_queue += 1
            continue
        filtered_queue.append(payload)
    if removed_queue:
        save_email_queue(filtered_queue)

    if removed_pending or removed_queue:
        print(
            f"[EMAIL_RUNTIME_LIMPO] deals={sorted(target_deal_ids)} person={target_person_id or 0} "
            f"email={target_email or '-'} phone={target_phone or '-'} "
            f"fila={removed_queue} pendencias={removed_pending}"
        )
    return {"queue_removed": removed_queue, "pending_removed": removed_pending}


def stop_whatsapp_warm_cadence_state(deal_ids=None, phone="", email="", reason="inbound_replied", paused_until=""):
    state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "whatsapp_warm_cadence.json")
    deal_ids = {int(item) for item in list(deal_ids or []) if int(item or 0) > 0}
    normalized_phone = whatsapp.normalize_phone(phone)
    target_email = str(email or "").strip().lower()
    try:
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as fh:
                state = json.load(fh) or {}
        else:
            state = {}
        stopped = state.setdefault("stopped", {})
        now = datetime.now().isoformat(timespec="seconds")
        payload = {"reason": str(reason or "inbound_replied"), "stopped_at": now}
        if paused_until:
            payload["paused_until"] = str(paused_until)
        for deal_id in sorted(deal_ids):
            stopped[str(deal_id)] = dict(payload)
        if normalized_phone:
            stopped[f"phone:{normalized_phone}"] = dict(payload)
        if target_email:
            stopped[f"email:{target_email}"] = dict(payload)
        if deal_ids or normalized_phone or target_email:
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            tmp = state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, state_file)
            print(f"[WARM_WA_CADENCE_STOPPED] deals={sorted(deal_ids)} phone={normalized_phone or '-'} email={target_email or '-'}")
        return True
    except Exception as exc:
        print(f"[WARM_WA_CADENCE_STOP_FAIL] erro={exc}")
        return False


def clear_email_runtime_state_for_lead(lead_state, phone=""):
    current_person = dict((lead_state or {}).get("person") or {})
    deal_ids = []
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id > 0:
            deal_ids.append(deal_id)
    emails = person_email_values(current_person)
    target_phone = whatsapp.normalize_phone(phone) or next(iter(person_phone_values(current_person)), "")
    result = clear_email_runtime_state(
        deal_ids=deal_ids,
        person_id=extract_entity_id(current_person.get("id")),
        email=emails[0] if emails else "",
        phone=target_phone,
    )
    stop_whatsapp_warm_cadence_state(deal_ids=deal_ids, phone=target_phone, email=emails[0] if emails else "")
    return result


def pause_whatsapp_warm_cadence_for_lead(lead_state, phone="", days=7):
    current_person = dict((lead_state or {}).get("person") or {})
    deal_ids = []
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id > 0:
            deal_ids.append(deal_id)
    emails = person_email_values(current_person)
    target_phone = whatsapp.normalize_phone(phone) or next(iter(person_phone_values(current_person)), "")
    paused_until = (datetime.now().date() + timedelta(days=max(1, int(days or 7)))).isoformat()
    return stop_whatsapp_warm_cadence_state(
        deal_ids=deal_ids,
        phone=target_phone,
        email=emails[0] if emails else "",
        reason="pause_not_now",
        paused_until=paused_until,
    )


def append_crm_note_for_lead(lead_state, note_text):
    if is_test_whitelist_phone((lead_state or {}).get('phone')):
        print(f"[CRM_BYPASS_TESTE] telefone={(lead_state or {}).get('phone')} acao=append_note")
        return True
    if not str(note_text or "").strip():
        return False
    related_deals = related_deals_from_state(lead_state)
    noted = False
    for deal in list(related_deals or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = crm.add_note(deal_id=deal_id, content=str(note_text).strip())
        except Exception:
            ok = False
        if ok:
            noted = True
    if noted:
        print("[NOTA_CRM]")
    return noted


def add_deal_label_id_for_lead(lead_state, label_id):
    if is_test_whitelist_phone((lead_state or {}).get("phone")):
        print(f"[CRM_BYPASS_TESTE] telefone={(lead_state or {}).get('phone')} acao=add_label_id label={label_id}")
        return True
    tagged = False
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            current_deal = crm.get_deal_details(deal_id) or deal
            current_labels = current_deal.get("label") or []
            if isinstance(current_labels, str):
                merged = [item.strip() for item in current_labels.split(",") if item.strip()]
            elif isinstance(current_labels, list):
                merged = list(current_labels)
            else:
                merged = [current_labels]
            keys = set()
            normalized = []
            for item in merged:
                if isinstance(item, dict):
                    key = str(item.get("id") or item.get("label") or item.get("name") or item.get("value") or "").strip()
                else:
                    key = str(item or "").strip()
                if not key or key in keys:
                    continue
                keys.add(key)
                normalized.append(item)
            if str(label_id) not in keys:
                normalized.append(int(label_id))
            ok = crm.update_deal(deal_id, {"label": normalized})
        except Exception:
            ok = False
        tagged = tagged or bool(ok)
    return tagged


def add_respondido_tag_for_lead(lead_state):
    return add_deal_label_id_for_lead(lead_state, 196)


def add_wa_respondeu_tag_for_lead(lead_state):
    if is_test_whitelist_phone((lead_state or {}).get("phone")):
        print(f"[CRM_BYPASS_TESTE] telefone={(lead_state or {}).get('phone')} acao=add_tag tag=WA_RESPONDEU")
        return True
    tagged = False
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = crm.add_tag("deal", deal_id, "WA_RESPONDEU")
        except Exception:
            ok = False
        tagged = tagged or bool(ok)
    return tagged


def add_conversa_tag_for_lead(lead_state):
    if is_test_whitelist_phone((lead_state or {}).get("phone")):
        print(f"[CRM_BYPASS_TESTE] telefone={(lead_state or {}).get('phone')} acao=add_tag tag=CONVERSANDO")
        return True
    tagged = False
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = crm.add_tag("deal", deal_id, "CONVERSANDO")
        except Exception:
            ok = False
        tagged = tagged or bool(ok)
    return tagged


def _normalize_status_bot_value(status_bot):
    normalized = str(status_bot or "").strip().lower()
    aliases = {
        "sem_interesse": "lead_sem_interesse",
        "nao_interessado": "lead_sem_interesse",
        "lead_sem_interesse": "lead_sem_interesse",
        "numero_errado": "numero_nao_corresponde",
        "wrong_number": "numero_nao_corresponde",
        "unknown_person": "numero_nao_corresponde",
        "numero_nao_corresponde": "numero_nao_corresponde",
        "interesse": "lead_interessado",
        "respondido": "lead_interessado",
        "lead_interessado": "lead_interessado",
        "aguardando_horario": "lead_sem_contato",
        "lead_sem_contato": "lead_sem_contato",
    }
    return aliases.get(normalized, normalized)


def update_crm_status_for_lead(lead_state, status_bot):
    current_person = dict((lead_state or {}).get("person") or {})
    target_phone = whatsapp.normalize_phone(
        (lead_state or {}).get("phone")
        or next(iter(person_phone_values(current_person)), "")
    )
    if is_test_whitelist_phone(target_phone):
        print(f"[CRM_BYPASS_TESTE] telefone={(lead_state or {}).get('phone')} acao=update_status status={status_bot}")
        return True
    current_person_id = extract_entity_id(current_person.get("id"))
    updated = False
    normalized_status = _normalize_status_bot_value(status_bot)
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = crm.update_deal(deal_id, {crm.STATUS_BOT_FIELD: normalized_status})
        except Exception:
            ok = False
        updated = updated or bool(ok)
    return updated


def resolve_lead_state_from_payload(payload):
    data = dict(payload or {})
    phone = whatsapp.normalize_phone(data.get("phone") or data.get("telefone") or "")
    email = str(data.get("email") or "").strip().lower()
    deal_id = int(data.get("deal_id") or 0)
    person_id = int(data.get("person_id") or 0)

    if phone:
        try:
            state = validate_lead_with_cache(phone)
            if isinstance(state, dict):
                return state
        except Exception as exc:
            print(f"[ERRO_RESOLVE_LEAD_PHONE] telefone={phone} erro={exc}")

    deal = {}
    if deal_id > 0:
        try:
            deal = crm.get_deal_details(deal_id) or {}
        except Exception as exc:
            print(f"[ERRO_RESOLVE_DEAL] deal={deal_id} erro={exc}")
        if not person_id:
            person_id = extract_entity_id((deal or {}).get("person_id"))

    person = {}
    if person_id > 0:
        try:
            person = crm.get_person_details(person_id) or {}
        except Exception as exc:
            print(f"[ERRO_RESOLVE_PERSON] person={person_id} erro={exc}")

    if not person and email:
        try:
            person = crm.find_person(email=email) or {}
        except Exception as exc:
            print(f"[ERRO_RESOLVE_PERSON_EMAIL] email={email} erro={exc}")

    if not person and phone and phone in TEST_WHITELIST:
        person = {"id": -1, "name": "Teste", "phone": [{"value": phone}], "email": [{"value": email}] if email else []}

    deals = []
    if deal:
        deals = [deal]
    else:
        resolved_person_id = extract_entity_id((person or {}).get("id"))
        resolved_org_id = extract_entity_id((person or {}).get("org_id"))
        if resolved_person_id > 0 or resolved_org_id > 0:
            try:
                deals = crm.find_open_deals_by_person_or_org(person_id=resolved_person_id, org_id=resolved_org_id)
            except Exception as exc:
                print(f"[ERRO_RESOLVE_DEALS] person={resolved_person_id} org={resolved_org_id} erro={exc}")

    return {
        "valid": bool(person or deals or phone in TEST_WHITELIST or email or deal_id or person_id),
        "person": person or {"id": person_id or -1, "name": data.get("name") or data.get("nome") or "Lead"},
        "deals": deals or ([deal] if deal else []),
        "bypass": bool(phone in TEST_WHITELIST),
        "source": "email_inbound",
    }


def build_lead_payload_from_state(phone, lead_state):
    current_person = dict((lead_state or {}).get("person") or {})
    deals = list((lead_state or {}).get("deals") or [])
    primary_deal = dict(deals[0] or {}) if deals else {}
    emails = person_email_values(current_person)
    return {
        "name": str(current_person.get("name") or "").strip(),
        "nome": str(current_person.get("name") or "").strip(),
        "empresa": str(
            primary_deal.get("org_name")
            or primary_deal.get("title")
            or (current_person.get("org_id") or {}).get("name")
            or ""
        ).strip(),
        "email": emails[0] if emails else "",
        "phone": whatsapp.normalize_phone(phone),
        "telefone": whatsapp.normalize_phone(phone),
    }


def serialize_conversation_history(items):
    serialized = []
    for item in list(items or []):
        if isinstance(item, dict):
            serialized.append(
                {
                    "direction": str(item.get("direction") or "").strip().lower(),
                    "message": str(item.get("message") or "").strip(),
                    "step": int(item.get("step") or 0),
                    "created_at": str(item.get("created_at") or "").strip(),
                    "source": str(item.get("source") or "").strip().lower(),
                }
            )
            continue
        message = str(item or "").strip()
        if message:
            serialized.append({"direction": "", "message": message, "step": 0, "created_at": "", "source": ""})
    return serialized


def current_tags_from_lead_state(lead_state):
    tags = set()
    options = crm.get_deal_labels()
    current_person = dict((lead_state or {}).get("person") or {})
    if extract_entity_id(current_person.get("id")) > 0 or extract_entity_id(current_person.get("org_id")) > 0:
        deals = related_deals_from_state(lead_state)
    else:
        deals = list((lead_state or {}).get("deals") or [])
    for deal in list(deals or []):
        try:
            tags.update(crm.resolve_label_tokens((deal or {}).get("label"), options))
        except Exception:
            continue
    return sorted(tag for tag in tags if str(tag or "").strip())


def status_bot_from_lead_state(lead_state):
    current_person = dict((lead_state or {}).get("person") or {})
    deals = list((lead_state or {}).get("deals") or [])
    for deal in list(deals or []):
        value = str((deal or {}).get("status_bot") or (deal or {}).get(crm.STATUS_BOT_FIELD) or "").strip()
        if value:
            return value
    return str(current_person.get("status_bot") or current_person.get(crm.STATUS_BOT_FIELD) or "").strip()


def latest_inbound_message(history):
    for item in reversed(list(history or [])):
        if str((item or {}).get("direction") or "").strip().lower() != "in":
            continue
        message = str((item or {}).get("message") or "").strip()
        if message:
            return message
    return ""


def build_agent_decide_payload(
    *,
    channel,
    lead_state,
    message_text,
    history,
    cadence_step,
    source,
    phone="",
    email="",
):
    current_person = dict((lead_state or {}).get("person") or {})
    deals = list((lead_state or {}).get("deals") or [])
    primary_deal = dict(deals[0] or {}) if deals else {}
    fallback_phone = whatsapp.normalize_phone(phone)
    if not fallback_phone:
        fallback_phone = next(iter(person_phone_values(current_person)), "")
    payload = build_lead_payload_from_state(fallback_phone, lead_state)
    return {
        "channel": str(channel or "whatsapp").strip().lower() or "whatsapp",
        "phone": whatsapp.normalize_phone(phone or payload.get("phone") or ""),
        "email": str(email or payload.get("email") or "").strip().lower(),
        "deal_id": extract_entity_id(primary_deal.get("id")),
        "person_id": extract_entity_id(current_person.get("id")),
        "org_id": extract_entity_id(current_person.get("org_id")),
        "message_text": str(message_text or "").strip(),
        "conversation_history": serialize_conversation_history(history),
        "current_tags": current_tags_from_lead_state(lead_state),
        "status_bot": status_bot_from_lead_state(lead_state),
        "cadence_step": max(0, int(cadence_step or 0)),
        "source": str(source or "").strip().lower(),
        "name": str(payload.get("name") or "").strip(),
        "company": str(payload.get("empresa") or "").strip(),
    }


def registrar_aguardando_horario(phone, message, msg_id="", timestamp="", source="", lead_state=None):
    whatsapp.upsert_after_hours_pending(
        phone,
        message=message,
        msg_id=msg_id,
        timestamp=timestamp,
        source=source,
    )
    if lead_state:
        try:
            update_crm_status_for_lead(lead_state, FORA_HORARIO_STATUS)
        except Exception:
            pass


def handle_fora_do_horario(phone, message, msg_id="", timestamp="", source="", lead_state=None):
    append_history(phone, "in", message, step=0)
    registrar_aguardando_horario(
        phone,
        message,
        msg_id=msg_id,
        timestamp=timestamp,
        source=source,
        lead_state=lead_state,
    )
    if whatsapp.was_after_hours_notice_sent_today(phone):
        print(f"[FORA_HORARIO_BLOQUEADO] telefone={phone} status={FORA_HORARIO_STATUS}")
        return {"ok": True, "confirmed": True, "after_hours": True, "notice_sent": False}
    if has_recent_history_entry(phone, "out", FORA_HORARIO_MENSAGEM, within_seconds=18 * 3600):
        whatsapp.mark_after_hours_notice_sent(phone)
        print(f"[FORA_HORARIO_BLOQUEADO] telefone={phone} status={FORA_HORARIO_STATUS}")
        return {"ok": True, "confirmed": True, "after_hours": True, "notice_sent": False}
    if not can_emit_reply(phone, FORA_HORARIO_MENSAGEM, within_seconds=REPLY_WINDOW_SECONDS):
        print(f"[FORA_HORARIO_BLOQUEADO] telefone={phone} status={FORA_HORARIO_STATUS}")
        return {"ok": True, "confirmed": True, "after_hours": True, "notice_sent": False}
    ok = blocked_whatsapp_send(phone, FORA_HORARIO_MENSAGEM)
    if ok:
        append_history(phone, "out", FORA_HORARIO_MENSAGEM, step=0)
        whatsapp.mark_after_hours_notice_sent(phone)
        print(f"[FORA_HORARIO_RESPOSTA] telefone={phone} status={FORA_HORARIO_STATUS}")
        return {"ok": True, "confirmed": True, "after_hours": True, "notice_sent": True}
    release_reply_guard(phone)
    print(f"[FORA_HORARIO_BLOQUEADO] telefone={phone} status={FORA_HORARIO_STATUS} motivo=falha_envio")
    return {"ok": False, "confirmed": False, "after_hours": True, "notice_sent": False}


def handle_closing_intent(phone, message, closing_key, lead_state=None):
    closing_message = pitch.get_closing_message(closing_key)
    if not closing_message:
        return False
    block_reason = {
        "no_interest": "sem_interesse",
        "wrong_number": "numero_errado",
        "unknown_person": "numero_errado",
    }.get(str(closing_key or "").strip(), "encerrado")
    append_to_blocklist(phone, block_reason)
    whatsapp.clear_after_hours_pending(phone)
    clear_email_runtime_state_for_lead(lead_state, phone=phone)
    if closing_key == "no_interest":
        print(f"[INTENT_NEGATIVO] telefone={phone}")
        update_crm_status_for_lead(lead_state, "lead_sem_interesse")
        add_respondido_tag_for_lead(lead_state)
        for deal in list(related_deals_from_state(lead_state) or []):
            crm.add_tag("deal", extract_entity_id(deal.get("id")), "SEM_INTERESSE")
        move_related_deals_to_terminal_stage(lead_state, reason="sem_interesse")
        append_crm_note_for_lead(lead_state, f"Lead informou nao ter interesse. Texto: {str(message or '').strip()}")
    elif closing_key in {"wrong_number", "unknown_person"}:
        print(f"[INTENT_ERRADO] telefone={phone}")
        update_crm_status_for_lead(lead_state, "numero_nao_corresponde")
        add_respondido_tag_for_lead(lead_state)
        for deal in list(related_deals_from_state(lead_state) or []):
            crm.add_tag("deal", extract_entity_id(deal.get("id")), "NUMERO_ERRADO")
        move_related_deals_to_terminal_stage(lead_state, reason="numero_errado")
        append_crm_note_for_lead(lead_state, f"Contato incorreto. Texto recebido: {str(message or '').strip()}")
    update_whatsapp_conversation_state_for_lead(
        lead_state,
        phone,
        message,
        intent="wrong_contact" if closing_key in {"wrong_number", "unknown_person"} else "opt_out",
    )
    append_history(phone, "in", message, step=0)
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={closing_message}")
    print(f"[CONVERSATION_STOPPED_NO_REPLY] telefone={phone} motivo={closing_key}")
    return True


def is_system_jid(raw_phone):
    jid = str(raw_phone or "").strip().lower()
    return (
        not jid
        or "@g.us" in jid
        or "status@broadcast" in jid
        or jid.endswith("@broadcast")
        or jid.endswith("@newsletter")
    )


def processed_key(phone, msg_id, message):
    if msg_id:
        return f"{phone}:{msg_id}"
    # Se nao tem msg_id, gera um unico baseado em tempo para permitir mensagens iguais ("ok", "sim")
    unique_suffix = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    return f"{phone}:auto_{unique_suffix}"


def extract_entity_id(field):
    if isinstance(field, dict):
        for key in ("value", "id"):
            try:
                value = int(field.get(key) or 0)
            except Exception:
                value = 0
            if value > 0:
                return value
        return 0
    try:
        return int(field or 0)
    except Exception:
        return 0


def detect_referral_intent(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    if not normalized:
        return False

    direct_patterns = (
        "vou te passar o contato",
        "vou passar o contato",
        "posso te passar o contato",
        "te passo o contato",
        "passo o contato",
        "passar com o marketing",
        "falar com o marketing",
        "fala com o marketing",
        "falar com o comercial",
        "fala com o comercial",
        "melhor contato eh",
        "melhor contato é",
        "segue contato",
        "segue o contato",
        "segue contato do",
        "segue o contato do",
        "segue email",
        "segue o email",
        "segue e mail",
        "email do gerente",
        "email da gerente",
        "email do responsavel",
        "email do responsável",
        "fale com",
        "responsavel e",
        "responsavel eh",
        "responsável é",
        "fala com fulano",
        "chama esse número",
        "chama esse numero",
        "o responsável é",
        "o responsavel e",
        "quem cuida disso é",
        "quem cuida disso e",
        "procura o joão",
        "procura o joao",
        "contato é",
        "contato e",
        "telefone dele é",
        "telefone dele e",
        "email dele é",
        "email dele e",
        "pode falar com",
    )
    if any(token in normalized for token in direct_patterns):
        return True
    if re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", str(message or ""), flags=re.IGNORECASE):
        if any(token in normalized for token in ("gerente", "compras", "comercial", "marketing", "responsavel", "responsável", "setor", "departamento", "segue", "contato")):
            return True
        return True

    has_contact = "contato" in normalized
    has_target_area = any(token in normalized for token in ("marketing", "comercial", "responsavel", "responsável"))
    return has_contact and has_target_area


def maybe_handle_inbound_agent_decision(*, phone, message, source, lead_state, lead, history, current_step, key):
    if not is_agent_decide_enabled():
        return None

    try:
        inbound_messages = [
            item.get("message", "")
            for item in list(history or [])
            if str(item.get("direction") or "").strip().lower() == "in"
        ]
        decision = resolve_agent_decision(
            build_agent_decide_payload(
                channel="whatsapp",
                lead_state=lead_state,
                message_text=message,
                history=history,
                cadence_step=current_step,
                source=source,
                phone=phone,
            ),
            caller="inbound",
        )
    except Exception as exc:
        print(f"[AGENT_DECIDE_ERRO] Fallback local devido a erro: {exc}")
        return None

    intent = str(decision.get("intent") or "").strip().lower()
    action = str(decision.get("action") or "").strip().lower()
    print(
        f"[AGENT_DECIDE_ACTION] caller=inbound phone={phone} deal={int(decision.get('deal_id') or 0)} "
        f"intent={intent or '-'} action={action or '-'} send={1 if decision.get('should_send') else 0} "
        f"blocklist={1 if decision.get('should_blocklist') else 0} pause={1 if decision.get('should_pause_automation') else 0}"
    )

    if intent == "nao_interessado":
        handle_closing_intent(phone, message, "no_interest", lead_state)
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, **decision}
    if intent == "numero_errado":
        handle_closing_intent(phone, message, "wrong_number", lead_state)
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, **decision}
    if intent == "indicacao":
        handle_referral_redirect(phone, message, lead_state)
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, **decision}
    if intent == "pause_not_now":
        add_respondido_tag_for_lead(lead_state)
        add_wa_respondeu_tag_for_lead(lead_state)
        update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="pause_not_now")
        clear_email_runtime_state_for_lead(lead_state, phone=phone)
        pause_whatsapp_warm_cadence_for_lead(lead_state, phone=phone)
        append_crm_note_for_lead(lead_state, f"Lead pediu pausa/contato futuro. Sem resposta automatica insistente. Texto: {message}")
        if decision.get("should_create_activity"):
            create_soft_negative_followup_for_lead(lead_state, f"Follow-up leve apos pausa solicitada pelo lead. Texto: {message}")
        whatsapp.clear_after_hours_pending(phone)
        print(f"[PAUSE_NOT_NOW] telefone={phone} action=pause_automation send=0 texto={message}")
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, **decision}

    if intent in {"interesse", "duvida", "neutro", "agendamento"}:
        add_respondido_tag_for_lead(lead_state)
        add_wa_respondeu_tag_for_lead(lead_state)
        add_conversa_tag_for_lead(lead_state)
        update_whatsapp_conversation_state_for_lead(
            lead_state,
            phone,
            message,
            intent="interested" if intent in {"interesse", "agendamento"} else classify_conversation_intent(message),
        )
    if decision.get("should_pause_automation"):
        clear_email_runtime_state_for_lead(lead_state, phone=phone)
    if intent == "agendamento" or action == "schedule":
        if decision.get("should_move_stage"):
            move_related_deals_to_scheduled(lead_state)
        if decision.get("should_create_activity"):
            create_scheduling_activity_for_lead(lead_state, f"Inbound WhatsApp com pedido de agendamento: {message}")
        append_crm_note_for_lead(lead_state, f"Inbound WhatsApp com pedido de agendamento. Texto: {message}")

    if not decision.get("should_send"):
        whatsapp.clear_after_hours_pending(phone)
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, **decision}

    if not is_auto_reply_router_enabled():
        clear_email_runtime_state_for_lead(lead_state, phone=phone)
        append_crm_note_for_lead(lead_state, f"Inbound WhatsApp recebido. Auto-resposta bloqueada ate existir roteador por origem/tag. Texto: {message}")
        whatsapp.clear_after_hours_pending(phone)
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, "auto_reply_blocked": True, **decision}

    reply = str(decision.get("reply") or "").strip() or pitch.build_reply(lead, inbound_messages, current_step=current_step, allow_opening=False)
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")
    if False and has_recent_history_entry(phone, "out", reply, within_seconds=1800):
        print(f"[DUPLICADO_HISTORY_OUTBOUND] {phone} ignorado")
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, "duplicated": True, **decision}
    if not can_emit_reply(phone, reply, within_seconds=REPLY_WINDOW_SECONDS):
        print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
        commit_processed_key(key)
        return {"ok": True, "confirmed": True, **decision}

    if not whatsapp.healthcheck():
        print("[AVISO] API pode estar offline, mas tentando enviar mesmo assim...")
    ok = blocked_whatsapp_send(phone, reply)
    if not ok:
        release_reply_guard(phone)
        print(f"[FALHA_ENVIO] {phone}")
        return {"ok": False, "confirmed": False, **decision}

    append_history(phone, "out", reply, step=current_step)
    update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent=classify_conversation_intent(message), last_bot_reply=reply)
    whatsapp.clear_after_hours_pending(phone)
    print(f"[RESPOSTA_ENVIADA] {phone}")
    commit_processed_key(key)
    return {"ok": True, "confirmed": True, **decision, "reply": reply}


def extract_phone_candidates(message, current_phone=""):
    current = whatsapp.normalize_phone(current_phone)
    candidates = []
    seen = set()
    for raw in re.findall(r"(?:\+?\d[\d\-\s\(\)]{7,}\d)", str(message or "")):
        normalized = whatsapp.normalize_phone(raw)
        if not whatsapp.is_valid_phone(normalized):
            continue
        if normalized == current:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


def extract_email_candidates(message):
    candidates = []
    seen = set()
    for raw in re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", str(message or ""), flags=re.IGNORECASE):
        normalized = str(raw or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


def extract_referral_name(message):
    text = str(message or "").strip()
    patterns = (
        r"\b(?:com|falar com|contato com|whatsapp)\s+(?:a\s+|o\s+)?([A-ZÁÉÍÓÚÂÊÔÃÕÇ][A-Za-zÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç]{2,})\b",
        r"\b([A-ZÁÉÍÓÚÂÊÔÃÕÇ][A-Za-zÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç]{2,})\s+pelo\s+WhatsApp\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = str(match.group(1) or "").strip()
        if candidate.lower() in {"whatsapp", "contato", "telefone", "setor"}:
            continue
        return candidate
    return ""


def person_phone_values(person):
    values = []
    seen = set()
    for item in list((person or {}).get("phone") or []):
        raw = item.get("value") if isinstance(item, dict) else item
        normalized = whatsapp.normalize_phone(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return values


def person_email_values(person):
    values = []
    seen = set()
    for item in list((person or {}).get("email") or []):
        raw = item.get("value") if isinstance(item, dict) else item
        normalized = str(raw or "").strip().lower()
        if not normalized or "@" not in normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return values


def merge_person_phone_payload(existing, candidates):
    merged = []
    seen = set()
    for phone in list(existing or []) + list(candidates or []):
        normalized = whatsapp.normalize_phone(phone)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append({"label": "work", "value": normalized, "primary": len(merged) == 0})
    return merged


def merge_person_email_payload(existing, candidates):
    merged = []
    seen = set()
    for email in list(existing or []) + list(candidates or []):
        normalized = str(email or "").strip().lower()
        if not normalized or "@" not in normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append({"label": "work", "value": normalized, "primary": len(merged) == 0})
    return merged


def find_or_create_referral_person(target_phones, target_emails, current_person, referral_name=""):
    org_id = extract_entity_id((current_person or {}).get("org_id"))
    target_phone = str((target_phones or [""])[0] or "").strip()
    target_email = str((target_emails or [""])[0] or "").strip().lower()

    referral_person = {}
    for candidate_phone in list(target_phones or []):
        referral_person = crm.find_person(org_id=org_id, phone=candidate_phone)
        if referral_person and referral_person.get("id"):
            break
    if not referral_person:
        for candidate_email in list(target_emails or []):
            referral_person = crm.find_person(org_id=org_id, email=candidate_email)
            if referral_person and referral_person.get("id"):
                break

    current_person_id = extract_entity_id((current_person or {}).get("id"))
    if not referral_person and not str(referral_name or "").strip() and current_person_id > 0:
        referral_person = dict(current_person or {})

    if referral_person and referral_person.get("id"):
        person_id = extract_entity_id(referral_person.get("id"))
        existing_phones = person_phone_values(referral_person)
        existing_emails = person_email_values(referral_person)
        merged_phones = merge_person_phone_payload(existing_phones, target_phones)
        merged_emails = merge_person_email_payload(existing_emails, target_emails)
        update_payload = {}
        if [item.get("value") for item in merged_phones] != existing_phones:
            update_payload["phone"] = merged_phones
        if [item.get("value") for item in merged_emails] != existing_emails:
            update_payload["email"] = merged_emails
        if update_payload:
            None if is_test_whitelist_phone(phone) else crm.update_person(person_id, update_payload)
            referral_person = crm.get_person_details(person_id) or referral_person
            print(
                f"[REFERRAL_CRM_ATUALIZADO] person={person_id} "
                f"phones={len(merged_phones)} emails={len(merged_emails)}"
            )
        return referral_person

    org_name = ""
    org_payload = (current_person or {}).get("org_id")
    if isinstance(org_payload, dict):
        org_name = str(org_payload.get("name") or "").strip()
    clean_name = str(referral_name or "").strip()
    if clean_name and org_name:
        person_name = f"{clean_name} - {org_name}"
    elif clean_name:
        person_name = clean_name
    else:
        person_name = f"Contato indicado - {org_name}".strip(" -") if org_name else "Contato indicado"
    payload = {"name": person_name}
    if target_phones:
        payload["phone"] = [f"55{phone}" for phone in list(target_phones or []) if str(phone or "").strip()]
    if target_emails:
        payload["email"] = [email for email in list(target_emails or []) if str(email or "").strip()]
    if org_id > 0:
        payload["org_id"] = org_id
    created = crm.create_person(payload)
    created_id = extract_entity_id((created or {}).get("id"))
    if created_id > 0:
        print(
            f"[REFERRAL_CRM_CRIADO] person={created_id} "
            f"phones={len(list(target_phones or []))} emails={len(list(target_emails or []))}"
        )
    return created


def handle_referral_redirect(phone, message, lead_state):
    referral_numbers = extract_phone_candidates(message, current_phone=phone)
    referral_emails = extract_email_candidates(message)
    if not referral_numbers and not referral_emails:
        return False

    target_phone = referral_numbers[0] if referral_numbers else ""
    target_email = referral_emails[0] if referral_emails else ""
    referral_name = extract_referral_name(message)
    history = append_history(phone, "in", message, step=0)
    print(f"[INTENT_INDICACAO] telefone={phone}")
    
    # Bloquear o número original (Requirement 8)
    append_to_blocklist(phone, "indicacao")
    whatsapp.clear_after_hours_pending(phone)
    clear_email_runtime_state_for_lead(lead_state, phone=phone)
    update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="referred")

    reply = "Perfeito, obrigado pela indicação. Vou seguir por esse contato e não te incomodo mais por aqui."
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")
    print(f"[CONVERSATION_STOPPED_NO_REPLY] telefone={phone} motivo=indicacao")
    current_person = dict((lead_state or {}).get("person") or {})
    current_person_id = extract_entity_id(current_person.get("id"))
    current_org_id = extract_entity_id(current_person.get("org_id"))
    related_deals = []
    if current_person_id > 0 or current_org_id > 0:
        related_deals = crm.find_open_deals_by_person_or_org(person_id=current_person_id, org_id=current_org_id)

    referral_person = find_or_create_referral_person(
        referral_numbers,
        referral_emails,
        current_person,
        referral_name=referral_name,
    )
    referral_person_id = extract_entity_id((referral_person or {}).get("id"))
    if referral_person_id <= 0:
        print(f"[REFERRAL_SEM_PESSOA] origem={phone} destino={target_phone or target_email}")
        return False
    print(f"[NOVO_CONTATO_SALVO] person={referral_person_id}")

    contact_summary_parts = []
    if referral_numbers:
        contact_summary_parts.append("telefones: " + ", ".join(referral_numbers))
    if referral_emails:
        contact_summary_parts.append("emails: " + ", ".join(referral_emails))
    contact_summary = " | ".join(contact_summary_parts) if contact_summary_parts else (target_phone or target_email or "nao informado")

    if not related_deals:
        append_to_blocklist(f"55{phone}", "referral_redirect")
        print(f"[BLOCKLIST_NUMERO] telefone={phone}")
        print(f"[INTENT_REDIRECIONADO] telefone={phone}")
        print(f"[CONTATO_REDIRECIONADO_SEM_DEAL] origem={phone} destino={target_phone or target_email} person={referral_person_id}")
        update_crm_status_for_lead(lead_state, "encerrado")
        if current_person_id > 0:
            try:
                None if is_test_whitelist_phone(phone) else crm.create_note(
                    person_id=current_person_id,
                    content=f"Contato redirecionado. Novos contatos salvos no CRM: {contact_summary}.",
                )
            except Exception:
                pass
        return True

    target_deal = dict(related_deals[0] or {})
    deal_id = extract_entity_id(target_deal.get("id"))
    if deal_id <= 0:
        print(f"[REFERRAL_DEAL_INVALIDO] origem={phone} destino={target_phone or target_email}")
        return False

    participants = crm.get_deal_participants(deal_id, limit=100)
    already_participant = False
    for participant in participants:
        participant_person_id = extract_entity_id(participant.get("person_id"))
        if participant_person_id == referral_person_id:
            already_participant = True
            break

    if not already_participant and not crm.add_deal_participant(deal_id, referral_person_id):
        print(f"[REFERRAL_FALHA_PARTICIPANTE] deal={deal_id} destino={target_phone or target_email}")
        return False

    # ATUALIZA O DEAL: RESET CADENCIA PARA O NOVO PARTICIPANTE
    try:
        # Busca labels atuais para remover as de cadencia
        deal_details = crm.get_deal_details(deal_id) or {}
        current_labels_raw = deal_details.get("label") or ""
        
        # Converte para lista de IDs
        current_label_ids = []
        if isinstance(current_labels_raw, list):
            current_label_ids = [str(l) for l in current_labels_raw]
        elif isinstance(current_labels_raw, str):
            current_label_ids = [l.strip() for l in current_labels_raw.split(",") if l.strip()]
        
        # Filtra labels: remove RESPONDIDO e CADencias
        all_options = crm.get_deal_labels()
        id_to_name = {str(opt.get("id")): str(opt.get("label") or opt.get("name") or "").upper() for opt in all_options}
        name_to_id = {str(opt.get("label") or opt.get("name") or "").upper(): str(opt.get("id")) for opt in all_options}
        
        new_label_ids = []
        for lid in current_label_ids:
            lname = id_to_name.get(lid, "")
            if "CAD" in lname or "RESPONDIDO" == lname or "CONVERSANDO" == lname:
                continue
            new_label_ids.append(lid)
        
        # Adiciona tag de origem de indicacao
        crm.add_tag("deal", deal_id, "CONTATO_INDICADO")

        update_payload = {
            "label": ",".join(new_label_ids)
        }
        crm.update_deal(deal_id, update_payload)
        print(f"[REFERRAL_DEAL_UPDATED] deal={deal_id} new_labels={len(new_label_ids)}")
    except Exception as e:
        print(f"[REFERRAL_DEAL_UPDATE_ERRO] {e}")

    if target_phone:
        set_cached_lead(
            target_phone,
            valid=True,
            person=referral_person,
            deals=[target_deal],
            bypass=False,
            source="referral",
        )
    
    # Blocklist do contato original
    append_to_blocklist(phone, "referral_redirect")
    print(f"[BLOCKLIST_NUMERO] telefone={phone}")
    print(f"[INTENT_REDIRECIONADO] telefone={phone}")
    print(f"[CONTATO_REDIRECIONADO] origem={phone} destino={target_phone or target_email} deal={deal_id}")
    # update_crm_status_for_lead(lead_state, "encerrado")
    append_crm_note_for_lead(
        lead_state,
        f"Encaminhado para outro responsavel. Novos contatos salvos no CRM: {contact_summary}.",
    )

    return True


def get_cached_lead(phone):
    entry = lead_cache.get(phone)
    if not isinstance(entry, dict):
        return None
    checked_at = float(entry.get("checked_at") or 0)
    if time.time() - checked_at > CRM_CACHE_TTL_SECONDS:
        lead_cache.pop(phone, None)
        return None
    print(f"[CACHE_HIT] {phone}")
    return entry


def set_cached_lead(phone, *, valid, person=None, deals=None, bypass=False, source="crm"):
    lead_cache[phone] = {
        "valid": bool(valid),
        "person": person or {},
        "deals": deals or [],
        "bypass": bool(bypass),
        "source": str(source or "crm"),
        "checked_at": time.time(),
    }


def validate_lead_with_cache(phone):
    cached = get_cached_lead(phone)
    if cached is not None:
        return cached

    print(f"[CACHE_MISS] {phone}")
    print(f"[CRM_RATE_LIMIT] {phone} consulta_unica_por_24h")
    try:
        person = crm.find_person(phone=phone)
        if person and person.get("id"):
            payload = {
                "valid": True,
                "person": person,
                "deals": [],
                "bypass": False,
                "source": "crm",
            }
            set_cached_lead(phone, **payload)
            return payload
    except Exception as e:
        if "429" in str(e):
            print("[CRM_429_BYPASS]")
            payload = {
                "valid": True,
                "person": {"id": -1, "name": "Lead"},
                "deals": [],
                "bypass": True,
                "source": "bypass_429",
            }
            set_cached_lead(phone, **payload)
            return payload
        raise

    print("[LEAD_NAO_ENCONTRADO_CONTINUANDO]")
    payload = {
        "valid": bool(phone in TEST_WHITELIST),
        "person": {"id": -1, "name": "Teste"} if phone in TEST_WHITELIST else {"id": -1, "name": "Lead"},
        "deals": [],
        "bypass": True,
        "source": "whitelist" if phone in TEST_WHITELIST else "no_crm",
    }
    set_cached_lead(phone, **payload)
    return payload



def _digits_only(v):
    return "".join(ch for ch in str(v or "") if ch.isdigit())

def _phone_matches(a,b):
    a=_digits_only(a)
    b=_digits_only(b)
    if not a or not b:
        return False
    return a==b or a.endswith(b[-10:]) or b.endswith(a[-10:])

def is_phone_allowed_for_auto_reply(phone):
    import json
    from pathlib import Path
    phone=_digits_only(phone)
    paths=[
        Path("data/whatsapp_warm_cadence.json"),
        Path("data/whatsapp_conversation_state.json"),
    ]
    for path in paths:
        if not path.exists():
            continue
        try:
            data=json.load(open(path,encoding="utf-8"))
        except Exception as e:
            print("[AUTO_REPLY_ALLOWLIST_READ_FAIL]", path, e)
            continue

        if isinstance(data,dict):
            items=data.items()
        elif isinstance(data,list):
            items=enumerate(data)
        else:
            continue

        for k,v in items:
            if _phone_matches(phone,k):
                return True
            if isinstance(v,dict):
                if _phone_matches(phone,v.get("phone")):
                    return True
                if _phone_matches(phone,v.get("telefone")):
                    return True
    return False



def is_brazil_business_hours():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:
        import holidays
        br_holidays = holidays.Brazil()
    except Exception:
        br_holidays = set()

    now = datetime.now(ZoneInfo("America/Sao_Paulo"))
    if now.weekday() >= 5:
        return False
    if now.date() in br_holidays:
        return False
    return 9 <= now.hour < 18


@app.route("/", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/agent/decide", methods=["POST"])
def agent_decide():
    try:
        data = request.json or {}
        if not data:
            return jsonify({"ok": False, "error": "empty_payload"}), 400
        decision = build_agent_decision(data)
        return jsonify({"ok": True, **decision})
    except Exception as e:
        print("[ERRO_AGENT_DECIDE]", str(e))
        return jsonify({"ok": False, "error": "agent_decide_failed"}), 500


@app.route("/agent/decide/botpress", methods=["POST"])
def agent_decide_botpress():
    try:
        data = request.json or {}
        if not data:
            return jsonify({"ok": False, "error": "empty_payload"}), 400
        decision = build_botpress_agent_decision(data)
        return jsonify({"ok": True, **decision})
    except Exception as e:
        print("[ERRO_AGENT_DECIDE_BOTPRESS]", str(e))
        return jsonify({"ok": False, "error": "agent_decide_botpress_failed"}), 500


def _maybe_mark_hybrid_cadence_exhausted(deal_id, cadence_step):
    target_deal_id = int(deal_id or 0)
    target_step = int(cadence_step or 0)
    if target_deal_id <= 0 or target_step < 6:
        return False
    try:
        deal = crm.get_deal_details(target_deal_id) or {}
    except Exception as exc:
        print(f"[HYBRID_EXAUSTAO_FALHA] deal={target_deal_id} erro={exc}")
        return False

    try:
        tokens = crm.resolve_label_tokens(deal.get("label"), crm.get_deal_labels())
    except Exception:
        tokens = set()
    if "RESPONDIDO" in tokens:
        return False

    history_key = f"email:deal={target_deal_id}"
    history_items = load_history().get(history_key, [])
    if any(str(item.get("direction") or "").strip().lower() == "in" for item in list(history_items or []) if isinstance(item, dict)):
        return False

    stage_id = resolve_terminal_stage_id()
    stage_ok = bool(crm.update_stage(deal_id=target_deal_id, stage_id=stage_id)) if stage_id > 0 else False
    status_ok = bool(crm.update_deal(target_deal_id, {"status_bot": "tentativa_esgotada"}))
    note_ok = bool(crm.add_note(deal_id=target_deal_id, content="Tentativa de contato esgotada apos 6 cadencias hibridas."))
    print(
        f"[HYBRID_EXAUSTAO] deal={target_deal_id} cadence={target_step} "
        f"stage_ok={1 if stage_ok else 0} status_ok={1 if status_ok else 0} note_ok={1 if note_ok else 0}"
    )
    return bool(stage_ok or status_ok or note_ok)


@app.route("/email/callback", methods=["POST"])
@app.route("/email/confirm", methods=["POST"])
def email_callback():
    try:
        data = request.json or {}
        event_id = str(data.get("event_id") or data.get("id") or "").strip()
        callback_status = normalize_intent_text(
            data.get("status")
            or data.get("delivery_status")
            or data.get("result")
            or data.get("state")
            or ""
        )
        pending = load_email_pending_confirmations()
        item = dict(pending.get(event_id) or {})
        if not item and event_id:
            history_key = f"email:deal={int(data.get('deal_id') or 0)}"
            history_message = str(data.get("cadence_tag") or data.get("message") or "").strip() or f"EMAIL_CAD{int(data.get('cadence_step') or 0)}"
            if has_recent_history_entry(history_key, "out", history_message, within_seconds=7 * 24 * 3600):
                print(f"[EMAIL_CALLBACK_DUPLICADO] event_id={event_id}")
                return jsonify({"ok": True, "duplicated": True})
            print(f"[EMAIL_CALLBACK_DESCONHECIDO] event_id={event_id or '-'} status={callback_status or '-'}")
            return jsonify({"ok": True, "ignored": True})

        deal_id = int(item.get("deal_id") or data.get("deal_id") or 0)
        person_id = int(item.get("person_id") or data.get("person_id") or 0)
        cadence_step = max(1, int(item.get("cadence_step") or data.get("cadence_step") or 1))
        cadence_tag = str(item.get("cadence_tag") or data.get("cadence_tag") or f"EMAIL_CAD{cadence_step}").strip() or f"EMAIL_CAD{cadence_step}"
        email = str(item.get("email") or data.get("email") or "").strip().lower()
        history_key = f"email:deal={deal_id}"
        history_message = f"EMAIL_CAD{cadence_step}"
        success_values = {"sent", "confirmed", "delivered", "success", "ok", "email_sent"}

        if callback_status not in success_values:
            pending.pop(event_id, None)
            save_email_pending_confirmations(pending)
            print(
                f"[EMAIL_CALLBACK_SEM_CONFIRMACAO] event_id={event_id or '-'} deal={deal_id} "
                f"email={email or '-'} status={callback_status or '-'}"
            )
            return jsonify({"ok": True, "confirmed": False})

        if not has_recent_history_entry(history_key, "out", history_message, within_seconds=30 * 24 * 3600):
            append_history(history_key, "out", history_message, step=cadence_step, source="manual_sync")

        tag_ok = bool(crm.add_tag("deal", deal_id, cadence_tag)) if deal_id > 0 else False
        status_ok = bool(crm.update_deal(deal_id, {"status_bot": "contato_iniciado"})) if deal_id > 0 else False
        note_ok = bool(
            crm.add_note(
                deal_id=deal_id,
                content=f"E-mail enviado na cadencia {cadence_step}. Confirmado por callback nativo do SDR."
            )
        ) if deal_id > 0 else False
        pending.pop(event_id, None)
        save_email_pending_confirmations(pending)
        _maybe_mark_hybrid_cadence_exhausted(deal_id, cadence_step)
        print(
            f"[EMAIL_CONFIRMADO] event_id={event_id or '-'} deal={deal_id} email={email or '-'} "
            f"cadencia={cadence_step} tag_ok={1 if tag_ok else 0} status_ok={1 if status_ok else 0} note_ok={1 if note_ok else 0}"
        )
        return jsonify({"ok": True, "confirmed": True})
    except Exception as e:
        print("[ERRO_EMAIL_CALLBACK]", str(e))
        return jsonify({"ok": False, "error": "email_callback_failed"}), 500


@app.route("/email/inbound", methods=["POST"])
def email_inbound():
    try:
        data = request.json or {}
        if not data:
            return jsonify({"ok": False, "error": "empty_payload"}), 400

        text = str(data.get("text") or data.get("message") or data.get("body") or "").strip()
        if not text:
            return jsonify({"ok": False, "error": "missing_text"}), 400

        deal_id = int(data.get("deal_id") or 0)
        person_id = int(data.get("person_id") or 0)
        email = str(data.get("email") or "").strip().lower()
        phone = whatsapp.normalize_phone(data.get("phone") or data.get("telefone") or "")
        source_id = str(data.get("message_id") or data.get("event_id") or data.get("id") or "").strip()
        identity = email or phone or f"deal:{deal_id or person_id or 0}"
        key = processed_key(identity, source_id, text)

        fd = None
        try:
            fd = acquire_lock(PROCESSED_LOCK_FILE)
            refreshed = load_processed()
            if key in refreshed:
                print(f"[DUPLICADO_EMAIL_INBOUND] key={key}")
                return jsonify({"ok": True, "confirmed": True, "duplicated": True})
        finally:
            if fd is not None:
                release_lock(fd, PROCESSED_LOCK_FILE)

        lead_state = resolve_lead_state_from_payload(data)
        clear_email_runtime_state(
            deal_ids=[deal_id] if deal_id > 0 else [],
            person_id=person_id or extract_entity_id((lead_state.get("person") or {}).get("id")),
            email=email,
            phone=phone,
        )
        history_key = f"email:deal={deal_id}" if deal_id > 0 else (f"email:person={person_id}" if person_id > 0 else f"email:{identity}")
        append_history(history_key, "in", text, step=0, source="email_inbound")
        decision = build_agent_decision(
            {
                **data,
                "channel": "email",
                "text": text,
                "message": text,
                "deal_id": deal_id,
                "person_id": person_id,
                "email": email,
                "phone": phone,
            }
        )
        intent = str(decision.get("intent") or "").strip().lower()
        note_prefix = "Inbound email"

        if intent == "nao_interessado":
            if phone:
                append_to_blocklist(phone, "sem_interesse_email")
                whatsapp.clear_after_hours_pending(phone)
            add_respondido_tag_for_lead(lead_state)
            update_crm_status_for_lead(lead_state, "lead_sem_interesse")
            move_related_deals_to_terminal_stage(lead_state, reason="email_sem_interesse")
            append_crm_note_for_lead(lead_state, f"{note_prefix} negativo. Texto: {text}")
        elif intent == "numero_errado":
            if phone:
                append_to_blocklist(phone, "numero_errado_email")
                whatsapp.clear_after_hours_pending(phone)
            add_respondido_tag_for_lead(lead_state)
            update_crm_status_for_lead(lead_state, "numero_nao_corresponde")
            move_related_deals_to_terminal_stage(lead_state, reason="email_numero_errado")
            append_crm_note_for_lead(lead_state, f"{note_prefix} numero errado. Texto: {text}")
        elif intent in {"empresa_incompativel", "setor_errado"}:
            add_respondido_tag_for_lead(lead_state)
            update_crm_status_for_lead(lead_state, "lead_sem_interesse")
            move_related_deals_to_terminal_stage(lead_state, reason="email_empresa_incompativel")
            append_crm_note_for_lead(lead_state, f"{note_prefix} empresa incompativel. Texto: {text}")
        elif intent == "agendamento":
            add_respondido_tag_for_lead(lead_state)
            update_crm_status_for_lead(lead_state, "lead_interessado")
            move_related_deals_to_scheduled(lead_state)
            create_scheduling_activity_for_lead(lead_state, f"{note_prefix} com pedido de agendamento: {text}")
            append_crm_note_for_lead(lead_state, f"{note_prefix} com pedido de agendamento. Texto: {text}")
        elif intent == "interesse":
            add_respondido_tag_for_lead(lead_state)
            update_crm_status_for_lead(lead_state, "lead_interessado")
            append_crm_note_for_lead(lead_state, f"{note_prefix} com interesse. Texto: {text}")
        elif intent in {"duvida", "neutro"}:
            add_respondido_tag_for_lead(lead_state)
            append_crm_note_for_lead(lead_state, f"{note_prefix} com duvida/interacao. Texto: {text}")

        commit_processed_key(key)
        print(
            f"[EMAIL_INBOUND_PROCESSADO] deal={deal_id} person={person_id} email={email or '-'} "
            f"intent={intent or '-'} action={decision.get('action') or '-'}"
        )
        return jsonify({"ok": True, "confirmed": True, **decision})
    except Exception as e:
        print("[ERRO_EMAIL_INBOUND]", str(e))
        return jsonify({"ok": False, "error": "email_inbound_failed"}), 500


@app.route("/history/outbound", methods=["POST"])
def sync_outbound_history():
    try:
        data = request.json or {}
        phone_raw = data.get("phone") or data.get("number") or ""
        message_raw = data.get("message") or data.get("text") or ""
        if not phone_raw or not message_raw:
            return jsonify({"ok": False})

        phone = whatsapp.normalize_phone(phone_raw)
        message = str(message_raw).strip()
        if not whatsapp.is_valid_phone(phone) or not message:
            return jsonify({"ok": False})
        if message.strip().upper() == "[MENSAGEM_SEM_TEXTO]":
            print(f"[OUTBOUND_SEM_TEXTO_IGNORADO] {phone}")
            return jsonify({"ok": True, "ignored": True})

        if has_recent_confirmed_outbound_entry(phone, message, within_seconds=180):
            print(f"[OUTBOUND_MANUAL_DUPLICADO] {phone}")
            return jsonify({"ok": True, "duplicated": True})

        history = append_history(phone, "out", message, step=0, source="sync")
        print(f"[OUTBOUND_MANUAL_SINCRONIZADO] {phone}")
        return jsonify({"ok": True, "items": len(history)})
    except Exception as e:
        print("[ERRO_OUTBOUND_SYNC]", str(e))
        return jsonify({"ok": False})


@app.route("/inbound", methods=["POST"])
@app.route("/inbox", methods=["POST"])
def inbox():
    try:
        data = request.json or {}
        if not data:
            return jsonify({"ok": False})

        evol_data = data.get("data", {})
        phone_raw = (
            data.get("phone")
            or data.get("remoteJid")
            or evol_data.get("key", {}).get("remoteJid")
            or evol_data.get("from")
            or data.get("number")
        )
        message_raw = (
            data.get("message")
            or evol_data.get("message", {}).get("conversation")
            or evol_data.get("message", {}).get("extendedTextMessage", {}).get("text")
            or evol_data.get("body")
            or data.get("text")
        )
        msg_id = (
            data.get("id")
            or data.get("messageId")
            or evol_data.get("key", {}).get("id")
            or evol_data.get("id")
        )
        source = str(data.get("source") or "realtime").strip().lower()
        force_process = bool(data.get("force_process"))

        if evol_data.get("key", {}).get("fromMe") is True:
            return jsonify({"ok": True})
        if is_system_jid(phone_raw):
            return jsonify({"ok": True})
        if not phone_raw or not message_raw:
            return jsonify({"ok": False})

        phone = whatsapp.normalize_phone(str(phone_raw).split("@")[0])
        message = str(message_raw).strip()
        print(f"[INBOUND_RECEBIDO] {phone}: {message}")

        if not is_brazil_business_hours():
            print(f"[INBOUND_FORA_HORARIO_BR] telefone={phone}")

        if not is_phone_allowed_for_auto_reply(phone):
            print(f"[BLOCK_NON_LEAD_AUTO_REPLY] telefone={phone}")
            return jsonify({"ok": True, "ignored": True, "reason": "not_active_lead"})

        import os
        flag = os.path.join(os.path.dirname(__file__), "data", "auto_reply_disabled.flag")
        if os.path.exists(flag):
            print(f"[AUTO_REPLY_BLOCKED_GLOBAL] telefone={phone}")
            return jsonify({"ok": True, "auto_reply": False})


        try:
            import os as _os
            _flag = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data", "auto_reply_disabled.flag")
            if _os.path.exists(_flag):
                print(f"[AUTO_REPLY_HARD_BLOCK] telefone={phone} inbound_capturado_sem_resposta")
                return jsonify({"ok": True, "auto_reply": False, "reason": "disabled"})
        except Exception as e:
            print(f"[AUTO_REPLY_FLAG_ERROR] {e}")

        if not _session_guard_allows_inbound(data):
            print(f"[INBOUND_OLD_SESSION_IGNORED] telefone={phone} texto={message}")
            return jsonify({"ok": True, "ignored": True, "reason": "old_session"})
        if not whatsapp.is_valid_phone(phone):
            whatsapp.mark_invalid(phone, "invalid_phone_inbound")
            return jsonify({"ok": True})
        if not message:
            return jsonify({"ok": True})
        if message.strip().upper() == "[MENSAGEM_SEM_TEXTO]":
            print(f"[INBOUND_SEM_TEXTO_IGNORADO] {phone}")
            return jsonify({"ok": True})

        if phone in load_manual_blocklist() and phone not in TEST_WHITELIST:
            print(f"[BLOQUEADO_MANUAL] {phone} ignorado")
            commit_processed_key(processed_key(phone, msg_id, message))
            return jsonify({"ok": True})

        # --- NOVA LOGICA DE ANALISE ANTES DE DEDUPLICACAO ---
        print(f"[INBOUND_ANALISE] {phone}: {message}")

        _txt = (message or "").lower()
        if detect_pause_not_now_intent(message):
            print(f"[PAUSE_NOT_NOW_PRECHECK] telefone={phone} texto={message}")
            _txt = ""

        # 👉 CASO: IDEIA / EXEMPLO
        if any(x in _txt for x in ["ideia", "exemplo", "promoção", "promocao", "campanha"]):
            print("[FLOW_EXEMPLO_DIRETO]", phone)
            import requests
            numero = str(phone or "").replace("@s.whatsapp.net", "").replace("+", "").strip()
            if numero and not numero.startswith("55"):
                numero = "55" + numero

            partes = [
                "Claro 🙂 Vou te dar um exemplo bem direto pra supermercado:\n\nQR Code no cupom ou na loja levando para uma roleta ou raspadinha da Copa.",
                "O cliente participa, pode ganhar algo e deixa dados como nome e WhatsApp.\n\nIsso gera engajamento na hora e cria base própria pra próximas campanhas."
            ]

            for p in partes:
                print("[INBOX_SEND_BLOCKED_EMISSOR_UNICO]", numero, p[:80])

            return {"ok": True, "flow": "exemplo"}



        _txt = (message or "").lower()
        if detect_pause_not_now_intent(message):
            _txt = ""
        if any(x in _txt for x in ["ideia", "promoção", "promocao", "campanha", "exemplo", "ação", "acao"]):
            print("[FORCE_PITCH_FLOW]", phone)
            return send_reply(phone, "Claro 🙂 Vou te dar um exemplo bem direto pra supermercado:\n\nVocês podem colocar um QR Code no cupom fiscal ou nas peças da loja levando para uma roleta ou raspadinha digital.|||O cliente participa, pode ganhar algo e deixa dados como nome, WhatsApp e loja.\n\nIsso gera engajamento e cria base própria pra próximas campanhas. Quer que eu adapte isso pra vocês?")


        _txt = (message or "").lower()
        if detect_pause_not_now_intent(message):
            _txt = ""
        if any(x in _txt for x in ["ideia", "promoção", "promocao", "campanha", "exemplo", "ação", "acao"]):
            print("[FORCE_INTERESSE_INTENT]", phone)
            intent = "interesse"

        
        # Detecta intent logo no inicio para o log obrigatorio
        current_intent = "neutro"
        if is_hard_negative_message(message): current_intent = "nao_interessado_hard"
        elif is_opt_out(message): current_intent = "nao_interessado_optout"
        elif detect_closing_intent(message): current_intent = "fechamento_" + detect_closing_intent(message)
        elif detect_referral_intent(message): current_intent = "indicacao"
        elif detect_pause_not_now_intent(message): current_intent = "pause_not_now"
        elif detect_scheduling_intent(message): current_intent = "agendamento"
        elif detect_positive_intent(message): current_intent = "interesse"
        elif detect_neutral_intent(message): current_intent = "duvida"
        
        print(f"[INTENT_DETECTED] telefone={phone} intent={current_intent} texto={message}")
        try:
            _deal_id = None
            try:
                if "lead_state" in locals() and isinstance(lead_state, dict):
                    _deal_id = lead_state.get("deal_id") or lead_state.get("deal")
            except Exception:
                _deal_id = None
            _route = register_inbound(phone=phone, deal_id=_deal_id, intent=current_intent, message=message)
            print(f"[AGENT_ROUTE] telefone={phone} intent={current_intent} state={_route.get('state')} action={_route.get('action')}")
        except Exception as _e:
            print(f"[AGENT_ROUTE_ERROR] telefone={phone} erro={_e}")

        
        key = processed_key(phone, msg_id, message)
        is_duplicated = False
        fd = None
        try:
            fd = acquire_lock(PROCESSED_LOCK_FILE)
            refreshed = load_processed()

            if key in refreshed:
                print(f"[DUPLICADO_STRICT] Mensagem {key} já processada.")

                intent_safe = locals().get("intent_name") or locals().get("intent") or ""

                if intent_safe not in ("interesse", "positive_interest", "fechamento_wrong_number", "wrong_contact", "fechamento_sem_interesse", "optout"):
                    print(f"[DUPLICADO_STRICT_IGNORADO] Mensagem {key} já processada. Ignorando execução.")
                    return jsonify({"confirmed": True, "duplicated": True, "ok": True})

                print(f"[DUPLICADO_REPROCESSAR_CRITICO] telefone={phone} intent={intent_safe} texto={text}")

            else:
                refreshed.add(key)
                save_processed(refreshed)

        finally:
            if fd is not None:
                release_lock(fd, PROCESSED_LOCK_FILE)

        
        if is_duplicated and not force_process:
            return jsonify({"ok": True, "confirmed": True, "duplicated": True})

        print(f"[INBOUND_EXECUTANDO] {phone}: {message}")

        if is_hard_negative_message(message):
            try:
                lead_state = validate_lead_with_cache(phone)
            except Exception as e:
                print(f"[ERRO_CRM_FIND] {e}")
                lead_state = {
                    "valid": True,
                    "person": {"id": -1, "name": "Lead"},
                    "deals": [],
                    "bypass": True,
                    "source": "error_fallback",
                }
            try:
                whatsapp.blocklist_add(phone, reason="sem_interesse")
            except Exception:
                pass
            append_to_blocklist(phone, "sem_interesse")
            whatsapp.clear_after_hours_pending(phone)
            print(f"[BLOCKLIST_ADICIONADO] telefone={phone} motivo=sem_interesse_hard_rule")
            print(f"[INTENT_NEGATIVO_HARD_RULE] telefone={phone}")
            try:
                add_respondido_tag_for_lead(lead_state)
                add_wa_respondeu_tag_for_lead(lead_state)
                update_crm_status_for_lead(lead_state, "lead_sem_interesse")
                clear_email_runtime_state_for_lead(lead_state, phone=phone)
                move_related_deals_to_terminal_stage(lead_state, reason="sem_interesse_hard_rule")
                append_crm_note_for_lead(lead_state, f"Lead informou nao ter interesse. Texto: {message}")
                update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="opt_out")
            except Exception:
                pass
            reply = "Perfeito, obrigada por avisar. Vou encerrar por aqui para não incomodar."
            print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")
            commit_processed_key(key)
            return jsonify({"ok": True, "status": "sem_interesse_hard_rule", "confirmed": True, "auto_reply_blocked": True})
            ok = blocked_whatsapp_send(phone, reply, bypass_manual_blocklist=True)
            if ok:
                append_history(phone, "out", reply, step=0)
                print(f"[RESPOSTA_ENVIADA] {phone}")
            commit_processed_key(key)
            return jsonify({"ok": True, "status": "sem_interesse_hard_rule", "confirmed": bool(ok)})


        if not dentro_do_horario() and not is_test_whitelist_phone(phone):
            lead_state = None
            try:
                lead_state = validate_lead_with_cache(phone)
            except Exception as e:
                print(f"[ERRO_CRM_FIND] {e}")
                print("[FLUXO_SEM_CRM]")
            if lead_state and has_stopped_whatsapp_conversation(lead_state, phone):
                append_crm_note_for_lead(lead_state, f"Inbound WhatsApp fora do horario em conversa ja encerrada. Sem resposta automatica. Texto: {message}")
                commit_processed_key(key)
                return jsonify({"ok": True, "confirmed": True, "conversation_stopped": True, "reason": "after_hours"})
            if lead_state:
                update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="replied")
            if not is_auto_reply_router_enabled():
                if lead_state:
                    add_respondido_tag_for_lead(lead_state)
                    add_wa_respondeu_tag_for_lead(lead_state)
                    clear_email_runtime_state_for_lead(lead_state, phone=phone)
                    append_crm_note_for_lead(lead_state, f"Inbound WhatsApp fora do horario. Auto-resposta bloqueada ate existir roteador por origem/tag. Texto: {message}")
                    update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="replied")
                commit_processed_key(key)
                return jsonify({"ok": True, "confirmed": True, "auto_reply_blocked": True, "reason": "after_hours_no_router"})
            payload = handle_fora_do_horario(
                phone,
                message,
                msg_id=msg_id,
                timestamp=data.get("timestamp"),
                source=source,
                lead_state=lead_state,
            )
            if payload.get("ok") is True:
                commit_processed_key(key)
            return jsonify(payload)

        if False and has_recent_history_entry(phone, "in", message, within_seconds=900) and not is_test_whitelist_phone(phone):
            print(f"[DUPLICADO_HISTORY_INBOUND] {phone} ignorado")
            if (locals().get("intent_name") or locals().get("intent") or "") in ("interesse", "positive_interest", "fechamento_wrong_number", "wrong_contact", "fechamento_sem_interesse", "optout"):
                print(f"[HISTORY_REPROCESSAR_CRITICO] telefone={phone} texto={text}")
            else:
                return jsonify({"confirmed": True, "duplicated": True, "ok": True})

        if extract_phone_candidates(message, current_phone=phone) or extract_email_candidates(message):
            try:
                lead_state = validate_lead_with_cache(phone)
            except Exception as e:
                print(f"[ERRO_CRM_FIND] {e}")
                lead_state = {
                    "valid": True,
                    "person": {"id": -1, "name": "Lead"},
                    "deals": [],
                    "bypass": True,
                    "source": "error_fallback",
                }
                print("[FLUXO_SEM_CRM]")
            if handle_referral_redirect(phone, message, lead_state):
                commit_processed_key(key)
                return jsonify({"ok": True})

        is_bot_menu, bot_reason = should_ignore_bot_menu(message)
        if is_bot_menu:
            print(f"[BOT_MENU_DETECTADO] {phone} motivo={bot_reason}")
            if not is_auto_reply_router_enabled():
                commit_processed_key(key)
                return jsonify({"ok": True, "confirmed": True, "auto_reply_blocked": True, "reason": "bot_menu_no_router"})
            if handle_bot_menu_navigation(phone, message):
                commit_processed_key(key)
                return jsonify({"ok": True, "confirmed": True})
            return jsonify({"ok": False, "confirmed": False})

        bot_keywords = [
            "digite", "opção", "escolha", "atendimento", "automático",
            "automatico", "assistente virtual", "sou um bot", "mensagem automática",
            "menu", "pressione", "configurações", "ajuda", "0 -", "1 -", "2 -", "3 -",
        ]
        msg_lower = message.lower()
        is_bot = any(keyword in msg_lower for keyword in bot_keywords)
        if len(message) > 400 and (message.count("\n") > 5 or sum(c.isdigit() for c in message) > 20):
            is_bot = True
        if is_bot:
            print(f"[BOT_MENU_DETECTADO] {phone} motivo=rule_fallback")
            if not is_auto_reply_router_enabled():
                commit_processed_key(key)
                return jsonify({"ok": True, "confirmed": True, "auto_reply_blocked": True, "reason": "bot_menu_no_router"})
            if handle_bot_menu_navigation(phone, message):
                commit_processed_key(key)
                return jsonify({"ok": True, "confirmed": True})
            return jsonify({"ok": False, "confirmed": False})

        try:
            lead_state = validate_lead_with_cache(phone)
        except Exception as e:
            print(f"[ERRO_CRM_FIND] {e}")
            lead_state = {
                "valid": True,
                "person": {"id": -1, "name": "Lead"},
                "deals": [],
                "bypass": True,
                "source": "error_fallback",
            }
            print("[FLUXO_SEM_CRM]")

        person = lead_state.get("person") or {}
        deals = lead_state.get("deals") or []
        if str(lead_state.get("source") or "").strip().lower() == "no_crm" and phone not in TEST_WHITELIST:
            print(f"[BLOQUEADO_FORA_CRM] {phone}")
            commit_processed_key(key)
            return jsonify({"ok": True})
        if not lead_state.get("valid"):
            if phone in TEST_WHITELIST:
                print("[LEAD_NAO_ENCONTRADO_CONTINUANDO]")
                lead_state = {
                    "valid": True,
                    "person": {"id": -1, "name": "Teste"},
                    "deals": [],
                    "bypass": True,
                    "source": "whitelist",
                }
                person = lead_state["person"]
                deals = []
            else:
                print(f"[BLOQUEADO_FORA_CRM] {phone}")
                commit_processed_key(key)
                return jsonify({"ok": True})

        if lead_state.get("bypass") or not person or not person.get("id"):
            if phone in TEST_WHITELIST:
                print(f"[TEST_MODE] {phone} autorizado fora do CRM")
                print("[FLUXO_SEM_CRM]")
                person = person if person else {"id": -1, "name": "Teste"}
            else:
                print(f"[BLOQUEADO_FORA_CRM] {phone}")
                commit_processed_key(key)
                return jsonify({"ok": True})

        lead = build_lead_payload_from_state(phone, lead_state)
        clear_email_runtime_state_for_lead(lead_state, phone=phone)
        if has_stopped_whatsapp_conversation(lead_state, phone):
            append_crm_note_for_lead(lead_state, f"Inbound WhatsApp recebido em conversa ja encerrada. Sem resposta automatica. Texto: {message}")
            whatsapp.clear_after_hours_pending(phone)
            commit_processed_key(key)
            return jsonify({"ok": True, "confirmed": True, "conversation_stopped": True})
        if detect_pause_not_now_intent(message):
            add_respondido_tag_for_lead(lead_state)
            add_wa_respondeu_tag_for_lead(lead_state)
            clear_email_runtime_state_for_lead(lead_state, phone=phone)
            pause_whatsapp_warm_cadence_for_lead(lead_state, phone=phone)
            append_crm_note_for_lead(lead_state, f"Lead pediu pausa/contato futuro. Sem resposta automatica insistente. Texto: {message}")
            create_soft_negative_followup_for_lead(lead_state, f"Follow-up leve apos pausa solicitada pelo lead. Texto: {message}")
            history = append_history(phone, "in", message, step=0)
            update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="pause_not_now")
            whatsapp.clear_after_hours_pending(phone)
            print(f"[PAUSE_NOT_NOW] telefone={phone} action=pause_automation send=0 texto={message}")
            commit_processed_key(key)
            return jsonify({"ok": True, "confirmed": True, "intent": "pause_not_now", "action": "pause_automation", "should_send": False, "history_items": len(history)})
        update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="replied")
        lead = enrich_lead_with_whatsapp_state(lead, lead_state, phone)

        if not is_auto_reply_router_enabled():
            add_respondido_tag_for_lead(lead_state)
            add_wa_respondeu_tag_for_lead(lead_state)
            append_crm_note_for_lead(lead_state, f"Inbound WhatsApp recebido. Auto-resposta bloqueada ate existir roteador por origem/tag. Texto: {message}")
            whatsapp.clear_after_hours_pending(phone)
            history = append_history(phone, "in", message, step=0)
            commit_processed_key(key)
            return jsonify({"ok": True, "confirmed": True, "auto_reply_blocked": True, "history_items": len(history)})

        if is_opt_out(message):
            print(f"[INTENT_NEGATIVO] telefone={phone}")
            closing_reply = "Perfeito, obrigada por avisar. Vou encerrar por aqui para não incomodar."
            append_to_blocklist(phone, "opt_out")
            whatsapp.clear_after_hours_pending(phone)
            add_respondido_tag_for_lead(lead_state)
            update_crm_status_for_lead(lead_state, "lead_sem_interesse")
            move_related_deals_to_terminal_stage(lead_state, reason="opt_out")
            append_crm_note_for_lead(lead_state, f"Lead informou nao ter interesse. Texto: {message}")
            history = append_history(phone, "in", message, step=0)
            update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="opt_out")
            print(f"[CONVERSATION_STOPPED_NO_REPLY] telefone={phone} motivo=opt_out")
            commit_processed_key(key)
            return jsonify({"ok": True, "conversation_stopped": True, "history_items": len(history)})

        closing_key = detect_closing_intent(message)
        if closing_key:
            handle_closing_intent(phone, message, closing_key, lead_state)
            commit_processed_key(key)
            return jsonify({"ok": True})

        if detect_referral_intent(message):
            if handle_referral_redirect(phone, message, lead_state):
                commit_processed_key(key)
                return jsonify({"ok": True})
            history = append_history(phone, "in", message, step=0)
            update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="referred")
            print(f"[CONVERSATION_STOPPED_NO_REPLY] telefone={phone} motivo=indicacao_sem_contato")
            commit_processed_key(key)
            return jsonify({"ok": True, "conversation_stopped": True, "history_items": len(history)})

        if not is_agent_decide_enabled():
            if detect_positive_intent(message):
                print(f"[INTENT_POSITIVO] telefone={phone}")
                add_respondido_tag_for_lead(lead_state)
                update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="interested")
                if detect_scheduling_intent(message):
                    move_related_deals_to_scheduled(lead_state)
                    create_scheduling_activity_for_lead(lead_state, f"Inbound WhatsApp com pedido de agendamento: {message}")
                    append_crm_note_for_lead(lead_state, f"Inbound WhatsApp com pedido de agendamento. Texto: {message}")
            elif detect_neutral_intent(message):
                print(f"[INTENT_NEUTRO] telefone={phone}")
                add_respondido_tag_for_lead(lead_state)
                update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent=classify_conversation_intent(message))
            elif detect_scheduling_intent(message):
                print(f"[INTENT_AGENDAMENTO] telefone={phone}")
                add_respondido_tag_for_lead(lead_state)
                update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent="interested")
                move_related_deals_to_scheduled(lead_state)
                create_scheduling_activity_for_lead(lead_state, f"Inbound WhatsApp com pedido de agendamento: {message}")
                append_crm_note_for_lead(lead_state, f"Inbound WhatsApp com pedido de agendamento. Texto: {message}")

        if source == "after_hours_resume" and has_recent_history_entry(phone, "in", message, within_seconds=48 * 3600):
            history = get_history_items(phone)
        else:
            history = append_history(phone, "in", message, step=0)
        lead = enrich_lead_with_whatsapp_state(lead, lead_state, phone)
        current_step = max(infer_step_from_history(history), infer_step_from_deals(deals))
        inbound_messages = [
            item.get("message", "")
            for item in history
            if str(item.get("direction") or "").strip().lower() == "in"
        ]
        agent_payload = maybe_handle_inbound_agent_decision(
            phone=phone,
            message=message,
            source=source,
            lead_state=lead_state,
            lead=lead,
            history=history,
            current_step=current_step,
            key=key,
        )
        if agent_payload is not None:
            return jsonify(agent_payload)

        latest_inbound = (message or "").strip().lower()
        lead_name = str((lead or {}).get("name") or "").strip().lower()
        generic_names = {"", "cliente", "lead", "contato", "responsavel", "responsável"}
        positive_name_gate = (
            latest_inbound in {"pode sim", "sou eu", "sim", "claro", "pode falar", "pode", "fala", "pode perguntar"}
            or latest_inbound.startswith("pode sim")
            or latest_inbound.startswith("sou eu")
        )
        negative_gate = any(x in latest_inbound for x in [
            "nao estamos interessados", "não estamos interessados", "nao tenho interesse", "não tenho interesse",
            "sem interesse", "nao interessado", "não interessado", "nao quero", "não quero",
            "agradeco o interesse", "agradeço o interesse", "obrigado", "obrigada"
        ])
        if negative_gate:
            reply = "Perfeito, obrigada por avisar. Vou encerrar por aqui para não incomodar."
        elif positive_name_gate and lead_name in generic_names:
            reply = "Perfeito. Como posso te chamar?"
        else:
            reply = pitch.build_reply(
                lead,
                inbound_messages,
                current_step=current_step,
                allow_opening=False,
            )
        print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")

        if False and has_recent_history_entry(phone, "out", reply, within_seconds=1800):
            print(f"[DUPLICADO_HISTORY_OUTBOUND] {phone} ignorado")
            commit_processed_key(key)
            return jsonify({"ok": True})
        if not can_emit_reply(phone, reply, within_seconds=REPLY_WINDOW_SECONDS):
            print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
            commit_processed_key(key)
            return jsonify({"ok": True})

        if not whatsapp.healthcheck():
            print("[AVISO] API pode estar offline, mas tentando enviar mesmo assim...")

        ok = blocked_whatsapp_send(phone, reply)
        if ok:
            append_history(phone, "out", reply, step=current_step)
            update_whatsapp_conversation_state_for_lead(lead_state, phone, message, intent=classify_conversation_intent(message), last_bot_reply=reply)
            whatsapp.clear_after_hours_pending(phone)
            print(f"[RESPOSTA_ENVIADA] {phone}")
            if lead_state.get("bypass") or not person or int(person.get("id") or -1) <= 0:
                print(f"[RESPOSTA_ENVIADA_SEM_CRM] {phone}")
            if source in {"backlog", "after_hours_resume"}:
                print(f"[MSG_RESPONDIDA_BACKLOG] {phone}")
            commit_processed_key(key)
        else:
            release_reply_guard(phone)
            print(f"[FALHA_ENVIO] {phone}")
            return jsonify({"ok": False, "confirmed": False})

        return jsonify({"ok": True, "confirmed": True})
    except Exception as e:
        print("[ERRO_INBOX]", str(e))
        return jsonify({"ok": False})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
