import hashlib
import json
import logging
import os
import re
import sys
import time
import unicodedata
from datetime import datetime
from threading import Lock

import requests
from flask import Flask, jsonify, request

from crm.pipedrive_client import PipedriveClient
from logic.whatsapp_pitch_engine import WhatsAppPitchEngine
from services.whatsapp_service import WhatsAppService


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
crm.ensure_deal_labels(["cad1", "respondido"])

BLOCKLIST_FILE = "logs/whatsapp_manual_blocklist.json"
PROCESSED_FILE = "inbound_processed.json"
PROCESSED_LOCK_FILE = "inbound_processed.json.lock"
HISTORY_FILE = "logs/whatsapp_message_history.json"
TEST_WHITELIST = {"5535920002020", "35920002020", "5511998804191", "11998804191"}
CRM_CACHE_TTL_SECONDS = 24 * 60 * 60
REPLY_WINDOW_SECONDS = 30 * 60
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/auto")
OPENROUTER_TOKEN = os.getenv("OPENROUTER_API_KEY") or getattr(WhatsAppPitchEngine, "OPENROUTER_TOKEN", "")
lead_cache = {}
reply_guard = {}
reply_guard_lock = Lock()
FORA_HORARIO_STATUS = "aguardando_horario"
FORA_HORARIO_MENSAGEM = "Oi, aqui é a Carol da Mand. Não estou disponível no momento, mas responderei assim que possível 😊"


def dentro_do_horario():
    agora = datetime.now()
    if agora.weekday() <= 4:
        return agora.hour < 17 or (agora.hour == 17 and agora.minute <= 30)
    return False


def append_to_blocklist(phone, reason):
    try:
        os.makedirs("logs", exist_ok=True)
        data = []
        if os.path.exists(BLOCKLIST_FILE):
            with open(BLOCKLIST_FILE, "r", encoding="utf-8-sig") as f:
                loaded = json.load(f)
                if isinstance(loaded, list):
                    data = loaded
        if not any(str(item.get("telefone")) == phone for item in data if isinstance(item, dict)):
            data.append({
                "telefone": phone,
                "reason": reason,
                "tag": "blocked_automatic",
                "created_at": datetime.now().isoformat(),
            })
            with open(BLOCKLIST_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"[BLOCKLIST_ADICIONADO] telefone={phone} motivo={reason}")
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
                return {
                    whatsapp.normalize_phone(item["telefone"])
                    for item in data
                    if isinstance(item, dict) and item.get("telefone")
                }
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


def save_history(history):
    os.makedirs("logs", exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f)


def append_history(phone, direction, message, step=0):
    history = load_history()
    items = history.get(phone, [])
    items.append({
        "direction": direction,
        "message": str(message or "").strip(),
        "step": int(step or 0),
        "created_at": datetime.now().isoformat(),
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


def can_emit_reply(phone, message, within_seconds=1800):
    normalized_phone = whatsapp.normalize_phone(phone)
    normalized_message = str(message or "").strip()
    if not normalized_phone or not normalized_message:
        return False
    now = time.time()
    with reply_guard_lock:
        expired = [
            key
            for key, created_at in reply_guard.items()
            if now - float(created_at or 0) > within_seconds
        ]
        for key in expired:
            reply_guard.pop(key, None)
        guard_key = normalized_phone
        if guard_key in reply_guard:
            return False
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


def is_opt_out(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    return any(token in normalized for token in ("pare", "não quero", "nao quero", "sair"))


def normalize_intent_text(message):
    text = str(message or "").strip().lower()
    decomposed = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", ascii_text).strip()


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


def classify_inbound_intent_openrouter(message):
    api_key = str(OPENROUTER_TOKEN or "").strip()
    if not api_key:
        return "unknown", "missing_token"
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Classifique a mensagem recebida no WhatsApp. Responda APENAS JSON puro "
                    "{\"intent\":\"bot_menu|human|unknown\",\"reason\":\"...\"}. Use bot_menu para "
                    "resposta automatica, menu numerado, URA, assistente virtual ou atendimento automatico "
                    "em que o SDR nao deve responder. Use human para pessoa respondendo livremente."
                ),
            },
            {"role": "user", "content": str(message or "").strip()[:2000]},
        ],
        "temperature": 0,
    }
    try:
        response = requests.post(
            OPENROUTER_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=8,
        )
        if response.status_code != 200:
            return "unknown", f"http_{response.status_code}"
        body = response.json()
        content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        content = re.sub(r"^```(?:json)?|```$", "", content, flags=re.IGNORECASE).strip()
        parsed = json.loads(content)
        intent = str(parsed.get("intent") or "").strip().lower()
        reason = str(parsed.get("reason") or "").strip()
        if intent in {"bot_menu", "human", "unknown"}:
            return intent, reason
    except Exception as exc:
        return "unknown", f"erro={exc}"
    return "unknown", "invalid_payload"


def choose_bot_menu_option_openrouter(message):
    api_key = str(OPENROUTER_TOKEN or "").strip()
    if not api_key:
        return "", "missing_token"
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Voce recebe um menu/URA de WhatsApp. Escolha a melhor resposta curta e EXATA para navegar "
                    "ate um humano, vendas, comercial ou compras. Responda APENAS JSON puro "
                    "{\"reply\":\"token_exato\",\"reason\":\"...\"}. "
                    "Se o menu pedir um numero, devolva so o numero. "
                    "Se o menu pedir uma palavra, devolva so a palavra exata do menu. "
                    "Priorize nesta ordem: humano/atendente, comercial/vendas, compras, representante/consultor, "
                    "sac/atendimento. Nao invente resposta fora do menu. "
                    "So use \"atendente\" ou \"menu\" se isso realmente aparecer como opcao valida."
                ),
            },
            {"role": "user", "content": str(message or "").strip()[:2500]},
        ],
        "temperature": 0,
    }
    try:
        response = requests.post(
            OPENROUTER_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=8,
        )
        if response.status_code != 200:
            return "", f"http_{response.status_code}"
        body = response.json()
        content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        content = re.sub(r"^```(?:json)?|```$", "", content, flags=re.IGNORECASE).strip()
        parsed = json.loads(content)
        reply = str(parsed.get("reply") or "").strip()
        reason = str(parsed.get("reason") or "").strip()
        if re.fullmatch(r"[0-9]{1,2}|[#*]|[A-Za-z0-9][A-Za-z0-9 _./-]{0,40}", reply, flags=re.IGNORECASE):
            return reply, reason or "openrouter"
    except Exception as exc:
        return "", f"erro={exc}"
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

    reply, reason = choose_bot_menu_option_openrouter(message)
    if reply:
        return reply, f"openrouter:{reason}"

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


def handle_bot_menu_navigation(phone, message):
    reply, reason = choose_bot_menu_option(message)
    append_history(phone, "in", message, step=0)
    if not reply:
        print(f"[BOT_MENU_SEM_ROTA] {phone}")
        return False
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")
    print(f"[BOT_MENU_NAVEGACAO] telefone={phone} resposta={reply} motivo={reason}")
    if has_recent_history_entry(phone, "out", reply, within_seconds=1800):
        print(f"[BOT_MENU_DUPLICADO] {phone} resposta={reply}")
        return True
    if not can_emit_reply(phone, reply, within_seconds=REPLY_WINDOW_SECONDS):
        print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
        return False
    ok = whatsapp.send_message(phone, reply)
    if ok:
        append_history(phone, "out", reply, step=0)
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
    intent, reason = classify_inbound_intent_openrouter(message)
    print(f"[INTENT_OPENROUTER] intent={intent} reason={reason}")
    return intent == "bot_menu", f"openrouter:{reason}"


def detect_closing_intent(message):
    normalized = re.sub(r"\s+", " ", str(message or "").strip().lower())
    if not normalized:
        return ""

    no_interest_tokens = (
        "sem interesse",
        "sem interesse nisso",
        "não tenho interesse",
        "nao tenho interesse",
        "não me interessa",
        "nao me interessa",
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
    )
    if any(token in normalized for token in no_interest_tokens):
        return "no_interest"

    wrong_number_tokens = (
        "numero errado",
        "número errado",
        "telefone errado",
        "ligou errado",
        "falou com a pessoa errada",
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


def related_deals_from_state(lead_state):
    current_person = dict((lead_state or {}).get("person") or {})
    current_person_id = extract_entity_id(current_person.get("id"))
    current_org_id = extract_entity_id(current_person.get("org_id"))
    if current_person_id > 0 or current_org_id > 0:
        return crm.find_open_deals_by_person_or_org(person_id=current_person_id, org_id=current_org_id)
    return []


def append_crm_note_for_lead(lead_state, note_text):
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


def update_crm_status_for_lead(lead_state, status_bot):
    current_person = dict((lead_state or {}).get("person") or {})
    current_person_id = extract_entity_id(current_person.get("id"))
    updated = False
    for deal in list(related_deals_from_state(lead_state) or []):
        deal_id = extract_entity_id((deal or {}).get("id"))
        if deal_id <= 0:
            continue
        try:
            ok = crm.update_deal(deal_id, {crm.STATUS_BOT_FIELD: str(status_bot or "").strip()})
        except Exception:
            ok = False
        updated = updated or bool(ok)
    if current_person_id > 0:
        try:
            crm.update_person(current_person_id, {crm.STATUS_BOT_FIELD: str(status_bot or "").strip()})
        except Exception:
            pass
    return updated


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
    ok = whatsapp.send_message(phone, FORA_HORARIO_MENSAGEM)
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
    append_to_blocklist(f"55{phone}", block_reason)
    if closing_key == "no_interest":
        print(f"[INTENT_NEGATIVO] telefone={phone}")
        update_crm_status_for_lead(lead_state, "sem_interesse")
        append_crm_note_for_lead(lead_state, "Lead informou nao ter interesse. Movido para blocklist.")
    elif closing_key in {"wrong_number", "unknown_person"}:
        print(f"[INTENT_ERRADO] telefone={phone}")
        update_crm_status_for_lead(lead_state, "numero_errado")
        append_crm_note_for_lead(lead_state, "Contato incorreto. Numero bloqueado.")
    history = append_history(phone, "in", message, step=0)
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={closing_message}")
    if has_recent_history_entry(phone, "out", closing_message, within_seconds=7200):
        print(f"[ENCERRAMENTO_DUPLICADO] {phone} motivo={closing_key}")
        return True
    if not can_emit_reply(phone, closing_message, within_seconds=REPLY_WINDOW_SECONDS):
        print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
        return True
    ok = whatsapp.send_message(phone, closing_message)
    if ok:
        append_history(phone, "out", closing_message, step=infer_step_from_history(history))
        print(f"[RESPOSTA_ENVIADA] {phone}")
        print(f"[ENCERRAMENTO_APLICADO] {phone} motivo={closing_key}")
    else:
        release_reply_guard(phone)
        print(f"[FALHA_ENVIO] {phone}")
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
    digest = hashlib.md5(f"{phone}:{message}".encode("utf-8")).hexdigest()
    return f"{phone}:{digest}"


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
    )
    if any(pattern in normalized for pattern in direct_patterns):
        return True
    if re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", str(message or ""), flags=re.IGNORECASE):
        if any(token in normalized for token in ("gerente", "compras", "comercial", "marketing", "responsavel", "responsÃ¡vel", "setor", "departamento", "segue", "contato")):
            return True
        return True

    has_contact = "contato" in normalized
    has_target_area = any(token in normalized for token in ("marketing", "comercial", "responsavel", "responsável"))
    return has_contact and has_target_area


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
            crm.update_person(person_id, update_payload)
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
    reply = "Perfeito, obrigado por indicar! Vou falar com ele(a)."
    print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")
    if not has_recent_history_entry(phone, "out", reply, within_seconds=1800):
        if not can_emit_reply(phone, reply, within_seconds=REPLY_WINDOW_SECONDS):
            print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
        else:
            ok = whatsapp.send_message(phone, reply)
            if ok:
                append_history(phone, "out", reply, step=infer_step_from_history(history))
                print(f"[RESPOSTA_ENVIADA] {phone}")
            else:
                release_reply_guard(phone)
                print(f"[FALHA_ENVIO] {phone}")
                return False

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
                crm.create_note(
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

    if target_phone:
        set_cached_lead(
            target_phone,
            valid=True,
            person=referral_person,
            deals=[target_deal],
            bypass=False,
            source="referral",
        )
    append_to_blocklist(f"55{phone}", "referral_redirect")
    print(f"[BLOCKLIST_NUMERO] telefone={phone}")
    print(f"[INTENT_REDIRECIONADO] telefone={phone}")
    print(f"[CONTATO_REDIRECIONADO] origem={phone} destino={target_phone or target_email} deal={deal_id}")
    update_crm_status_for_lead(lead_state, "encerrado")
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


@app.route("/", methods=["GET"])
def health():
    return jsonify({"ok": True})


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

        if has_recent_history_entry(phone, "out", message, within_seconds=180):
            print(f"[OUTBOUND_MANUAL_DUPLICADO] {phone}")
            return jsonify({"ok": True, "duplicated": True})

        history = append_history(phone, "out", message, step=0)
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
            return jsonify({"ok": True})

        key = processed_key(phone, msg_id, message)
        fd = None
        try:
            fd = acquire_lock(PROCESSED_LOCK_FILE)
            refreshed = load_processed()
            if key in refreshed:
                print(f"[DUPLICADO_INBOUND] Mensagem {key} já processada.")
                return jsonify({"ok": True, "confirmed": True, "duplicated": True})
        finally:
            if fd is not None:
                release_lock(fd, PROCESSED_LOCK_FILE)

        print(f"[INBOUND] {phone}: {message}")

        if not dentro_do_horario():
            lead_state = None
            try:
                lead_state = validate_lead_with_cache(phone)
            except Exception as e:
                print(f"[ERRO_CRM_FIND] {e}")
                print("[FLUXO_SEM_CRM]")
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

        if has_recent_history_entry(phone, "in", message, within_seconds=900):
            print(f"[DUPLICADO_HISTORY_INBOUND] {phone} ignorado")
            commit_processed_key(key)
            return jsonify({"ok": True})

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

        if is_opt_out(message):
            print(f"[INTENT_NEGATIVO] telefone={phone}")
            append_to_blocklist(f"55{phone}", "opt_out")
            update_crm_status_for_lead(lead_state, "sem_interesse")
            append_crm_note_for_lead(lead_state, "Lead informou nao ter interesse. Movido para blocklist.")
            commit_processed_key(key)
            return jsonify({"ok": True})

        closing_key = detect_closing_intent(message)
        if closing_key:
            handle_closing_intent(phone, message, closing_key, lead_state)
            commit_processed_key(key)
            return jsonify({"ok": True})

        if detect_referral_intent(message):
            if handle_referral_redirect(phone, message, lead_state):
                commit_processed_key(key)
                return jsonify({"ok": True})
            referral_reply = pitch.get_closing_message("referral") or "Perfeito, obrigada. Pode me passar o melhor contato da pessoa responsavel?"
            history = append_history(phone, "in", message, step=0)
            if not has_recent_history_entry(phone, "out", referral_reply, within_seconds=1800):
                if not can_emit_reply(phone, referral_reply, within_seconds=REPLY_WINDOW_SECONDS):
                    print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
                    commit_processed_key(key)
                    return jsonify({"ok": True})
                ok = whatsapp.send_message(phone, referral_reply)
                if ok:
                    append_history(phone, "out", referral_reply, step=infer_step_from_history(history))
                    print(f"[RESPOSTA_ENVIADA] {phone}")
                else:
                    release_reply_guard(phone)
                    print(f"[FALHA_ENVIO] {phone}")
                    return jsonify({"ok": False, "confirmed": False})
            commit_processed_key(key)
            return jsonify({"ok": True})

        if detect_positive_intent(message):
            print(f"[INTENT_POSITIVO] telefone={phone}")
            if detect_scheduling_intent(message):
                move_related_deals_to_scheduled(lead_state)
        elif detect_neutral_intent(message):
            print(f"[INTENT_NEUTRO] telefone={phone}")
        elif detect_scheduling_intent(message):
            print(f"[INTENT_AGENDAMENTO] telefone={phone}")
            move_related_deals_to_scheduled(lead_state)

        if source == "after_hours_resume" and has_recent_history_entry(phone, "in", message, within_seconds=48 * 3600):
            history = get_history_items(phone)
        else:
            history = append_history(phone, "in", message, step=0)
        current_step = max(infer_step_from_history(history), infer_step_from_deals(deals))
        inbound_messages = [
            item.get("message", "")
            for item in history
            if str(item.get("direction") or "").strip().lower() == "in"
        ]

        reply = pitch.build_reply(
            lead={"telefone": phone, "nome": person.get("name") or ""},
            inbound_messages=inbound_messages,
            current_step=current_step,
        )
        print(f"[INTENT_DETECTADA] telefone={phone} step={current_step}")
        print(f"[RESPOSTA_GERADA] telefone={phone} texto={reply}")

        if has_recent_history_entry(phone, "out", reply, within_seconds=1800):
            print(f"[DUPLICADO_HISTORY_OUTBOUND] {phone} ignorado")
            commit_processed_key(key)
            return jsonify({"ok": True})
        if not can_emit_reply(phone, reply, within_seconds=REPLY_WINDOW_SECONDS):
            print(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {phone}")
            commit_processed_key(key)
            return jsonify({"ok": True})

        if not whatsapp.healthcheck():
            print("[AVISO] API pode estar offline, mas tentando enviar mesmo assim...")

        ok = whatsapp.send_message(phone, reply)
        if ok:
            append_history(phone, "out", reply, step=current_step)
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
    app.run(port=5000, threaded=True, use_reloader=False)
