import atexit
import json
import logging
import os
import random
import re
import signal
import socket
import subprocess
import sys
import time
import threading
import unicodedata
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(ROOT_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

try:
    from logic.whatsapp_pitch_engine import WhatsAppPitchEngine
except ImportError:
    # Fallback para ambientes onde a estrutura de pastas difere
    sys.path.append(os.path.join(ROOT_DIR, 'logic'))
    try:
        from whatsapp_pitch_engine import WhatsAppPitchEngine
    except ImportError:
        WhatsAppPitchEngine = None

def _pip_install(*packages):
    cmd = [sys.executable, "-m", "pip", "install"]
    if os.name != "nt":
        cmd.append("--break-system-packages")
    cmd.extend(packages)
    subprocess.check_call(cmd)


try:
    import flask  # type: ignore
except ImportError:
    print("[AUTO_INSTALL] Instalando Flask...")
    _pip_install("flask")
    print("[AUTO_INSTALL_OK]")

try:
    import fastapi  # type: ignore  # noqa: F401
    import uvicorn  # type: ignore  # noqa: F401
except ImportError:
    print("[AUTO_INSTALL] Instalando FastAPI/Uvicorn...")
    _pip_install("fastapi", "uvicorn")
    print("[AUTO_INSTALL_OK]")

from crm.pipedrive_client import PipedriveClient
from logic.whatsapp_pitch_engine import WhatsAppPitchEngine
from services.whatsapp_service import WhatsAppService


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
BLOCKLIST_FILE = str(LOGS_DIR / "whatsapp_manual_blocklist.json")
HISTORY_FILE = str(LOGS_DIR / "whatsapp_message_history.json")
LOCK_FILE = str(BASE_DIR / "sdr_process.lock")
SEND_PROGRESS_FILE = str(DATA_DIR / "send_progress.json")
INBOUND_RECOVERY_FILE = str(DATA_DIR / "inbound_recovery_state.json")
ACTIVITY_DEAL_STATE_FILE = str(DATA_DIR / "activity_deal_state.json")
MAX_BATCH_SIZE = 10
MAX_DEAL_AGE_DAYS = 30
PIPELINE_ID = 2
DAILY_SUCCESS_LIMIT = 50
MAX_DEALS_DIA = 50
CADENCE_MAX_STEP = 6
CADENCE_GAP_DAYS = 7
ARCHIVE_AFTER_CADENCE_STEP = 6
EMAIL_DELAY_MIN_SEC = 60
EMAIL_DELAY_MAX_SEC = 120
ENABLE_EMAIL_CADENCE = True
ENRICHMENT_MODE = "off"
MAX_ENRICH_PER_CYCLE = 5
LIGHT_ENRICH_FETCH_LIMIT = 25
LIGHT_ENRICH_TTL_SEC = 24 * 60 * 60
LIGHT_ENRICH_INTERVAL_MIN_SEC = 60
LIGHT_ENRICH_INTERVAL_MAX_SEC = 120
BOT_PRIORITY = True
SUPER_MINAS_REENTRY = True
ENRICHMENT_BATCH_SIZE = 20
ENRICHMENT_AFTER_MESSAGES = 5
SUPER_MINAS_LABEL_NAMES = {"SUPER_MINAS", "SUPER MINAS", "SUPERMINAS", "SUPER_MINAS_OPORTUNIDADE"}
OUTBOUND_BLOCKED_LABEL_IDS = {"173", "193", "166"}
OUTBOUND_BLOCKED_LABEL_NAMES = {
    "INDICACAO_CAROL_EVENTO", "INDICAÇÃO_CAROL_EVENTO", "LEAD_TRÁFEGO", "LEAD_TRAFEGO",
    "CONVERSANDO", "CONVERSATION", "INDICACAO", "INDICAÇÃO", "CAROL_EVENTO", "CAROL EVENTO",
    "STOP", "SEM_INTERESSE", "SEM INTERESSE", "BLOQUEADO", "BLOCKED"
}
BOOT_CHILDREN = []
CRM_IDLE_POLL_SEC = 60
CRM_RATE_LIMIT_COOLDOWN_SEC = 120
SUPERVISOR_LOOP_SLEEP_SEC = 30
N8N_EMAIL_WEBHOOK_PATH = "sdr-email"
N8N_EMAIL_WEBHOOK_URL = f"http://127.0.0.1:5678/webhook/{N8N_EMAIL_WEBHOOK_PATH}"
N8N_HEALTH_URL = "http://127.0.0.1:5678"
N8N_START_BAT = r"C:\Users\Asus\start_n8n.bat"
N8N_START_CMD = str(os.getenv("N8N_START_CMD", "")).strip()
VPS_HOST = "127.0.0.1"
VPS_PORT = 8001
VPS_BASE_URL = str(os.getenv("VPS_URL", "http://127.0.0.1:8001")).rstrip("/")
VPS_HEALTH_URL = f"{VPS_BASE_URL}/"
VPS_FILA_URL = f"{VPS_BASE_URL}/fila"
VPS_ACK_URL = f"{VPS_BASE_URL}/ack"
VPS_PENDING_URL = f"{VPS_BASE_URL}/pending"
VPS_HTTP_TIMEOUT_SEC = 40
WHATSAPP_SEND_GAP_MIN_SEC = 20
WHATSAPP_SEND_GAP_MAX_SEC = 40
WHATSAPP_OUTBOUND_MODE = str(os.getenv("WHATSAPP_OUTBOUND_MODE", "manual")).strip().lower()
ENVIAR_WHATSAPP_AUTOMATICO = WHATSAPP_OUTBOUND_MODE in {"automatico", "automatic", "auto"}
WHATSAPP_LISTENER_ATIVO = True
FAST_QUEUE_BUILD = True
STACK_RECOVERY_COOLDOWN_SEC = 5
STACK_RECOVERY_MAX_ATTEMPTS = 3
WATCHDOG_STALL_SECONDS = 60
BOOT_SERVICE_TIMEOUT_SEC = 10
BOOT_IDLE_SLEEP_SEC = 3
BOOT_BUSY_SLEEP_SEC = 1
BOOT_FAST_WINDOW_SEC = 60
FAST_START_DEAL_FETCH_LIMIT = 1000
RUNTIME_DEAL_FETCH_LIMIT = 1000
INBOUND_RECOVERY_LOOKBACK_HOURS = 24
INBOUND_RECOVERY_MAX_PER_CYCLE = 10
FORA_HORARIO_STATUS = "aguardando_horario"
TAGS_SDR_FIELD = "tags_sdr"
try:
    BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")
except ZoneInfoNotFoundError:
    BRASILIA_TZ = None
TEST_WHITELIST = {"5535920002020", "35920002020", "5511998804191", "11998804191"}


def _is_local_vps_url():
    try:
        parsed = urlparse(VPS_BASE_URL)
        host = str(parsed.hostname or "").strip().lower()
    except Exception:
        host = ""
    return host in {"127.0.0.1", "localhost"}


def check_lock():
    if os.path.exists(LOCK_FILE):
        try:
            lock_pid = 0
            try:
                with open(LOCK_FILE, "r", encoding="utf-8") as existing_lock:
                    lock_pid = int((existing_lock.read() or "0").strip() or "0")
            except Exception:
                lock_pid = 0
            file_age = time.time() - os.path.getmtime(LOCK_FILE)
            if lock_pid and is_pid_running(lock_pid):
                print(f"[ERRO] Outro loop SDR ativo. pid={lock_pid}. Abortando.")
                sys.exit(0)
            if file_age < 600:
                print(f"[AVISO] Lock recente sem processo ativo detectado. pid={lock_pid or 0}. Sobrescrevendo...")
            else:
                print("[AVISO] Lock antigo detectado (>10min). Sobrescrevendo...")
        except Exception:
            pass
    with open(LOCK_FILE, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))


def release_lock():
    if os.path.exists(LOCK_FILE):
        try:
            os.remove(LOCK_FILE)
        except Exception:
            pass


def _today_str():
    return datetime.now(BRASILIA_TZ).strftime("%Y-%m-%d") if BRASILIA_TZ else datetime.now().strftime("%Y-%m-%d")


def is_within_business_hours(phone=None):
    """
    Valida se o bot pode responder automaticamente.
    Whitelist (testadores/admin) funciona 24h.
    Horario padrao: Segunda a Sexta, 09:00 as 18:00 (Brasilia).
    """
    normalized = re.sub(r"\D+", "", str(phone or ""))
    # Whitelist ignora restrição de horário
    if normalized in TEST_WHITELIST or any(v in TEST_WHITELIST for v in [normalized, "55"+normalized]):
        return True
    
    agora = datetime.now(BRASILIA_TZ) if BRASILIA_TZ else datetime.now()
    # Segunda (0) a Sexta (4)
    if agora.weekday() <= 4:
        return 9 <= agora.hour < 18
    return False


def is_allowed_to_send_outbound(phone):
    """
    Define se um envio ATIVO (cadencia) pode ser feito agora.
    Regra: Whitelist sempre, outros apenas no horario comercial.
    """
    normalized = re.sub(r"\D+", "", str(phone or ""))
    is_whitelist = normalized in TEST_WHITELIST or any(v in TEST_WHITELIST for v in [normalized, "55"+normalized])
    
    if is_whitelist:
        print(f"[WHITELIST_24H] outbound permitido: {phone}")
        return True
        
    if is_within_business_hours(phone):
        print(f"[OUTBOUND_PERMITIDO] horario comercial: {phone}")
        return True
        
    print(f"[FORA_HORARIO_BLOCK_OUTBOUND] {phone}")
    return False


def should_reply(phone, is_inbound=True):
    """
    Define se o bot deve responder a uma mensagem recebida.
    Regra: Inbound responde SEMPRE (24h). Whitelist responde SEMPRE.
    """
    normalized = re.sub(r"\D+", "", str(phone or ""))
    is_whitelist = normalized in TEST_WHITELIST or any(v in TEST_WHITELIST for v in [normalized, "55"+normalized])
    
    if is_whitelist:
        print(f"[WHITELIST_24H] resposta permitida: {phone}")
        return True
        
    if is_inbound:
        print(f"[INBOUND_PRIORIDADE] respondendo lead 24h: {phone}")
        return True
        
    return is_within_business_hours(phone)


def _default_send_progress():
    return {
        "date": _today_str(),
        "enviados_hoje": 0,
        "meta": int(MAX_DEALS_DIA),
        "ultima_execucao": datetime.now().isoformat(),
    }


def _default_activity_deal_state():
    return {
        "date": _today_str(),
        "cleaned": False,
        "geracao_concluida": False,
        "deals_com_atividade": [],
        "ultima_execucao": datetime.now().isoformat(),
    }


def _load_inbound_recovery_state():
    try:
        if os.path.exists(INBOUND_RECOVERY_FILE):
            with open(INBOUND_RECOVERY_FILE, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
                if isinstance(payload, dict):
                    handled = payload.get("handled") or []
                    return {"handled": list(handled) if isinstance(handled, list) else []}
    except Exception:
        pass
    return {"handled": []}


def _save_inbound_recovery_state(payload):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(INBOUND_RECOVERY_FILE, "w", encoding="utf-8") as fh:
            json.dump(payload if isinstance(payload, dict) else {"handled": []}, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass


def append_history(phone, direction, message, step=0):
    os.makedirs(LOGS_DIR, exist_ok=True)
    history = {}
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    history = loaded
        except Exception:
            history = {}
    items = history.get(phone, [])
    items.append({
        "direction": direction,
        "message": str(message or "").strip(),
        "step": int(step or 0),
        "created_at": datetime.now().isoformat(),
    })
    history[phone] = items[-30:]
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f)


def read_history():
    if not os.path.exists(HISTORY_FILE):
        return {}
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def resolver_email_webhook_url():
    n8n_root = Path.home() / ".n8n"
    for candidate in (n8n_root / "database.sqlite-wal", n8n_root / "database.sqlite"):
        try:
            content = candidate.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if N8N_EMAIL_WEBHOOK_PATH in content and "n8n-nodes-base.gmail" in content:
            return N8N_EMAIL_WEBHOOK_URL
    return N8N_EMAIL_WEBHOOK_URL


def http_ok(url, timeout=2):
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def get_whatsapp_status(timeout=5):
    try:
        response = requests.get("http://127.0.0.1:3000/status", timeout=timeout)
        if response.status_code != 200:
            print("[WA_OFFLINE]")
            return {"connected": False, "session_invalid": False, "needs_qr": False, "mode": "offline"}
        payload = response.json() if response.content else {}
        if isinstance(payload, dict):
            return payload
    except Exception:
        print("[WA_OFFLINE]")
    return {"connected": False, "session_invalid": False, "needs_qr": False, "mode": "offline"}


def wait_for_http(url, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if http_ok(url):
            return True
        time.sleep(2)
    return False


def wait_for_port(port, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", int(port)), timeout=2):
                return True
        except Exception:
            time.sleep(1)
    return False


def is_port_in_use(port, host="127.0.0.1"):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    try:
        return sock.connect_ex((str(host), int(port))) == 0
    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def is_pid_running(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def read_pid_lock(lock_name):
    path = Path(lock_name)
    if not path.exists():
        return 0
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def cleanup_children():
    for proc in list(BOOT_CHILDREN):
        try:
            if proc.poll() is None:
                proc.terminate()
        except Exception:
            pass


def handle_exit(_signum=None, _frame=None):
    cleanup_children()
    release_lock()
    raise SystemExit(0)


def spawn_if_needed(name, command, health_url, timeout=90, env=None):
    if http_ok(health_url):
        print(f"[{name}_JA_ATIVO]")
        return
    print(f"[SUBINDO_{name}] {' '.join(command)}")
    try:
        proc = subprocess.Popen(command, cwd=str(BASE_DIR), env=env)
    except Exception as exc:
        if str(name).upper() == "N8N":
            print(f"[N8N_OFFLINE] erro_spawn={exc}")
            return
        raise
    if str(name).upper() != "WHATSAPP":
        BOOT_CHILDREN.append(proc)
    upper_name = str(name).upper()
    lock_file = ""
    if upper_name == "WHATSAPP":
        lock_file = "baileys.wa1.lock"
    if upper_name in {"WHATSAPP"}:
        time.sleep(2)
        if http_ok(health_url):
            print(f"[{name}_OK]")
            return
        if proc.poll() == 0:
            lock_pid = read_pid_lock(lock_file)
            if lock_pid and is_pid_running(lock_pid):
                print(f"[{name}_LOCK_ATIVO_PID] {lock_pid}")
                return
    if not wait_for_http(health_url, timeout=timeout):
        if upper_name in {"WHATSAPP"}:
            print("[WA_OFFLINE]")
            return
        if upper_name == "N8N":
            print("[N8N_OFFLINE]")
            return
        raise RuntimeError(f"{name} nao iniciou")
    print(f"[{name}_OK]")


def resolve_n8n_start_command():
    if os.name == "nt" and os.path.exists(N8N_START_BAT):
        return ["cmd", "/c", N8N_START_BAT]
    if N8N_START_CMD:
        if os.name == "nt":
            return ["cmd", "/c", N8N_START_CMD]
        return ["bash", "-lc", N8N_START_CMD]
    return []


def maybe_start_n8n(timeout=90):
    if not ENABLE_EMAIL_CADENCE:
        print("[EMAIL_DESATIVADO]")
        return
    if not is_within_business_hours():
        print("[N8N_FORA_HORARIO]")
        return
    command = resolve_n8n_start_command()
    if not command:
        print("[N8N_STARTER_AUSENTE]")
        return
    spawn_if_needed("N8N", command, N8N_HEALTH_URL, timeout=timeout)


def start_inbound_with_retry():
    print("[INBOUND_HTTP_DESATIVADO] processamento_direto_no_supervisor")


def start_vps_with_retry():
    if not _is_local_vps_url():
        print(f"[VPS_START] modo=remoto url={VPS_BASE_URL}")
        if wait_for_http(VPS_HEALTH_URL, timeout=30):
            print("[VPS_BIND_OK]")
            print("[VPS_OK]")
        else:
            print("[VPS_ERRO_PORTA] remote_healthcheck_fail")
        return

    if is_port_in_use(VPS_PORT, VPS_HOST):
        print("[VPS_JA_EM_EXECUCAO]")
        if http_ok(VPS_HEALTH_URL):
            print("[VPS_BIND_OK]")
            print("[VPS_OK]")
        else:
            print("[VPS_ERRO_PORTA] porta_em_uso_sem_healthcheck")
        return

    print("[VPS_START]")
    try:
        spawn_if_needed(
            "VPS",
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                VPS_HOST,
                "--port",
                str(VPS_PORT),
            ],
            VPS_HEALTH_URL,
            timeout=30,
        )
    except Exception as exc:
        print(f"[VPS_ERRO_PORTA] erro_spawn={exc}")
        return

    print("[VPS_BIND_OK]")
    if wait_for_http(VPS_HEALTH_URL, timeout=30):
        print("[VPS_OK]")
        return

    print("[VPS_ERRO_PORTA] bind_sem_resposta_http")


def start_stack():
    start_vps_with_retry()
    if WHATSAPP_LISTENER_ATIVO:
        spawn_if_needed("WHATSAPP", ["node", "central_whatsapp.mjs"], "http://127.0.0.1:3000/status", timeout=120)
    else:
        print("[WHATSAPP_LISTENER_DESATIVADO] stack_sem_listener")
    start_inbound_with_retry()
    maybe_start_n8n(timeout=120)


def _boot_worker(name, fn):
    started_at = time.time()
    try:
        fn()
    except Exception as exc:
        elapsed = time.time() - started_at
        if elapsed >= BOOT_SERVICE_TIMEOUT_SEC:
            print(f"[STARTUP_TIMEOUT_SKIP] servico={name} segundos={int(elapsed)}")
            return
        print(f"[STARTUP_TIMEOUT_SKIP] servico={name} erro={exc}")


def start_stack_fast():
    print("[BOOT_START]")
    print("[BOOT_FAST_MODE]")
    if WHATSAPP_LISTENER_ATIVO:
        try:
            wa_status = get_whatsapp_status(timeout=1)
            if bool(wa_status.get("connected")):
                print("[WHATSAPP_JA_ATIVO]")
        except Exception:
            pass
    else:
        print("[WHATSAPP_LISTENER_DESATIVADO] boot_sem_listener")

    workers = [("VPS", start_vps_with_retry), ("INBOUND", start_inbound_with_retry)]
    if WHATSAPP_LISTENER_ATIVO:
        workers.insert(
            1,
            ("WHATSAPP", lambda: spawn_if_needed("WHATSAPP", ["node", "central_whatsapp.mjs"], "http://127.0.0.1:3000/status", timeout=BOOT_SERVICE_TIMEOUT_SEC)),
        )
    if ENABLE_EMAIL_CADENCE:
        workers.append(("N8N", lambda: maybe_start_n8n(timeout=BOOT_SERVICE_TIMEOUT_SEC)))
    else:
        print("[EMAIL_DESATIVADO]")

    for name, fn in workers:
        thread = threading.Thread(target=_boot_worker, args=(name, fn), daemon=True)
        thread.start()


atexit.register(cleanup_children)
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


class SDRSupervisor:
    def __init__(self):
        self.boot_started_at = time.time()
        self.whatsapp = WhatsAppService()
        self.whatsapp_service = self.whatsapp
        self.pitch = WhatsAppPitchEngine(config=None)
        self.crm = PipedriveClient()
        self.blocklist = self.load_blocklist()
        self.deal_label_options = self.crm.get_deal_labels()
        self.logger = logging.getLogger("sdr_supervisor")
        self.pending_email_queue = []
        self.pending_whatsapp_queue = []
        self.next_whatsapp_send_at = 0.0
        self._archived_stage_id = 0
        self._stage_cache = {}
        self._next_stack_recovery_at = 0.0
        self._stack_recovery_attempts = 0
        self._next_n8n_start_attempt_at = 0.0
        self._send_progress_cache = None
        self._last_wa_heartbeat_at = 0.0
        self._inbound_recovery_state = _load_inbound_recovery_state()
        self._last_progress_at = time.time()
        self._activity_deal_state_cache = None
        self._next_crm_fetch_at = 0.0
        self._last_fetch_time = 0.0
        self._crm_cooldown_until = 0.0
        self._crm_backoff = 60
        self._last_cleanup_at = 0.0

    @staticmethod
    def _normalize_label_token(value):
        token = unicodedata.normalize("NFKD", str(value or ""))
        token = "".join(ch for ch in token if not unicodedata.combining(ch))
        token = token.upper().strip()
        token = re.sub(r"\s+", " ", token)
        return token

    def _daily_send_key(self, deal_id, phone):
        normalized_phone = self.whatsapp.normalize_phone(phone)
        normalized_deal = int(deal_id or 0)
        if normalized_deal <= 0 or not normalized_phone:
            return ""
        return f"{_today_str()}:{normalized_deal}:{normalized_phone}"

    def _already_sent_today_for_lead(self, deal_id, phone):
        key = self._daily_send_key(deal_id, phone)
        if not key:
            return False
        fd = None
        try:
            fd = self.whatsapp._acquire_lock()
            data = self.whatsapp._load_sent_data_unlocked()
            today_key = f"DEAL_PHONE_{_today_str()}"
            keys = {str(item).strip() for item in list(data.get(today_key) or []) if str(item).strip()}
            return key in keys
        except Exception:
            return False
        finally:
            if fd is not None:
                self.whatsapp._release_lock(fd)

    def _lock_sent_today_for_lead(self, deal_id, phone):
        key = self._daily_send_key(deal_id, phone)
        if not key:
            return False
        fd = None
        try:
            fd = self.whatsapp._acquire_lock()
            data = self.whatsapp._load_sent_data_unlocked()
            today_key = f"DEAL_PHONE_{_today_str()}"
            keys = [str(item).strip() for item in list(data.get(today_key) or []) if str(item).strip()]
            if key in set(keys):
                return False
            keys.append(key)
            data[today_key] = keys
            self.whatsapp._save_json(self.whatsapp.sent_file, data)
            return True
        except Exception:
            return False
        finally:
            if fd is not None:
                self.whatsapp._release_lock(fd)

    def _recent_inbound_messages(self, phone, limit=10):
        normalized = self.whatsapp.normalize_phone(phone)
        if not normalized:
            return []
        history = read_history()
        items = history.get(normalized, [])
        if not isinstance(items, list):
            return []
        messages = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("direction") or "").strip().lower() != "in":
                continue
            message = str(item.get("message") or "").strip()
            if not message:
                continue
            messages.append(message)
        return messages[-max(1, int(limit or 10)):]

    def _build_inbound_lead(self, phone):
        normalized = self.whatsapp.normalize_phone(phone)
        lead = {
            "id": 0,
            "nome": "",
            "empresa": "a empresa",
            "phone": normalized,
            "phones": [normalized] if normalized else [],
            "telefones": [normalized] if normalized else [],
            "person_id": 0,
            "cadence_step": 1,
        }
        try:
            person = self.crm.find_person(phone=normalized) or {}
        except Exception:
            person = {}
        if not isinstance(person, dict):
            person = {}
        person_id = int(self._extract_entity_id(person.get("id")) or 0)
        org_id = int(self._extract_entity_id(person.get("org_id")) or 0)
        deals = []
        try:
            deals = self.crm.find_open_deals_by_person_or_org(person_id=person_id, org_id=org_id)
        except Exception:
            deals = []
        deal = dict((deals or [None])[0] or {})
        org_name = ""
        org = person.get("org_id") or {}
        if isinstance(org, dict):
            org_name = str(org.get("name") or "").strip()
        lead.update(
            {
                "id": int(deal.get("id") or 0),
                "nome": str(person.get("name") or "").strip(),
                "empresa": self._extract_company_name(deal) if deal else (org_name or "a empresa"),
                "person_id": person_id,
                "cadence_step": max(1, int(deal.get("cadence_step") or 1)) if deal else 1,
            }
        )
        return lead

    @staticmethod
    def _normalize_inbound_intent_text(text):
        normalized = unicodedata.normalize("NFKD", str(text or ""))
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.lower()
        return re.sub(r"\s+", " ", normalized).strip()

    def _classify_inbound_intent(self, text):
        msg = self._normalize_inbound_intent_text(text)
        if not msg:
            return "duvida"

        hostil_tokens = (
            "vai se f",
            "tomar no",
            "caralho",
            "porra",
            "fdp",
            "filho da",
            "otario",
            "ot??rio",
            "idiota",
            "vsf",
        )
        if any(token in msg for token in hostil_tokens):
            return "hostil"

        no_interest_tokens = (
            "nao tenho interesse",
            "n??o tenho interesse",
            "sem interesse",
            "nao quero",
            "n??o quero",
            "nao me chama",
            "n??o me chama",
            "pare",
            "parar",
            "remove meu numero",
            "remova meu numero",
            "nao insist",
            "n??o insist",
        )
        if any(token in msg for token in no_interest_tokens):
            return "sem_interesse"

        referral_tokens = (
            "falar com",
            "procura",
            "procure",
            "responsavel e",
            "respons??vel ??",
            "contato de",
            "numero de",
            "whatsapp de",
            "nao sou eu",
            "n??o sou eu",
            "nao sou o responsavel",
            "n??o sou o respons??vel",
            "numero errado",
            "n??mero errado",
            "nao e aqui",
            "n??o ?? aqui",
        )
        if any(token in msg for token in referral_tokens):
            return "indicacao"

        scheduling_tokens = (
            "vamos agendar",
            "podemos agendar",
            "agendar",
            "agenda",
            "marcar",
            "reuniao",
            "reuni??o",
            "call",
            "amanha",
            "amanh??",
            "hoje a tarde",
            "hoje de tarde",
            "me chama",
            "pode ser",
            "disponivel",
            "dispon??vel",
        )
        if any(token in msg for token in scheduling_tokens):
            return "interesse"

        positive_tokens = (
            "sim",
            "tenho interesse",
            "interesse",
            "quero entender",
            "me explica",
            "pode me explicar",
            "sou eu",
            "pode falar",
            "claro",
            "manda",
            "envia",
            "pode enviar",
        )
        if any(token in msg for token in positive_tokens):
            return "interesse"

        return "duvida"

    def _build_opening_message(self, lead):
        opening = random.choice(self.pitch.WHATSAPP_OPENINGS)
        opening = self.pitch._render_placeholders(opening, lead=lead)
        opening = self.pitch._render_spintax(opening)
        return self.pitch._clean_block(opening)

    def _add_to_blocklist(self, phone, reason="manual"):
        normalized = self.whatsapp.normalize_phone(phone)
        if not normalized:
            return False
        already = normalized in self.blocklist
        self.blocklist.add(normalized)
        if already:
            return True
        try:
            os.makedirs(LOGS_DIR, exist_ok=True)
            payload = []
            if os.path.exists(BLOCKLIST_FILE):
                with open(BLOCKLIST_FILE, "r", encoding="utf-8-sig") as fh:
                    loaded = json.load(fh)
                    if isinstance(loaded, list):
                        payload = loaded
            if not any(self.whatsapp.normalize_phone(item.get("telefone")) == normalized for item in payload if isinstance(item, dict)):
                payload.append(
                    {
                        "telefone": normalized,
                        "reason": str(reason or "manual").strip() or "manual",
                        "tag": "blocked_automatic",
                        "created_at": datetime.now().isoformat(),
                    }
                )
                with open(BLOCKLIST_FILE, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, ensure_ascii=False, indent=2)
            print(f"[BLOCKLIST_ADICIONADO] telefone={normalized} motivo={reason}")
        except Exception as exc:
            print(f"[ERRO_BLOCKLIST] telefone={normalized} erro={exc}")
        return True

    @staticmethod
    def _extract_referral_payload(text):
        raw_text = str(text or "")
        if not raw_text.strip():
            return {}
        email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", raw_text)
        phone_candidates = re.findall(r"(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?(?:9?\d{4})[-\s]?\d{4}", raw_text)
        normalized_phones = []
        for candidate in phone_candidates:
            digits = re.sub(r"\D+", "", str(candidate or ""))
            if digits.startswith("55") and len(digits) > 11:
                digits = digits[2:]
            if len(digits) in {10, 11} and digits not in normalized_phones:
                normalized_phones.append(digits)

        name_match = re.search(
            r"(?:falar com|procura(?:r)?|contato(?: e| eh| e o)?|responsavel(?: e| eh)?)[:\\s-]*([A-Za-z]{2,}(?:\\s+[A-Za-z]{2,}){0,2})",
            raw_text,
            flags=re.IGNORECASE,
        )

        return {
            "name": str(name_match.group(1) if name_match else "").strip(),
            "phone": normalized_phones[0] if normalized_phones else "",
            "email": str(email_match.group(0) if email_match else "").strip().lower(),
        }

    def _register_referral_contact(self, lead, text):
        lead = dict(lead or {})
        payload = self._extract_referral_payload(text)
        referral_phone = self.whatsapp.normalize_phone(payload.get("phone"))
        referral_email = str(payload.get("email") or "").strip().lower()
        referral_name = str(payload.get("name") or "").strip()

        if not referral_phone and not referral_email:
            return False

        deal_id = int(lead.get("id") or 0)
        org_id = 0
        if deal_id > 0:
            try:
                deal = self.crm.get_deal_details(deal_id) or {}
                org_id = int(self._extract_entity_id((deal or {}).get("org_id")) or 0)
            except Exception:
                org_id = 0

        person = {}
        try:
            person = self.crm.find_person(org_id=org_id, phone=referral_phone, email=referral_email) or {}
        except Exception:
            person = {}

        person_id = int(self._extract_entity_id((person or {}).get("id")) or 0)
        created = False
        if person_id <= 0:
            person_payload = {
                "name": referral_name or "Contato indicado",
                "phone": [f"+55{referral_phone}"] if referral_phone else [],
                "email": [referral_email] if referral_email else [],
            }
            if org_id > 0:
                person_payload["org_id"] = org_id
            try:
                person = self.crm.create_person(person_payload) or {}
                person_id = int(self._extract_entity_id((person or {}).get("id")) or 0)
                created = person_id > 0
            except Exception:
                person_id = 0

        if deal_id > 0 and person_id > 0:
            try:
                self.crm.add_deal_participant(deal_id, person_id)
            except Exception:
                pass

        if deal_id > 0:
            note_lines = ["Contato indicado pelo lead."]
            if referral_name:
                note_lines.append(f"Nome: {referral_name}")
            if referral_phone:
                note_lines.append(f"Telefone: {referral_phone}")
            if referral_email:
                note_lines.append(f"Email: {referral_email}")
            if created:
                note_lines.append("Novo contato criado no CRM sem duplicidade.")
            self._append_deal_note(deal_id, "\n".join(note_lines))

        if person_id > 0:
            print(
                f"[CRM_INDICACAO_OK] deal={deal_id} person={person_id} "
                f"created={1 if created else 0}"
            )
            return True
        return False

    def _upsert_relevant_meeting_activity(self, lead, reason="interesse"):
        lead = dict(lead or {})
        deal_id = int(lead.get("id") or 0)
        if deal_id <= 0:
            return False
        subject = f"Reuniao SDR - DEAL {deal_id}"
        if self.crm.has_open_activity_today(deal_id=deal_id, activity_type="meeting", subject=subject):
            return False
        payload = {
            "subject": subject,
            "type": "meeting",
            "deal_id": deal_id,
            "person_id": int(lead.get("person_id") or 0),
            "due_date": self._next_business_activity_date(),
            "due_time": "10:00",
            "note": f"Lead demonstrou {str(reason or 'interesse')} no WhatsApp. Priorizar agendamento.",
            "done": 0,
        }
        ok = bool(self.crm.create_activity(**payload))
        if ok:
            print(f"[CRM_REUNIAO_CRIADA] deal={deal_id}")
        return ok

    def _classify_inbound_intent_llm(self, text):
        message = str(text or "").strip()
        if not message:
            return ""
        model_name = str(os.getenv("GEMINI_MODEL") or "gemini-1.5-flash").strip() or "gemini-1.5-flash"
        try:
            from config.config_loader import get_config_value as _get_config_value
        except Exception:
            def _get_config_value(_key, default=""):
                return default

        keys = []
        for candidate in (
            os.getenv("GEMINI_API_KEY_PRIMARY", ""),
            os.getenv("GEMINI_API_KEY_FALLBACK", ""),
            os.getenv("GEMINI_API_KEY", ""),
            _get_config_value("gemini_api_key_primary", ""),
            _get_config_value("gemini_api_key_fallback", ""),
            _get_config_value("gemini_api_key", ""),
        ):
            token = str(candidate or "").strip()
            if token and token not in keys:
                keys.append(token)
        if not keys:
            return ""

        prompt = (
            "Classifique a mensagem do lead em exatamente um rotulo: "
            "interesse, duvida, sem_interesse, indicacao, hostil. "
            "Responda somente com o rotulo, sem texto extra.\n\n"
            f"Mensagem: {message}"
        )
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0, "maxOutputTokens": 12},
        }

        for api_key in keys:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
                response = requests.post(url, params={"key": api_key}, json=payload, timeout=8)
                if int(response.status_code or 0) >= 400:
                    continue
                body = response.json() if response.content else {}
                text_out = ""
                candidates = list((body or {}).get("candidates") or [])
                if candidates:
                    parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
                    for part in parts:
                        maybe_text = str((part or {}).get("text") or "").strip()
                        if maybe_text:
                            text_out = maybe_text
                            break
                label = self._normalize_inbound_intent_text(text_out)
                if "sem_interesse" in label or "sem interesse" in label:
                    return "sem_interesse"
                if "hostil" in label:
                    return "hostil"
                if "indicacao" in label or "indicacao" in label or "indica" in label:
                    return "indicacao"
                if "interesse" in label:
                    return "interesse"
                if "duvida" in label or "d?vida" in label:
                    return "duvida"
            except Exception:
                continue
        return ""

    def process_inbound_message(self, msg):
        payload = dict(msg or {})
        phone = self.whatsapp.normalize_phone(payload.get("phone"))
        text = str(payload.get("text") or payload.get("message") or "").strip()
        if not phone or not text:
            print(f"[INBOUND_INVALIDO] telefone={phone or 'vazio'} texto={1 if text else 0}")
            return {"ok": False, "confirmed": False}
        
        normalized_phone = re.sub(r"\D+", "", str(phone or ""))
        is_whitelist = normalized_phone in TEST_WHITELIST or any(v in TEST_WHITELIST for v in [normalized_phone, "55"+normalized_phone])
        
        append_history(phone, "in", text, step=0)
        lead = self._build_inbound_lead(phone)
        deal_id = int((lead or {}).get("id") or 0)

        # REGRA CRITICA: SO RESPONDE SE EXISTIR NO CRM (EXCETO WHITELIST)
        if deal_id <= 0 and not is_whitelist:
            print(f"[INBOUND_IGNORE] Lead nao encontrado no CRM e nao eh whitelist: {phone}")
            return {"ok": True, "confirmed": True, "reason": "not_in_crm"}

        # CONTROLE DE RESPOSTA (INBOUND 24H)
        if not should_reply(phone, is_inbound=True):
            return {"ok": True, "confirmed": True, "reason": "blocked_by_logic"}

        inbound_messages = self._recent_inbound_messages(phone)
        has_outbound_history = self._phone_has_outbound_history(phone)
        intent = self._classify_inbound_intent(text)
        if intent == "duvida":
            llm_intent = self._classify_inbound_intent_llm(text)
            if llm_intent:
                intent = llm_intent
        print(f"[INTENT] telefone={phone} intent={intent}")

        reply = ""
        normalized_text = self._normalize_inbound_intent_text(text)
        try:
            if not has_outbound_history:
                reply = self._build_opening_message(lead)
                if phone in TEST_WHITELIST:
                    print(f"[TESTE_LIBERADO] telefone={phone} contexto=inbound_opening")
            elif intent == "interesse":
                if any(token in normalized_text for token in ("agendar", "agenda", "reuniao", "reuni?o", "call", "horario", "hor?rio")):
                    reply = str(self.pitch.get_day_message(10, lead=lead) or "").strip()
                else:
                    reply = str(self.pitch.get_day_message(8, lead=lead) or "").strip()
                self._upsert_relevant_meeting_activity(lead, reason="interesse")
                try:
                    if int((lead or {}).get("id") or 0) > 0:
                        self.crm.add_tag("deal", int((lead or {}).get("id") or 0), "respondido")
                except Exception:
                    pass
            elif intent == "sem_interesse":
                reply = str(self.pitch.get_closing_message("no_interest") or "").strip()
                self._add_to_blocklist(phone, reason="sem_interesse")
            elif intent == "hostil":
                reply = str(self.pitch.get_closing_message("no_interest") or "").strip()
                self._add_to_blocklist(phone, reason="hostil")
            elif intent == "indicacao":
                self._register_referral_contact(lead, text)
                reply = str(self.pitch.get_closing_message("referral") or "").strip()
            else:
                reply = str(
                    self.pitch.build_reply(
                        lead,
                        inbound_messages,
                        current_step=max(1, int((lead or {}).get("cadence_step") or 1)),
                    )
                    or ""
                ).strip()
        except Exception as exc:
            print(f"[RESPOSTA_FALLBACK] telefone={phone} erro={exc}")

        if not reply:
            reply = "Mensagem recebida."

        print(f"[RESPOSTA_GERADA] {phone}")
        try:
            sent = bool(
                self.whatsapp_service.send_message(
                    phone,
                    reply,
                    cadence_step=max(1, int((lead or {}).get("cadence_step") or 1)),
                    deal_id=int((lead or {}).get("id") or 0),
                )
            )
            if sent:
                print(f"[RESPOSTA_ENVIADA] {phone}")
            else:
                print(f"[ERRO_ENVIO] telefone={phone} erro=send_message_false estado={self.whatsapp_service.last_send_state}")
        except Exception as exc:
            print(f"[ERRO_ENVIO] telefone={phone} erro={exc}")
            sent = False

        if not sent:
            return {"ok": False, "confirmed": False}

        append_history(phone, "out", reply, step=max(1, int((lead or {}).get("cadence_step") or 1)))
        if int((lead or {}).get("id") or 0) > 0:
            self._mark_channel_sent("whatsapp", lead, reply)
            if intent in {"sem_interesse", "hostil"}:
                try:
                    self.crm.update_deal(int((lead or {}).get("id") or 0), {self.crm.STATUS_BOT_FIELD: "bloqueado"})
                except Exception:
                    pass
        return {"ok": True, "confirmed": True}

    def _startup_window_active(self):
        return (time.time() - float(self.boot_started_at or 0.0)) < BOOT_FAST_WINDOW_SEC

    def _deal_fetch_limit(self):
        if self._startup_window_active():
            return FAST_START_DEAL_FETCH_LIMIT
        return RUNTIME_DEAL_FETCH_LIMIT

    def load_blocklist(self):
        try:
            if os.path.exists(BLOCKLIST_FILE):
                with open(BLOCKLIST_FILE, "r", encoding="utf-8-sig") as f:
                    data = json.load(f)
                    return {
                        self.whatsapp.normalize_phone(item["telefone"])
                        for item in data
                        if isinstance(item, dict) and item.get("telefone")
                    }
        except Exception:
            pass
        return set()

    def _extract_person_id(self, deal):
        person = deal.get("person_id")
        if isinstance(person, dict):
            return person.get("value") or person.get("id") or 0
        return person or 0

    @staticmethod
    def _extract_entity_id(field):
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

    @staticmethod
    def _embedded_person(deal):
        person = (deal or {}).get("person_id")
        return person if isinstance(person, dict) else {}

    def _is_recent_deal(self, deal):
        now = datetime.now()
        for field_name in ("update_time", "add_time"):
            raw = str(deal.get(field_name) or "").strip()
            if not raw:
                continue
            normalized = raw.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                try:
                    parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone().replace(tzinfo=None)
            if now - parsed <= timedelta(days=MAX_DEAL_AGE_DAYS):
                return True
        return False

    def _deal_tokens(self, deal):
        return self.crm.resolve_label_tokens(deal.get("label"), self.deal_label_options)

    def _raw_label_ids(self, deal):
        labels = deal.get("label")
        if isinstance(labels, list):
            values = labels
        else:
            values = [labels]
        results = set()
        for item in values:
            if isinstance(item, dict):
                for candidate in (item.get("id"), item.get("value"), item.get("label"), item.get("name")):
                    token = str(candidate or "").strip()
                    if token:
                        results.add(token)
            else:
                token = str(item or "").strip()
                if token:
                    if "," in token:
                        results.update(part.strip() for part in token.split(",") if part.strip())
                    else:
                        results.add(token)
        return results

    def _is_super_minas(self, deal):
        tokens = {self._normalize_label_token(token) for token in self._deal_tokens(deal)}
        normalized_super = {self._normalize_label_token(item) for item in SUPER_MINAS_LABEL_NAMES}
        if tokens & normalized_super:
            return True
        if self._raw_label_ids(deal) & SUPER_MINAS_LABEL_IDS:
            return True
        return False

    def _is_outbound_blocked_label(self, deal):
        tokens = {self._normalize_label_token(token) for token in self._deal_tokens(deal)}
        normalized_blocked = {self._normalize_label_token(item) for item in OUTBOUND_BLOCKED_LABEL_NAMES}
        if tokens & normalized_blocked:
            return True
        if self._raw_label_ids(deal) & OUTBOUND_BLOCKED_LABEL_IDS:
            return True
        status_bot = self._normalize_label_token((deal or {}).get(self.crm.STATUS_BOT_FIELD) or "")
        if status_bot in normalized_blocked or status_bot in {"CONVERSANDO", "LINKEDIN", "BLOCKED", "BLOQUEADO"}:
            return True
        return False

    def _daily_success_count(self):
        progress = self._load_send_progress(reset_if_new_day=True)
        return int(progress.get("enviados_hoje") or 0)

    def _sent_today_from_history(self):
        today = _today_str()
        data = self.whatsapp._load_json(self.whatsapp.sent_file, {})
        sent_today = data.get(f"DEAL_{today}", [])
        if not isinstance(sent_today, list):
            return 0
        normalized = set()
        for item in sent_today:
            token = str(item or "").strip()
            if token:
                normalized.add(token)
        return len(normalized)

    def _load_activity_deal_state(self, reset_if_new_day=False, force=False):
        if not force and isinstance(getattr(self, "_activity_deal_state_cache", None), dict):
            cached = dict(self._activity_deal_state_cache)
            if not reset_if_new_day or str(cached.get("date") or "") == _today_str():
                return cached
        payload = _default_activity_deal_state()
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            if os.path.exists(ACTIVITY_DEAL_STATE_FILE):
                with open(ACTIVITY_DEAL_STATE_FILE, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                    if isinstance(loaded, dict):
                        payload.update(loaded)
        except Exception:
            pass
        if reset_if_new_day and str(payload.get("date") or "") != _today_str():
            payload = _default_activity_deal_state()
        payload["date"] = _today_str()
        payload["cleaned"] = bool(payload.get("cleaned"))
        payload["geracao_concluida"] = bool(payload.get("geracao_concluida"))
        payload["deals_com_atividade"] = [int(item) for item in list(payload.get("deals_com_atividade") or []) if int(item or 0) > 0]
        if len(payload["deals_com_atividade"]) >= MAX_DEALS_DIA:
            payload["geracao_concluida"] = True
        payload["ultima_execucao"] = datetime.now().isoformat()
        return self._save_activity_deal_state(payload)

    def _save_activity_deal_state(self, payload):
        state = dict(payload or {})
        state["date"] = _today_str()
        state["cleaned"] = bool(state.get("cleaned"))
        state["geracao_concluida"] = bool(state.get("geracao_concluida"))
        state["deals_com_atividade"] = sorted({int(item) for item in list(state.get("deals_com_atividade") or []) if int(item or 0) > 0})
        if len(state["deals_com_atividade"]) >= MAX_DEALS_DIA:
            state["geracao_concluida"] = True
        state["ultima_execucao"] = datetime.now().isoformat()
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(ACTIVITY_DEAL_STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2)
        self._activity_deal_state_cache = dict(state)
        return dict(state)

    def _activity_generation_locked_today(self):
        state = self._load_activity_deal_state(reset_if_new_day=True)
        return bool(state.get("geracao_concluida"))

    def _deal_has_activity_today(self, deal_id):
        target_id = int(deal_id or 0)
        if target_id <= 0:
            return False
        state = self._load_activity_deal_state(reset_if_new_day=True)
        return target_id in {int(item) for item in list(state.get("deals_com_atividade") or [])}

    def _mark_deal_activity_today(self, deal_id):
        target_id = int(deal_id or 0)
        if target_id <= 0:
            return False
        state = self._load_activity_deal_state(reset_if_new_day=True, force=True)
        deals = set(int(item) for item in list(state.get("deals_com_atividade") or []))
        already = target_id in deals
        deals.add(target_id)
        state["deals_com_atividade"] = sorted(deals)
        if len(state["deals_com_atividade"]) >= MAX_DEALS_DIA:
            state["geracao_concluida"] = True
        self._save_activity_deal_state(state)
        return not already

    def _cleanup_today_call_activities_once(self):
        state = self._load_activity_deal_state(reset_if_new_day=True)
        if bool(state.get("cleaned")):
            return 0
        total = 0
        rate_limited = False
        try:
            total = int(self.crm.cleanup_bot_call_activities_for_date(_today_str()) or 0)
        except Exception:
            total = 0
        rate_limited = int(getattr(self.crm, "last_http_status", 0) or 0) == 429
        if rate_limited:
            print("[CRM_LIMPEZA_ATIVIDADES_ADIADA] motivo=rate_limit")
            return 0
        state["cleaned"] = True
        state["geracao_concluida"] = False
        state["deals_com_atividade"] = []
        self._save_activity_deal_state(state)
        print(f"[CRM_LIMPEZA_ATIVIDADES] total={total}")
        return total

    def _load_send_progress(self, reset_if_new_day=False, force=False):
        if not force and isinstance(getattr(self, "_send_progress_cache", None), dict):
            cached = dict(self._send_progress_cache)
            if not reset_if_new_day or str(cached.get("date") or "") == _today_str():
                return cached
        progress = _default_send_progress()
        progress_file_exists = False
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            progress_file_exists = os.path.exists(SEND_PROGRESS_FILE)
            if progress_file_exists:
                with open(SEND_PROGRESS_FILE, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                    if isinstance(loaded, dict):
                        progress.update(loaded)
        except Exception:
            pass
        if reset_if_new_day and str(progress.get("date") or "") != _today_str():
            progress = _default_send_progress()
        progress["date"] = _today_str()
        progress["meta"] = int(progress.get("meta") or MAX_DEALS_DIA)
        progress["enviados_hoje"] = max(0, int(progress.get("enviados_hoje") or 0))
        if not progress_file_exists:
            progress["enviados_hoje"] = max(
                int(progress.get("enviados_hoje") or 0),
                int(self._sent_today_from_history()),
            )
        progress["ultima_execucao"] = datetime.now().isoformat()
        self._save_send_progress(progress)
        return dict(progress)

    def _save_send_progress(self, progress):
        payload = dict(progress or {})
        payload["date"] = _today_str()
        payload["meta"] = int(payload.get("meta") or MAX_DEALS_DIA)
        payload["enviados_hoje"] = max(0, int(payload.get("enviados_hoje") or 0))
        payload["ultima_execucao"] = datetime.now().isoformat()
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(SEND_PROGRESS_FILE, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        self._send_progress_cache = dict(payload)
        return dict(payload)

    def _increment_send_progress(self):
        progress = self._load_send_progress(reset_if_new_day=True, force=True)
        progress["enviados_hoje"] = max(0, int(progress.get("enviados_hoje") or 0) + 1)
        saved = self._save_send_progress(progress)
        print(f"[CONTADOR] enviados={int(saved['enviados_hoje'])}/{int(saved['meta'])}")
        return int(saved["enviados_hoje"])

    def _extract_phone(self, person):
        phones = self._extract_phones(person)
        return phones[0] if phones else ""

    def _extract_phones(self, person):
        phones = []
        seen = set()
        for item in person.get("phone") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = self.whatsapp.normalize_phone(value)
            if not self.whatsapp.is_valid_phone(normalized):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            phones.append(normalized)
        return phones

    @staticmethod
    def _extract_email(person):
        for item in person.get("email") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = str(value or "").strip().lower()
            if normalized and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", normalized):
                return normalized
        return ""

    @staticmethod
    def _first_name(value):
        raw = str(value or "").strip()
        return raw.split()[0].strip() if raw else ""

    @staticmethod
    def _cadence_step(lead=None):
        try:
            return max(1, min(10, int((lead or {}).get("cadence_step") or 1)))
        except Exception:
            return 1

    @staticmethod
    def _normalize_stage_text(value):
        text = unicodedata.normalize("NFKD", str(value or ""))
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r"[^a-zA-Z0-9]+", " ", text).strip().lower()
        return re.sub(r"\s+", " ", text)

    @staticmethod
    def _next_super_minas_cadence_step(tokens):
        if not SUPER_MINAS_REENTRY:
            return 0
        normalized = {str(token or "").strip().upper() for token in list(tokens or [])}
        if "RESPONDIDO" in normalized:
            return 0
        steps = []
        for token in normalized:
            match = re.fullmatch(r"(?:WHATSAPP_)?CAD([1-9]|10)", token)
            if match:
                steps.append(int(match.group(1)))
        if not steps:
            return 1
        last_step = max(steps)
        if last_step >= CADENCE_MAX_STEP:
            return 0
        return last_step + 1

    def _next_cadence_step(self, tokens):
        normalized = {str(token or "").strip().upper() for token in list(tokens or [])}
        if "RESPONDIDO" in normalized:
            return 0
        steps = []
        for token in normalized:
            match = re.fullmatch(r"(?:WHATSAPP_)?CAD([1-9]|10)", token)
            if match:
                steps.append(int(match.group(1)))
        if not steps:
            return 1
        last_step = max(steps)
        if last_step >= CADENCE_MAX_STEP:
            return 0
        return last_step + 1

    def _can_send_lead_phone(self, phone, *, is_super_minas=False, cadence_step=1):
        if int(cadence_step or 1) > 1:
            return self.whatsapp.can_send_followup(phone)
        return self.whatsapp.can_send(phone)

    def _phone_has_outbound_history(self, phone):
        normalized = self.whatsapp.normalize_phone(phone)
        if not normalized:
            return False
        history = read_history()
        items = history.get(normalized, [])
        if not isinstance(items, list):
            return False
        return any(str(item.get("direction") or "").strip().lower() == "out" for item in items if isinstance(item, dict))

    def _phone_has_inbound_history(self, phone):
        normalized = self.whatsapp.normalize_phone(phone)
        if not normalized:
            return False
        history = read_history()
        items = history.get(normalized, [])
        if not isinstance(items, list):
            return False
        return any(str(item.get("direction") or "").strip().lower() == "in" for item in items if isinstance(item, dict))

    def _last_outbound_at(self, phone):
        normalized = self.whatsapp.normalize_phone(phone)
        if not normalized:
            return None
        history = read_history()
        items = history.get(normalized, [])
        if not isinstance(items, list):
            return None
        for item in reversed(items):
            if not isinstance(item, dict):
                continue
            if str(item.get("direction") or "").strip().lower() != "out":
                continue
            raw = str(item.get("created_at") or "").strip()
            if not raw:
                return datetime.now()
            try:
                return datetime.fromisoformat(raw)
            except Exception:
                return datetime.now()
        return None

    def _lead_phones(self, lead):
        raw_phones = list((lead or {}).get("phones") or (lead or {}).get("telefones") or [])
        if not raw_phones and (lead or {}).get("phone"):
            raw_phones = [(lead or {}).get("phone")]
        phones = []
        for raw in raw_phones:
            normalized = self.whatsapp.normalize_phone(raw)
            if normalized and normalized not in phones:
                phones.append(normalized)
        return phones

    def _ordered_lead_phones_for_cycle(self, lead):
        phones = self._lead_phones(lead)
        if len(phones) <= 1:
            return phones

        def sort_key(phone):
            last_out = self._last_outbound_at(phone)
            return (
                1 if self._phone_has_outbound_history(phone) else 0,
                last_out.timestamp() if last_out else 0.0,
                phone,
            )

        return sorted(phones, key=sort_key)

    def _lead_has_inbound_history(self, lead):
        return any(self._phone_has_inbound_history(phone) for phone in self._lead_phones(lead))

    def _lead_has_outbound_history(self, lead):
        return any(self._phone_has_outbound_history(phone) for phone in self._lead_phones(lead))

    @staticmethod
    def _next_business_due_at(last_out):
        due_at = last_out + timedelta(days=CADENCE_GAP_DAYS)
        while due_at.weekday() >= 5:
            due_at += timedelta(days=1)
        return due_at

    def _last_channel_outbound_at(self, channel, lead):
        history = read_history()
        items = history.get(self._lead_history_key(channel, lead), [])
        if isinstance(items, list):
            for item in reversed(items):
                if not isinstance(item, dict):
                    continue
                if str(item.get("direction") or "").strip().lower() != "out":
                    continue
                raw = str(item.get("created_at") or "").strip()
                if not raw:
                    return datetime.now()
                try:
                    return datetime.fromisoformat(raw)
                except Exception:
                    return datetime.now()
        if str(channel or "").strip().lower() == "whatsapp":
            candidates = [self._last_outbound_at(phone) for phone in self._lead_phones(lead)]
            candidates = [item for item in candidates if item is not None]
            if candidates:
                return max(candidates)
        return None

    def _cadence_due(self, lead, cadence_step, channel="whatsapp"):
        step = int(cadence_step or 1)
        if step <= 1:
            return True
        last_out = self._last_channel_outbound_at(channel, lead)
        if last_out is None:
            return False
        return datetime.now() >= self._next_business_due_at(last_out)

    def _whatsapp_tag(self, lead=None):
        return f"WHATSAPP_CAD{self._cadence_step(lead)}"

    def _email_tag(self, lead=None):
        return f"EMAIL_CAD{self._cadence_step(lead)}"

    def _append_deal_note(self, deal_id, content):
        if not int(deal_id or 0) or not str(content or "").strip():
            return False
        ok = False
        try:
            ok = bool(self.crm.add_note(deal_id=int(deal_id), content=str(content).strip()))
        except Exception:
            ok = False
        if ok:
            print(f"[NOTA_CRM] deal={int(deal_id)}")
        return ok

    @staticmethod
    def _today_local_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _next_business_activity_date():
        due_at = datetime.now()
        if due_at.weekday() == 4:
            due_at += timedelta(days=3)
        elif due_at.weekday() == 5:
            due_at += timedelta(days=2)
        else:
            due_at += timedelta(days=1)
        while due_at.weekday() >= 5:
            due_at += timedelta(days=1)
        return due_at.strftime("%Y-%m-%d")

    @staticmethod
    def _extract_deal_id_from_subject(subject):
        match = re.search(r"\[DEAL\s+(\d+)\]", str(subject or ""), re.IGNORECASE)
        return int(match.group(1)) if match else 0

    def _build_call_activity_note(self, lead):
        lead = lead or {}
        phones = self._lead_phones(lead)
        if not phones:
            return ""
        best_phone = phones[0]
        manual_message = self.pitch.get_cadence_message(
            int(lead.get("cadence_step") or 1),
            lead={
                "nome": self._first_name((lead or {}).get("nome")) or "contato",
                "empresa": self.pitch._resolve_short_company_name(lead) or self._extract_company_name(lead),
                "telefone": best_phone,
            },
        )
        note_lines = [
            f"Melhor numero: {best_phone}",
            "",
            "WhatsApp manual:",
            self._build_whatsapp_manual_link(best_phone, manual_message),
            "",
            "Telefones priorizados:",
        ]
        for index, phone in enumerate(phones, start=1):
            marker = " (melhor)" if phone == best_phone else ""
            note_lines.append(f"{index}. {phone}{marker}")
            note_lines.append(f"   Link: {self._build_whatsapp_manual_link(phone, manual_message)}")
        return "\n".join(note_lines).strip()

    def _build_activity_note_from_existing(self, subject="", note="", cadence_step=1):
        existing_note = str(note or "").strip()
        phones = []
        for line in existing_note.splitlines():
            clean = str(line or "").strip()
            if not clean:
                continue
            if clean.lower().startswith("link:") or "wa.me/" in clean.lower():
                continue
            if ":" in clean and clean.lower().startswith("melhor numero"):
                maybe_phone = clean.split(":", 1)[1].strip()
                normalized = self.whatsapp.normalize_phone(maybe_phone)
                if self.whatsapp.is_valid_phone(normalized) and normalized not in phones:
                    phones.append(normalized)
                continue
            match = re.match(r"^\d+\.\s+([0-9\-\+\(\)\s]+)", clean)
            if match:
                normalized = self.whatsapp.normalize_phone(match.group(1))
                if self.whatsapp.is_valid_phone(normalized) and normalized not in phones:
                    phones.append(normalized)
        if not phones:
            return existing_note
        company_name = str(subject or "").strip()
        company_name = re.sub(r"^\s*Ligar\s*-\s*", "", company_name, flags=re.IGNORECASE)
        company_name = re.sub(r"\s*\[DEAL\s+\d+\]\s*$", "", company_name, flags=re.IGNORECASE).strip()
        lead = {
            "nome": "contato",
            "empresa": company_name or "a empresa",
            "phone": phones[0],
            "phones": phones,
            "telefones": phones,
            "cadence_step": int(cadence_step or 1),
        }
        return self._build_call_activity_note(lead)

    def _build_activity_lead_from_deal(self, deal):
        deal = deal or {}
        person_id = self._extract_person_id(deal)
        people = self._deal_people(deal, person_id, self._embedded_person(deal))
        deal_phones = []
        primary_name = ""
        primary_email = ""
        primary_person_id = 0
        for person in people:
            person_phones = self._extract_phones(person)
            if not person_phones:
                continue
            if not primary_name:
                primary_name = person.get("name") or ""
                primary_email = self._extract_email(person)
                primary_person_id = int(self._extract_entity_id(person.get("id")) or person_id or 0)
            elif not primary_email:
                primary_email = self._extract_email(person)
            for normalized in person_phones:
                if normalized not in deal_phones:
                    deal_phones.append(normalized)
        if not deal_phones:
            return {}
        return {
            "id": int(deal.get("id") or 0),
            "nome": primary_name,
            "empresa": self._extract_company_name(deal),
            "phone": deal_phones[0],
            "phones": deal_phones,
            "telefones": deal_phones,
            "email": primary_email,
            "person_id": primary_person_id,
            "cadence_step": 1,
        }

    def _create_call_activity_for_lead(self, lead):
        lead = lead or {}
        deal_id = int(lead.get("id") or 0)
        if deal_id <= 0:
            return False
        if self._activity_generation_locked_today() and not self._deal_has_activity_today(deal_id):
            print(f"[CRM_ATIVIDADE_IGNORADA_DUPLICADA] deal={deal_id}")
            return False
        if self._deal_has_activity_today(deal_id):
            print(f"[CRM_ATIVIDADE_IGNORADA_DUPLICADA] deal={deal_id}")
            return False
        phones = self._lead_phones(lead)
        if not phones:
            return False
        company_name = self.pitch._resolve_short_company_name(lead) or self._extract_company_name(lead)
        subject = f"Ligar - {company_name} [DEAL {deal_id}]".strip()
        if self.crm.has_open_activity_today(deal_id=deal_id, activity_type="call", subject=subject):
            self._mark_deal_activity_today(deal_id)
            print(f"[CRM_ATIVIDADE_IGNORADA_DUPLICADA] deal={deal_id}")
            return False
        best_phone = phones[0]
        payload = {
            "subject": subject,
            "type": "call",
            "deal_id": deal_id,
            "person_id": int(lead.get("person_id") or 0),
            "due_date": self._today_local_date(),
            "due_time": "17:30",
            "note": self._build_call_activity_note(lead),
            "done": 0,
        }
        ok = self.crm.create_activity(**payload)
        if ok:
            self._mark_deal_activity_today(deal_id)
            self.processar_tag("call", int(lead.get("person_id") or 0))
            print(f"[CRM_ATIVIDADE_CRIADA] deal={deal_id}")
        return ok

    def reprogram_bot_call_activities(self, target_date=None, new_due_date=None, new_due_time="17:30"):
        date_str = str(target_date or self._today_local_date()).strip() or self._today_local_date()
        due_date = str(new_due_date or self._next_business_activity_date()).strip() or self._next_business_activity_date()
        updated = 0
        try:
            activities = self.crm.get_activities(due_date=date_str, limit=500)
        except Exception:
            activities = []
        for activity in list(activities or []):
            if not isinstance(activity, dict):
                continue
            activity_type_value = str(activity.get("type") or "").strip().lower()
            if activity_type_value != "call":
                continue
            subject = str(activity.get("subject") or "").strip()
            if not subject.startswith("Ligar - ") or "[DEAL " not in subject:
                continue
            activity_id = int(self.crm._extract_id(activity.get("id")) or 0)
            if activity_id <= 0:
                continue
            note = str(activity.get("note") or "").strip()
            note = self._build_activity_note_from_existing(subject=subject, note=note, cadence_step=1)
            try:
                ok = bool(self.crm.update_activity(activity_id, due_date=due_date, due_time=str(new_due_time or "17:30"), note=note))
            except Exception:
                ok = False
            if ok:
                updated += 1
        print(f"[CRM_ATIVIDADES_REPROGRAMADAS] total={updated} de={date_str} para={due_date} hora={new_due_time}")
        return updated

    def processar_tag(self, activity_type, person_id):
        activity_key = str(activity_type or "").strip().lower()
        target_person_id = int(person_id or 0)
        if target_person_id <= 0:
            return False
        if activity_key == "whatsapp":
            nova_tag = "whatsapp_enviado"
        elif activity_key == "call":
            nova_tag = "ligado"
        elif activity_key == "email":
            nova_tag = "email_enviado"
        else:
            return False
        tag_writer = getattr(self.crm, "add_person_text_tag_incremental", None)
        if not callable(tag_writer):
            print(f"[TAG_SDR_SKIP] person={target_person_id} tag={nova_tag} motivo=metodo_ausente")
            return False
        ok = bool(tag_writer(target_person_id, TAGS_SDR_FIELD, nova_tag))
        if ok:
            print(f"[TAG_SDR_ATUALIZADA] person={target_person_id} tag={nova_tag}")
        return ok

    def _build_whatsapp_manual_link(self, phone, message):
        normalized = self.whatsapp.normalize_phone(phone)
        encoded_message = urllib.parse.quote(str(message or "").strip(), safe="")
        return f"https://wa.me/55{normalized}?text={encoded_message}"

    def _resolve_archived_stage_id(self):
        if int(self._archived_stage_id or 0) > 0:
            return int(self._archived_stage_id)
        try:
            stages = self.crm.get_stages()
        except Exception:
            stages = []
        for stage in list(stages or []):
            if not isinstance(stage, dict):
                continue
            try:
                pipeline_id = int(stage.get("pipeline_id") or 0)
            except Exception:
                pipeline_id = 0
            stage_name = str(stage.get("name") or "").strip().lower()
            if pipeline_id == int(PIPELINE_ID) and ("arquiv" in stage_name or "falta" in stage_name):
                self._archived_stage_id = int(stage.get("id") or 0)
                break
        return int(self._archived_stage_id or 0)

    def _archive_no_contact(self, deal_id):
        archived_stage_id = self._resolve_archived_stage_id()
        if not archived_stage_id:
            print(f"[ARQUIVO_FALHA] deal={int(deal_id or 0)} motivo=stage_nao_encontrado")
            return False
        try:
            ok = bool(self.crm.update_stage(deal_id=int(deal_id), stage_id=archived_stage_id))
        except Exception as exc:
            print(f"[ARQUIVO_FALHA] deal={int(deal_id or 0)} erro={exc}")
            return False
        if ok:
            print(f"[ARQUIVADO_FALTA_CONTATO] deal={int(deal_id)} stage={archived_stage_id}")
            self._append_deal_note(deal_id, "Tentativas de contato esgotadas.")
        return ok

    def _resolve_stage_by_keywords(self, *keyword_groups):
        cache_key = "|".join(",".join(group) for group in keyword_groups)
        if cache_key in self._stage_cache:
            return int(self._stage_cache.get(cache_key) or 0)
        try:
            stages = self.crm.get_stages()
        except Exception:
            stages = []
        for stage in list(stages or []):
            if not isinstance(stage, dict):
                continue
            try:
                pipeline_id = int(stage.get("pipeline_id") or 0)
            except Exception:
                pipeline_id = 0
            if pipeline_id != int(PIPELINE_ID):
                continue
            stage_name = self._normalize_stage_text(stage.get("name"))
            for keywords in keyword_groups:
                if all(str(keyword or "").strip().lower() in stage_name for keyword in keywords):
                    stage_id = int(stage.get("id") or 0)
                    self._stage_cache[cache_key] = stage_id
                    return stage_id
        self._stage_cache[cache_key] = 0
        return 0

    def _move_deal_to_cadence_stage(self, deal_id, cadence_step):
        step = int(cadence_step or 0)
        if step <= 0:
            return False
        if step <= 2:
            stage_id = self._resolve_stage_by_keywords(("contato 1 e 2",), ("contato", "1", "2"))
        elif step <= 4:
            stage_id = self._resolve_stage_by_keywords(("contato 3 e 4",), ("contato", "3", "4"))
        else:
            stage_id = self._resolve_stage_by_keywords(("contato 5 e 6",), ("contato", "5", "6"))
        if not stage_id:
            print(f"[STAGE_CAD_FALHA] deal={int(deal_id or 0)} cadencia={step} motivo=stage_nao_encontrado")
            return False
        try:
            ok = bool(self.crm.update_stage(deal_id=int(deal_id), stage_id=stage_id))
        except Exception as exc:
            print(f"[STAGE_CAD_FALHA] deal={int(deal_id or 0)} cadencia={step} erro={exc}")
            return False
        if ok:
            print(f"[STAGE_CAD_OK] deal={int(deal_id)} cadencia={step} stage={stage_id}")
        return ok

    def _deal_people(self, deal, primary_person_id, embedded_person):
        people = []
        seen = set()

        def add_person(person):
            if not isinstance(person, dict):
                return
            person_id = self._extract_entity_id(person.get("id") or person.get("value"))
            key = person_id or str(person.get("name") or "") + "|" + str(person.get("phone") or "")
            if not key or key in seen:
                return
            seen.add(key)
            people.append(dict(person))

        if embedded_person:
            add_person(embedded_person)
        if int(primary_person_id or 0) and int(primary_person_id or 0) not in seen:
            try:
                add_person(self.crm.get_person(int(primary_person_id)))
            except Exception as exc:
                print(f"[SKIP_PERSON_FETCH] deal={int((deal or {}).get('id') or 0)} person={int(primary_person_id or 0)} erro={exc}")

        if FAST_QUEUE_BUILD and people:
            return people

        deal_id = int((deal or {}).get("id") or 0)
        if deal_id:
            try:
                for item in self.crm.get_deal_persons(deal_id, limit=100):
                    add_person(item)
            except Exception as exc:
                print(f"[SKIP_DEAL_PERSONS] deal={deal_id} erro={exc}")
            try:
                for participant in self.crm.get_deal_participants(deal_id, limit=100):
                    participant_person_id = self._extract_entity_id(participant.get("person_id"))
                    if participant_person_id and participant_person_id not in seen:
                        try:
                            add_person(self.crm.get_person(participant_person_id))
                        except Exception as exc:
                            print(f"[SKIP_PARTICIPANT_FETCH] deal={deal_id} person={participant_person_id} erro={exc}")
            except Exception as exc:
                print(f"[SKIP_PARTICIPANTS] deal={deal_id} erro={exc}")
        return people

    def _lead_history_key(self, channel, lead):
        deal_id = int((lead or {}).get("id") or 0)
        return f"{channel}:deal={deal_id}"

    def _phone_has_send_record(self, phone):
        try:
            return bool(self.whatsapp.has_any_send_record(phone))
        except Exception:
            return False

    def _recent_unanswered_inbound_candidates(self):
        history = read_history()
        if not isinstance(history, dict):
            return []
        handled = set(str(item or "") for item in (self._inbound_recovery_state.get("handled") or []))
        now = datetime.now()
        candidates = []
        for phone, items in history.items():
            normalized_phone = self.whatsapp.normalize_phone(phone)
            if not normalized_phone or str(phone).startswith("whatsapp:") or str(phone).startswith("email:"):
                continue
            if not isinstance(items, list) or not items:
                continue
            last_inbound = None
            last_inbound_message = ""
            for item in reversed(items):
                if not isinstance(item, dict):
                    continue
                if str(item.get("direction") or "").strip().lower() != "in":
                    continue
                raw = str(item.get("created_at") or "").strip()
                try:
                    inbound_at = datetime.fromisoformat(raw) if raw else now
                except Exception:
                    inbound_at = now
                if (now - inbound_at).total_seconds() > INBOUND_RECOVERY_LOOKBACK_HOURS * 3600:
                    break
                last_inbound = inbound_at
                last_inbound_message = str(item.get("message") or "").strip()
                break
            if last_inbound is None or not last_inbound_message or last_inbound_message.upper() == "[MENSAGEM_SEM_TEXTO]":
                continue
            recovery_key = f"{normalized_phone}|{last_inbound.isoformat()}|{last_inbound_message}"
            if recovery_key in handled:
                continue
            has_outbound_after = False
            for item in reversed(items):
                if not isinstance(item, dict):
                    continue
                raw = str(item.get("created_at") or "").strip()
                try:
                    created_at = datetime.fromisoformat(raw) if raw else now
                except Exception:
                    created_at = now
                if created_at < last_inbound:
                    break
                if str(item.get("direction") or "").strip().lower() == "out":
                    has_outbound_after = True
                    break
            if has_outbound_after:
                continue
            candidates.append({
                "phone": normalized_phone,
                "message": last_inbound_message,
                "created_at": last_inbound,
                "recovery_key": recovery_key,
            })
        candidates.sort(key=lambda item: item["created_at"])
        return candidates[:INBOUND_RECOVERY_MAX_PER_CYCLE]

    def _recover_pending_inbounds(self):
        candidates = self._recent_unanswered_inbound_candidates()
        if not candidates:
            return 0
        recovered = 0
        handled = list(self._inbound_recovery_state.get("handled") or [])
        for candidate in candidates:
            phone = str(candidate.get("phone") or "").strip()
            message = str(candidate.get("message") or "").strip()
            recovery_key = str(candidate.get("recovery_key") or "").strip()
            if not phone or not message or not recovery_key:
                continue
            payload = {
                "phone": phone,
                "text": message,
                "messageId": f"recovery:{abs(hash(recovery_key))}",
                "source": "recovery",
            }
            try:
                result = self.process_inbound_message(payload)
                if not (isinstance(result, dict) and result.get("ok") is True and result.get("confirmed") is not False):
                    continue
            except Exception:
                continue
            handled.append(recovery_key)
            recovered += 1
            print(f"[INBOUND_PRIORITARIO] telefone={phone}")
        if recovered:
            self._inbound_recovery_state["handled"] = handled[-5000:]
            _save_inbound_recovery_state(self._inbound_recovery_state)
        return recovered

    def _load_vps_pending_queue(self):
        try:
            response = requests.get(VPS_FILA_URL, timeout=VPS_HTTP_TIMEOUT_SEC)
            if int(response.status_code or 0) != 200:
                return []
            payload = response.json() if response.content else {}
            items = payload.get("items") if isinstance(payload, dict) else []
            if not isinstance(items, list):
                return []
            print(f"[FILA_VPS_CARREGADA] total={len(items)}")
            return items
        except Exception as exc:
            print(f"[FILA_VPS_FALHOU] erro={exc}")
            return []

    def _ack_vps_message(self, msg_id):
        try:
            response = requests.post(VPS_ACK_URL, json={"id": str(msg_id or "")}, timeout=VPS_HTTP_TIMEOUT_SEC)
            if int(response.status_code or 0) != 200:
                return False
            payload = response.json() if response.content else {}
            if isinstance(payload, dict) and payload.get("ok") is True:
                print(f"[FILA_VPS_ACK] msg_id={msg_id}")
                return True
        except Exception as exc:
            print(f"[FILA_VPS_ACK_FALHOU] msg_id={msg_id} erro={exc}")
        return False

    def _return_vps_message_to_pending(self, msg_id):
        try:
            response = requests.post(VPS_PENDING_URL, json={"id": str(msg_id or "")}, timeout=VPS_HTTP_TIMEOUT_SEC)
            if int(response.status_code or 0) != 200:
                return False
            payload = response.json() if response.content else {}
            if isinstance(payload, dict) and payload.get("ok") is True:
                print(f"[FILA_VPS_RETORNO_PENDING] msg_id={msg_id}")
                return True
        except Exception as exc:
            print(f"[FILA_VPS_RETORNO_PENDING_FALHOU] msg_id={msg_id} erro={exc}")
        return False

    def _resume_after_hours_pending(self):
        today = _today_str()
        if not hasattr(self.whatsapp, "get_after_hours_resume_date"):
            return 0
        if self.whatsapp.get_after_hours_resume_date() == today:
            return 0
        pending_items = self.whatsapp.list_after_hours_pending()
        if not pending_items:
            self.whatsapp.mark_after_hours_resumed_today(today)
            return 0

        resumed = 0
        for item in pending_items:
            phone = self.whatsapp.normalize_phone(item.get("phone"))
            message = str(item.get("message") or "").strip()
            if not phone or not message:
                continue
            payload = {
                "phone": phone,
                "text": message,
                "messageId": f"after-hours-resume-{today}-{phone}",
                "source": "after_hours_resume",
                "timestamp": item.get("last_message_at") or item.get("updated_at"),
            }
            try:
                body = self.process_inbound_message(payload)
                if not (isinstance(body, dict) and body.get("ok") is True and body.get("confirmed") is not False):
                    continue
            except Exception as exc:
                print(f"[RETOMADA_DIA_UTIL] telefone={phone} erro={exc}")
                continue
            self.whatsapp.clear_after_hours_pending(phone)
            resumed += 1

        remaining = self.whatsapp.list_after_hours_pending()
        if resumed:
            print(f"[RETOMADA_DIA_UTIL] total={resumed}")
        if not remaining:
            self.whatsapp.mark_after_hours_resumed_today(today)
        return resumed

    def _process_vps_pending_inbounds(self):
        items = self._load_vps_pending_queue()
        if not items:
            return 0
        processed = 0
        for item in items:
            msg_id = str(item.get("id") or item.get("msg_id") or "").strip()
            phone = self.whatsapp.normalize_phone(item.get("phone"))
            text = str(item.get("text") or "").strip()
            if not msg_id or not phone or not text:
                continue
            print(f"[FILA_VPS_PROCESSANDO] telefone={phone} msg_id={msg_id}")
            try:
                body = self.process_inbound_message(item)
                if not (isinstance(body, dict) and body.get("ok") is True and body.get("confirmed") is not False):
                    print(f"[FILA_VPS_PENDING_MOTIVO] telefone={phone} msg_id={msg_id} body={body}")
                    self._return_vps_message_to_pending(msg_id)
                    continue
            except Exception as exc:
                print(f"[FILA_VPS_PROCESSAMENTO_FALHOU] msg_id={msg_id} erro={exc}")
                self._return_vps_message_to_pending(msg_id)
                continue
            if self._ack_vps_message(msg_id):
                processed += 1
            else:
                self._return_vps_message_to_pending(msg_id)
        return processed

    def _channel_already_sent(self, channel, lead):
        history = read_history()
        key = self._lead_history_key(channel, lead)
        items = history.get(key, [])
        if not isinstance(items, list):
            # Fallback para busca por telefone se a chave por deal_id falhar (transição)
            phones = self._lead_phones(lead)
            if phones:
                items = history.get(phones[0], [])
        
        if not isinstance(items, list):
            return False
            
        cadence_step = self._cadence_step(lead)
        if cadence_step <= 1:
            return any(str(item.get("direction") or "").strip().lower() == "out" for item in items if isinstance(item, dict))
        return any(
            str(item.get("direction") or "").strip().lower() == "out"
            and int(item.get("step") or 0) == cadence_step
            for item in items
            if isinstance(item, dict)
        )

    def _mark_channel_sent(self, channel, lead, message):
        append_history(self._lead_history_key(channel, lead), "out", message, step=self._cadence_step(lead))

    def _recent_outbound_message_seen(self, phone, message, within_seconds=180):
        normalized = self.whatsapp.normalize_phone(phone)
        target = str(message or "").strip()
        if not normalized or not target:
            return False
        history = read_history()
        items = history.get(normalized, [])
        if not isinstance(items, list):
            return False
        now = datetime.now()
        for item in reversed(items):
            if not isinstance(item, dict):
                continue
            if str(item.get("direction") or "").strip().lower() != "out":
                continue
            if str(item.get("message") or "").strip() != target:
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

    def _send_email(self, lead):
        if not ENABLE_EMAIL_CADENCE:
            deal_id = int((lead or {}).get("id") or 0)
            print(f"[SKIP_EMAIL] deal={deal_id} motivo=email_desativado")
            return False
        email = str((lead or {}).get("email") or "").strip().lower()
        deal_id = int((lead or {}).get("id") or 0)
        if not email:
            print(f"[SKIP_EMAIL] deal={deal_id} motivo=sem_email")
            return False
        if self._channel_already_sent("email", lead):
            print(f"[SKIP_DUPLICADO] canal=email deal={deal_id} email={email}")
            return False
        cadence_step = self._cadence_step(lead)
        payload = {
            "email": email,
            "nome": self._first_name((lead or {}).get("nome")) or "contato",
            "empresa": self.pitch._resolve_short_company_name(lead),
            "cadencia_step": cadence_step,
            "cadencia_tag": self._email_tag(lead),
            "deal_id": deal_id,
            "person_id": int((lead or {}).get("person_id") or 0),
            "origem": "supervisor",
            "canal": "email",
        }
        print(f"[EMAIL_ENVIO_INICIADO] deal={deal_id} email={email} cadencia={cadence_step}")
        try:
            response = requests.post(resolver_email_webhook_url(), json=payload, timeout=30)
            if int(response.status_code or 0) != 200:
                print(f"[EMAIL_ENVIO_FALHOU] deal={deal_id} email={email} motivo=webhook_status status={int(response.status_code or 0)}")
                return False
        except Exception as exc:
            print(f"[EMAIL_ENVIO_FALHOU] deal={deal_id} email={email} motivo=erro_envio erro={exc}")
            return False

        self._mark_channel_sent("email", lead, f"EMAIL_CAD_{cadence_step}")
        print(f"[EMAIL_ENVIADO] deal={deal_id} email={email} cadencia={cadence_step}")
        print(f"[N8N_TRIGGER] deal={deal_id} email={email}")
        print(f"[EMAIL_CAD_{cadence_step}] deal={deal_id} email={email}")
        self.crm.add_tag("deal", deal_id, self._email_tag(lead))
        self.processar_tag("email", int((lead or {}).get("person_id") or 0))
        try:
            self._append_deal_note(deal_id, f"E-mail enviado na cadencia {cadence_step}.")
        except Exception:
            pass
        return True

    def _queue_email_after_whatsapp(self, lead):
        if not ENABLE_EMAIL_CADENCE:
            return
        email = str((lead or {}).get("email") or "").strip().lower()
        deal_id = int((lead or {}).get("id") or 0)
        if not email:
            print(f"[SKIP_EMAIL] deal={deal_id} motivo=sem_email")
            return
        if self._channel_already_sent("email", lead):
            print(f"[SKIP_DUPLICADO] canal=email deal={deal_id} email={email}")
            return
        for item in list(self.pending_email_queue):
            if int(item.get("id") or 0) == deal_id and str(item.get("email") or "").strip().lower() == email:
                print(f"[SKIP_DUPLICADO] canal=email_queue deal={deal_id} email={email}")
                return
        queued = dict(lead or {})
        delay_seconds = random.randint(EMAIL_DELAY_MIN_SEC, EMAIL_DELAY_MAX_SEC)
        queued["due_at"] = time.time() + delay_seconds
        self.pending_email_queue.append(queued)
        print(f"[DELAY_EMAIL] deal={deal_id} email={email} segundos={delay_seconds}")

    def _process_pending_emails(self):
        if not ENABLE_EMAIL_CADENCE:
            self.pending_email_queue = []
            return
        if not self.pending_email_queue:
            return
        now = time.time()
        ready = [item for item in list(self.pending_email_queue) if float(item.get("due_at") or 0) <= now]
        self.pending_email_queue = [item for item in list(self.pending_email_queue) if float(item.get("due_at") or 0) > now]
        print(f"[EMAIL_QUEUE_STATUS] pendentes={len(self.pending_email_queue)} prontos={len(ready)}")
        for lead in ready:
            self._send_email(lead)

    @staticmethod
    def _extract_company_name(deal):
        org = deal.get("org_id")
        if isinstance(org, dict):
            name = str(org.get("name") or "").strip()
            if name:
                return name
        return str(deal.get("org_name") or deal.get("title") or deal.get("name") or "a empresa").strip() or "a empresa"

    def buscar(self):
        leads = []
        super_skip = {"cad": 0, "sem_person": 0, "sem_phone": 0, "bloqueado": 0}
        now = time.time()
        
        # Executa cleanup no maximo uma vez a cada 6 horas para poupar API
        if now - getattr(self, "_last_cleanup_at", 0) > 21600:
            self._last_cleanup_at = now
            print("[CRM_LIMPEZA_ATIVIDADES_INICIO] rodando em background")
            threading.Thread(target=self._cleanup_today_call_activities_once, daemon=True).start()

        last_fetch_time = float(getattr(self, "_last_fetch_time", 0.0) or 0.0)
        fetch_limit = self._deal_fetch_limit()

        # BUSCA DIRETA PELA API PARA EVITAR 401 OU PROBLEMAS NO CLIENT
        api_token = getattr(self.crm, "token", "") or getattr(self.crm, "api_token", "")
        if not api_token:
            from config.config_loader import get_config_value
            api_token = get_config_value("pipedrive_api_token", "") or get_config_value("pipedrive_token", "")

        deals = []
        try:
            url = "https://api.pipedrive.com/v1/deals"
            params = {
                "api_token": api_token,
                "status": "open",
                "limit": fetch_limit,
                "pipeline_id": PIPELINE_ID,
                "sort": "id DESC"
            }
            resp = requests.get(url, params=params, timeout=20)
            self.crm.last_http_status = resp.status_code
            if resp.status_code == 200:
                data = resp.json()
                deals = data.get("data") or []
            elif resp.status_code == 401:
                print(f"[CRM_UNAUTHORIZED] verifique o token: {api_token[:5]}***")
                deals = []
            elif resp.status_code == 429:
                print("[CRM_RATE_LIMIT_DETECTADO] ativando cooldown forte")
                self._crm_cooldown_until = time.time() + float(self._crm_backoff or 60)
                self._crm_backoff = min(int(self._crm_backoff or 60) * 2, 600)
                self._next_crm_fetch_at = max(float(self._next_crm_fetch_at or 0.0), self._crm_cooldown_until)
                print(f"[MODO_ECONOMICO] rate limit no CRM, aguardando {int(self._crm_backoff)}s...")
                time.sleep(int(self._crm_backoff))
                return []
            else:
                print(f"[CRM_ERROR] status={resp.status_code} body={resp.text[:100]}")
                deals = []
        except Exception as e:
            print(f"[CRM_EXCEPTION] erro={e}")
            deals = []

        print("[DEALS_FETCHED]", len(deals))
        self._crm_backoff = 60
        self._crm_cooldown_until = 0.0
        print(f"[BUSCA_DEALS_OK] total={len(deals)}")
        for deal in deals:
            if len(leads) >= MAX_DEALS_DIA:
                print(f"[CAP_DEALS_ATINGIDO] total={MAX_DEALS_DIA}")
                break
            if self._is_outbound_blocked_label(deal):
                continue
            tokens = set(self._deal_tokens(deal))
            is_super_minas = self._is_super_minas(deal)
            cadence_step = self._next_cadence_step(tokens)
            if cadence_step <= 0:
                if is_super_minas:
                    super_skip["cad"] += 1
                continue
            if not is_super_minas and cadence_step == 1 and not self._is_recent_deal(deal):
                continue
            lead_stub = {
                "id": int(deal.get("id") or 0),
                "cadence_step": cadence_step,
            }
            deal_due = self._cadence_due(lead_stub, cadence_step, channel="whatsapp")

            person_id = self._extract_person_id(deal)
            if not person_id:
                if is_super_minas:
                    super_skip["sem_person"] += 1
                continue

            people = self._deal_people(deal, person_id, self._embedded_person(deal))
            if not people:
                if is_super_minas:
                    super_skip["sem_person"] += 1
                continue

            deal_added = 0
            deal_phones = []
            primary_name = ""
            primary_email = ""
            primary_person_id = 0
            deal_validation_checked = 0
            deal_validation_valid = 0
            deal_validation_invalid = 0
            for person in people:
                person_cadence_step = cadence_step
                person_phones = self._extract_phones(person)
                if not person_phones:
                    if is_super_minas:
                        super_skip["sem_phone"] += 1
                    raw_phone = ""
                    phone_list = person.get("phone") or []
                    if phone_list:
                        first = phone_list[0]
                        raw_phone = first.get("value") if isinstance(first, dict) else first
                    if raw_phone:
                        self.whatsapp.mark_invalid(raw_phone, "invalid_phone_outbound")
                    continue
                valid_person_phones = []
                for normalized in person_phones:
                    if self._phone_has_inbound_history(normalized):
                        continue
                    if person_cadence_step > 1 and not deal_due:
                        continue
                    if not self._can_send_lead_phone(
                        normalized,
                        is_super_minas=is_super_minas,
                        cadence_step=person_cadence_step,
                    ):
                        continue
                    deal_validation_checked += 1
                    if not self.whatsapp.validate_whatsapp_cached(normalized):
                        deal_validation_invalid += 1
                        print(
                            f"[WHATSAPP_PREVALIDACAO_INVALIDA] deal={int(deal.get('id') or 0)} "
                            f"telefone={normalized}"
                        )
                        continue
                    deal_validation_valid += 1
                    valid_person_phones.append(normalized)
                if not valid_person_phones:
                    if is_super_minas:
                        super_skip["bloqueado"] += 1
                    continue
                if not primary_name:
                    primary_name = person.get("name") or ""
                    primary_email = self._extract_email(person)
                    primary_person_id = int(self._extract_entity_id(person.get("id")) or person_id or 0)
                elif not primary_email:
                    primary_email = self._extract_email(person)
                for normalized in valid_person_phones:
                    if normalized not in deal_phones:
                        deal_phones.append(normalized)
                person_cadence_step = min(person_cadence_step, cadence_step)
                deal_added += len(valid_person_phones)
            email_ready = bool(str(primary_email or "").strip())
            if deal_phones:
                lead_payload = {
                    "id": int(deal.get("id") or 0),
                    "nome": primary_name,
                    "empresa": self._extract_company_name(deal),
                    "phone": deal_phones[0],
                    "phones": deal_phones,
                    "telefones": deal_phones,
                    "email": primary_email,
                    "person_id": primary_person_id,
                    "super_minas": is_super_minas,
                    "cadence_step": cadence_step,
                    "deal_cadence_step": cadence_step,
                    "tentativas": 0,
                }
                leads.append(lead_payload)
                print(
                    f"[DEAL_ENFILEIRADO] deal={int(deal.get('id') or 0)} telefones={len(deal_phones)} "
                    f"prevalidado_ok={deal_validation_valid} prevalidado_invalid={deal_validation_invalid}"
                )
                print(
                    f"[DEAL_CANAIS] deal={int(deal.get('id') or 0)} "
                    f"whatsapp_valido={len(deal_phones)} email_valido={1 if email_ready else 0}"
                )
                if email_ready:
                    print(f"[DEAL_EMAIL_VALIDO] deal={int(deal.get('id') or 0)} email={primary_email}")
                if deal_phones and email_ready:
                    print(
                        f"[DEAL_MULTI_CANAL_VALIDADO] deal={int(deal.get('id') or 0)} "
                        f"telefones={len(deal_phones)} email={primary_email}"
                    )
            elif deal_validation_checked > 0:
                print(
                    f"[DEAL_SEM_WHATSAPP_VALIDO] deal={int(deal.get('id') or 0)} "
                    f"checados={deal_validation_checked} invalidos={deal_validation_invalid}"
                )
                if email_ready:
                    print(f"[DEAL_EMAIL_VALIDO] deal={int(deal.get('id') or 0)} email={primary_email}")
                    print(
                        f"[DEAL_EMAIL_ONLY_VALIDO] deal={int(deal.get('id') or 0)} "
                        f"checados={deal_validation_checked} email={primary_email}"
                    )

        super_minas = [lead for lead in leads if lead["super_minas"]]
        normal = [lead for lead in leads if not lead["super_minas"]]
        print(f"[LEADS_ELEGIVEIS] total={len(super_minas) + len(normal)} super_minas={len(super_minas)} normal={len(normal)}")
        print(
            "[SUPER_MINAS_SKIP] "
            f"cad={super_skip['cad']} sem_person={super_skip['sem_person']} "
            f"sem_phone={super_skip['sem_phone']} bloqueado={super_skip['bloqueado']}"
        )
        if BOT_PRIORITY:
            return super_minas + normal
        return leads

    def _whatsapp_status(self):
        return get_whatsapp_status(timeout=5)

    def _heartbeat_whatsapp(self):
        now = time.time()
        if now - float(self._last_wa_heartbeat_at or 0.0) < 10:
            return
        self._last_wa_heartbeat_at = now
        try:
            self.whatsapp.heartbeat()
        except Exception:
            print("[WA_OFFLINE]")

    def _touch_progress(self):
        self._last_progress_at = time.time()

    def _watchdog_maybe_reset(self):
        if not self.pending_whatsapp_queue:
            self._touch_progress()
            return
        if time.time() - float(self._last_progress_at or 0.0) < WATCHDOG_STALL_SECONDS:
            return
        wa_status = self._whatsapp_status()
        if bool(wa_status.get("needs_qr")) or bool(wa_status.get("session_invalid")):
            return
        print("[WATCHDOG_RESET]")
        self._touch_progress()
        self._recover_stack_if_needed(reason="watchdog")

    def _recover_stack_if_needed(self, reason="runtime"):
        if not WHATSAPP_LISTENER_ATIVO:
            print(f"[WHATSAPP_LISTENER_DESATIVADO] stack_recovery_ignorado motivo={reason}")
            return False
        now = time.time()
        if now < float(self._next_stack_recovery_at or 0):
            remaining = max(0, int(float(self._next_stack_recovery_at or 0) - now))
            print(f"[STACK_RECOVERY_AGUARDANDO] motivo={reason} segundos_restantes={remaining}")
            return False
        if self._stack_recovery_attempts >= STACK_RECOVERY_MAX_ATTEMPTS:
            print(f"[STACK_RECOVERY_MAX] motivo={reason} tentativas={self._stack_recovery_attempts}")
            return False
        self._next_stack_recovery_at = now + STACK_RECOVERY_COOLDOWN_SEC
        self._stack_recovery_attempts += 1
        print(f"[STACK_RECOVERY] motivo={reason}")
        try:
            start_stack_fast()
            wa_status = get_whatsapp_status(timeout=1)
            if bool(wa_status.get("connected")):
                self._stack_recovery_attempts = 0
                return True
            print("[WA_OFFLINE]")
            return False
        except Exception as exc:
            print(f"[STACK_RECOVERY_FALHOU] motivo={reason} erro={exc}")
            return False

    def _lead_queue_key(self, lead):
        lead = lead or {}
        return (
            0 if BOT_PRIORITY and bool(lead.get("super_minas")) else 1,
            int(lead.get("id") or 0),
            self._cadence_step(lead),
        )

    def _queue_has_lead(self, lead):
        target_id = int((lead or {}).get("id") or 0)
        return any(int((item or {}).get("id") or 0) == target_id for item in self.pending_whatsapp_queue)

    def _lead_runtime_ready(self, lead):
        lead = lead or {}
        lead_phones = self._lead_phones(lead)
        num = next((phone for phone in lead_phones if phone not in self.blocklist), lead_phones[0] if lead_phones else "")
        cadence_step = self._cadence_step(lead)
        deal_id = int((lead or {}).get("id") or 0)
        if not num:
            return False, "invalid"
        if self._lead_has_inbound_history(lead):
            return False, "ja_respondeu"
        if deal_id > 0 and any(self._already_sent_today_for_lead(deal_id, phone) for phone in lead_phones):
            return False, "ja_enviado_hoje"
        if bool(lead.get("super_minas")) and cadence_step == 1 and (
            self._lead_has_outbound_history(lead) or any(self._phone_has_send_record(phone) for phone in lead_phones)
        ):
            return False, "telefone_ja_contatado"
        if cadence_step > 1 and not self._cadence_due(lead, cadence_step, channel="whatsapp"):
            return False, "cadencia_ainda_no_cooldown"
        return True, "ok"

    def _enqueue_whatsapp_leads(self, leads, target_size=None):
        added = 0
        skip_blocked = 0
        skip_duplicate = 0
        skip_queue = 0
        skip_runtime = 0
        max_size = max(0, int(target_size or 0)) if target_size is not None else 0
        for lead in leads:
            if max_size and len(self.pending_whatsapp_queue) >= max_size:
                break
            lead_phones = self._lead_phones(lead)
            available_phones = [phone for phone in lead_phones if phone not in self.blocklist]
            num = available_phones[0] if available_phones else (lead_phones[0] if lead_phones else "")
            if not available_phones:
                skip_blocked += 1
                continue
            if self._channel_already_sent("whatsapp", lead):
                skip_duplicate += 1
                continue
            if self._queue_has_lead(lead):
                skip_queue += 1
                continue
            runtime_ready, runtime_reason = self._lead_runtime_ready(lead)
            if not runtime_ready:
                skip_runtime += 1
                print(
                    f"[QUEUE_SKIP_RUNTIME] deal={int((lead or {}).get('id') or 0)} "
                    f"telefone={num} motivo={runtime_reason}"
                )
                continue
            self.pending_whatsapp_queue.append(dict(lead))
            added += 1
        self.pending_whatsapp_queue.sort(key=self._lead_queue_key)
        print(
            f"[QUEUE_PREFILTER] alvo={max_size or len(self.pending_whatsapp_queue)} adicionados={added} "
            f"skip_bloqueado={skip_blocked} skip_duplicado={skip_duplicate} "
            f"skip_fila={skip_queue} skip_runtime={skip_runtime}"
        )
        return added

    def _next_whatsapp_delay(self):
        return random.randint(WHATSAPP_SEND_GAP_MIN_SEC, WHATSAPP_SEND_GAP_MAX_SEC)

    def _maybe_start_n8n_runtime(self):
        now = time.time()
        if now < float(self._next_n8n_start_attempt_at or 0):
            return
        self._next_n8n_start_attempt_at = now + 300
        maybe_start_n8n(timeout=10)

    def run(self):
        check_lock()
        try:
            if not hasattr(self, "_warmup_done"):
                self._warmup_done = True
                print("[WARMUP] aguardando 60s antes da primeira busca CRM")
                time.sleep(60)
            now = time.time()
            if now < float(getattr(self, "_crm_cooldown_until", 0.0) or 0.0):
                restante = int(float(self._crm_cooldown_until or 0.0) - now)
                print(f"[CRM_COOLDOWN_ATIVO] aguardando {restante}s")
                time.sleep(min(max(restante, 1), 30))
                return
            if not self.pending_whatsapp_queue and now < float(self._next_crm_fetch_at or 0.0):
                wait_seconds = max(1, int(float(self._next_crm_fetch_at or 0.0) - now))
                print("[MODO_ECONOMICO_ATIVO]")
                print(f"[MODO_ECONOMICO] fila vazia, aguardando... segundos={wait_seconds}")
                return
            self._maybe_start_n8n_runtime()
            self._process_pending_emails()
            if BOT_PRIORITY:
                print("[BOT_PRIORITY]")
            print("[BOT_ATIVO]")
            print("[BOT_READY]")
            self._watchdog_maybe_reset()
            sent_today = self._daily_success_count()
            print(f"[CONTADOR] enviados={sent_today}/{MAX_DEALS_DIA}")
            print(f"[SALDO_DIA] restantes={max(0, MAX_DEALS_DIA - sent_today)} enviados={sent_today} cap={MAX_DEALS_DIA}")
            resumed_after_hours = self._resume_after_hours_pending()
            if resumed_after_hours:
                self._touch_progress()
                self._process_pending_emails()
                return
            vps_processed = self._process_vps_pending_inbounds()
            if vps_processed:
                self._touch_progress()
                self._process_pending_emails()
                return
            recovered_inbounds = self._recover_pending_inbounds()
            if recovered_inbounds:
                print(f"[INBOUND_PRIORIDADE] recuperados={recovered_inbounds}")
                self._process_pending_emails()
                return
            leads = []
            added_to_queue = 0
            if not self.pending_whatsapp_queue:
                leads = self.buscar()
                if ENVIAR_WHATSAPP_AUTOMATICO:
                    added_to_queue = self._enqueue_whatsapp_leads(leads, target_size=len(leads))
                if not leads and not self.pending_whatsapp_queue:
                    self._next_crm_fetch_at = time.time() + CRM_IDLE_POLL_SEC
                else:
                    self._next_crm_fetch_at = 0.0
            super_minas_count = sum(1 for lead in self.pending_whatsapp_queue if lead["super_minas"])
            print(
                f"[PIPELINE] total={len(leads) if leads else len(self.pending_whatsapp_queue)} super_minas={super_minas_count} "
                f"fila_whatsapp={len(self.pending_whatsapp_queue)} novos={added_to_queue} enviados_hoje={sent_today}"
            )
            print(f"[QUEUE_READY] fila_whatsapp={len(self.pending_whatsapp_queue)}")
            print(f"[QUEUE_FULL_READY] deals={len(self.pending_whatsapp_queue)}")
            if not leads and not self.pending_whatsapp_queue:
                print("[PIPELINE_VAZIA] aguardando 120s antes de nova busca")
                print("[MODO_ECONOMICO_ATIVO]")
                print("[MODO_ECONOMICO] fila vazia, aguardando...")
                time.sleep(120)
                return
            if not ENVIAR_WHATSAPP_AUTOMATICO:
                print("[WHATSAPP_AUTO_DESATIVADO] envio_inicial_bloqueado")
                self._process_pending_emails()
                return
            if self.pending_whatsapp_queue:
                print("[ENVIO_INICIADO]")
            sent_this_run = 0

            while self.pending_whatsapp_queue:
                if time.time() < float(self.next_whatsapp_send_at or 0):
                    wait_seconds = max(0, int(float(self.next_whatsapp_send_at or 0) - time.time()))
                    print(f"[WHATSAPP_COOLDOWN] segundos_restantes={wait_seconds} fila={len(self.pending_whatsapp_queue)}")
                    break

                lead = self.pending_whatsapp_queue.pop(0)

                nome = lead["nome"]
                empresa = lead.get("empresa") or "a empresa"
                deal_id = lead["id"]
                prioridade = "SUPER_MINAS" if lead["super_minas"] else "CRM"
                cadence_step = self._cadence_step(lead)
                lead_phones = self._ordered_lead_phones_for_cycle(lead)
                print(
                    f"[DEAL_TENTATIVA] deal={deal_id} origem={prioridade} cadencia={cadence_step} "
                    f"telefones_total={len(lead_phones)} saldo_restante={max(0, MAX_DEALS_DIA - sent_today)}"
                )

                try:
                    deal_actual = self.crm.get_deal_details(deal_id)
                    actual_tokens = set(self._deal_tokens(deal_actual))
                    if "RESPONDIDO" in actual_tokens:
                        print(f"[RACE_CONDITION] deal={deal_id} ja_tagueado")
                        continue
                except Exception:
                    continue

                wa_status = self.whatsapp.status_payload()
                if bool(wa_status.get("needs_qr")) or bool(wa_status.get("session_invalid")):
                    print(
                        f"[WA_BLOQUEADO_SEM_SESSAO] mode={str(wa_status.get('mode') or 'offline')} "
                        f"needs_qr={bool(wa_status.get('needs_qr'))} "
                        f"session_invalid={bool(wa_status.get('session_invalid'))}"
                    )
                    self.pending_whatsapp_queue.insert(0, lead)
                    self.next_whatsapp_send_at = time.time() + 5
                    break
                if not bool(wa_status.get("connected")):
                    print(
                        f"[WA_OFFLINE] mode={str(wa_status.get('mode') or 'offline')} "
                        f"needs_qr={bool(wa_status.get('needs_qr'))} "
                        f"session_invalid={bool(wa_status.get('session_invalid'))}"
                    )
                    self.pending_whatsapp_queue.insert(0, lead)
                    self.next_whatsapp_send_at = time.time() + 5
                    self._recover_stack_if_needed(reason="whatsapp_offline")
                    break

                sent_phones = []
                blocked_by_session = False
                blocked_by_offline = False
                for num in lead_phones:
                    num = self.whatsapp.normalize_phone(num)
                    if not num or num in self.blocklist:
                        print(f"[BLOQUEADO] deal={deal_id} telefone={num}")
                        continue

                    # TRAVA DE OUTBOUND (HORARIO + WHITELIST)
                    if not is_allowed_to_send_outbound(num):
                        continue

                    if self._already_sent_today_for_lead(deal_id, num):
                        print(f"[DUPLICIDADE_BLOQUEADA_DIA] deal={deal_id} telefone={num}")
                        continue
                    reserved = (
                        self.whatsapp.reserve_followup_send(num)
                        if cadence_step > 1
                        else self.whatsapp.reserve_send(num)
                    )
                    if not reserved:
                        print(f"[DUPLICIDADE_BLOQUEADA] deal={deal_id} telefone={num}")
                        continue

                    msg = self.pitch.get_cadence_message(
                        cadence_step,
                        lead={"nome": nome, "empresa": empresa, "telefone": num},
                    )
                    print(f"[PROCESSANDO] origem={prioridade} deal={deal_id} telefone={num} cadencia={cadence_step}")
                    print(f"[WHATSAPP_ENVIO] deal={deal_id} telefone={num} cadencia={cadence_step}")
                    daily_locked = self._lock_sent_today_for_lead(deal_id, num)
                    if not daily_locked:
                        self.whatsapp.release_send_slot(num)
                        print(f"[DUPLICIDADE_BLOQUEADA_DIA] deal={deal_id} telefone={num}")
                        continue
                    ok = self.whatsapp.send_message(num, msg, cadence_step=cadence_step, deal_id=deal_id)
                    if ok:
                        confirmed = self.whatsapp.wait_for_outbound_sync(num, msg, timeout_seconds=8, poll_seconds=0.5)
                        if not confirmed and self._recent_outbound_message_seen(num, msg, within_seconds=180):
                            print(f"[ENVIO_CONFIRMADO_POR_HISTORICO] deal={deal_id} telefone={num}")
                            confirmed = True
                        if not confirmed:
                            self.whatsapp.release_send_slot(num)
                            print(f"[ENVIO_SEM_CONFIRMACAO] deal={deal_id} telefone={num}")
                            continue
                        self.whatsapp.mark_sent(num)
                        sent_phones.append(num)
                        self._touch_progress()
                        print(f"[DEAL_CONTATO_UNICO] deal={deal_id} telefone={num}")
                        break

                    if self.whatsapp.last_send_state == "timeout":
                        self.whatsapp.release_send_slot(num)
                        print(f"[WHATSAPP_TIMEOUT] deal={deal_id} telefone={num}")
                    elif self.whatsapp.last_send_state == "session_blocked":
                        self.whatsapp.release_send_slot(num)
                        print(f"[WA_BLOQUEADO_SEM_SESSAO] deal={deal_id} telefone={num}")
                        blocked_by_session = True
                        break
                    elif self.whatsapp.last_send_state == "offline":
                        self.whatsapp.release_send_slot(num)
                        print(f"[WA_OFFLINE] deal={deal_id} telefone={num}")
                        blocked_by_offline = True
                        break
                    elif self.whatsapp.last_send_state == "invalid":
                        self.whatsapp.mark_invalid(num, "invalid_phone_outbound")
                        print(f"[SEM_WHATSAPP] deal={deal_id} telefone={num}")
                    else:
                        self.whatsapp.release_send_slot(num)
                        print(f"[FALHA_ENVIO] deal={deal_id} telefone={num}")

                if blocked_by_session or blocked_by_offline:
                    self.pending_whatsapp_queue.insert(0, lead)
                    self.next_whatsapp_send_at = time.time() + 5
                    if blocked_by_offline:
                        self._recover_stack_if_needed(reason="whatsapp_offline")
                    break

                if not sent_phones:
                    print(
                        f"[ENVIO_FALHOU_DEAL] deal={deal_id} telefones_total={len(lead_phones)} "
                        f"telefones_validos_enviados=0"
                    )
                    self.next_whatsapp_send_at = time.time() + 1
                    continue

                self.whatsapp.mark_deal_sent(deal_id)
                self._mark_channel_sent("whatsapp", lead, f"WHATSAPP_CAD_{cadence_step}")
                for successful_phone in sent_phones:
                    print(f"[WHATSAPP_ENVIADO] deal={deal_id} telefone={successful_phone}")
                    print(f"[ENVIADO_REAL] deal={deal_id} telefone={successful_phone}")
                try:
                    whatsapp_tag = self._whatsapp_tag(lead)
                    tag_ok = self.crm.add_tag("deal", deal_id, whatsapp_tag)
                    if tag_ok:
                        print(f"[TAG_APLICADA] deal={deal_id} tag={whatsapp_tag}")
                    else:
                        print(f"[TAG_FALHA] deal={deal_id} tag={whatsapp_tag}")
                except Exception:
                    print(f"[TAG_FALHA] deal={deal_id} tag={self._whatsapp_tag(lead)}")
                if cadence_step == 1:
                    try:
                        self.crm.add_tag("deal", deal_id, "cad1")
                    except Exception:
                        pass
                if str(self.crm.STATUS_BOT_FIELD or "").strip() not in {"", "status_bot"}:
                    try:
                        self.crm.update_deal(deal_id, {self.crm.STATUS_BOT_FIELD: "contato_iniciado"})
                    except Exception:
                        pass
                self.processar_tag("whatsapp", int((lead or {}).get("person_id") or 0))
                self._append_deal_note(deal_id, f"WhatsApp enviado na cadencia {cadence_step}.")
                stage_step = max(cadence_step, int(lead.get("deal_cadence_step") or cadence_step))
                self._move_deal_to_cadence_stage(deal_id, stage_step)
                if cadence_step >= ARCHIVE_AFTER_CADENCE_STEP and not any(self._phone_has_inbound_history(phone) for phone in sent_phones):
                    self._archive_no_contact(deal_id)
                self._queue_email_after_whatsapp(lead)
                self._process_pending_emails()
                sent_this_run += 1
                sent_today = self._increment_send_progress()
                print(f"[DEAL_SUCESSO] deal={deal_id} enviados={sent_today}/{MAX_DEALS_DIA} telefones_ok={len(sent_phones)}")
                self.next_whatsapp_send_at = time.time() + self._next_whatsapp_delay()
            if sent_this_run == 0 and not self.pending_whatsapp_queue:
                pass
            self._process_pending_emails()
        finally:
            release_lock()

    def start(self):
        while True:
            time.sleep(SUPERVISOR_LOOP_SLEEP_SEC)
            if WHATSAPP_LISTENER_ATIVO:
                self._heartbeat_whatsapp()
                self._watchdog_maybe_reset()
                wa_status = self._whatsapp_status()
                if ENVIAR_WHATSAPP_AUTOMATICO and (bool(wa_status.get("needs_qr")) or bool(wa_status.get("session_invalid"))):
                    print(
                        f"[WA_BLOQUEADO_SEM_SESSAO] mode={str(wa_status.get('mode') or 'offline')} "
                        f"needs_qr={bool(wa_status.get('needs_qr'))} session_invalid={bool(wa_status.get('session_invalid'))}"
                    )
                elif ENVIAR_WHATSAPP_AUTOMATICO and not bool(wa_status.get("connected")):
                    self._recover_stack_if_needed(reason="pre_run_whatsapp_offline")
            if not ENVIAR_WHATSAPP_AUTOMATICO:
                print("[WHATSAPP_AUTO_MANUAL] outbound_inicial_desativado")
            self.run()


if __name__ == "__main__":
    run_all = any(str(arg).strip().lower() in {"--all", "all"} for arg in sys.argv[1:])
    if run_all:
        start_stack_fast()
    SDRSupervisor().start()
