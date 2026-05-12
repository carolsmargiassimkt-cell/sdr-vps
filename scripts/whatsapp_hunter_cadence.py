from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

BASE_DIR = Path(os.getenv("SDR_BASE_DIR") or Path(__file__).resolve().parents[1])
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
DATA_DIR = BASE_DIR / "data"
RUNTIME_DIR = BASE_DIR / "runtime"
LOG_PREFIX = "[WA_HUNTER_CADENCE]"

STATE_FILE = DATA_DIR / "whatsapp_hunter_state.json"
EMAIL_QUEUE_FILE = DATA_DIR / "email_cadence_queue.json"
LOCK_FILE = RUNTIME_DIR / "whatsapp_hunter_cadence.lock"
BUSINESS_DAY_SCRIPT = BASE_DIR / "scripts" / "is_business_day_br.py"

PIPELINE_ID = int(os.getenv("PIPEDRIVE_PIPELINE_ID", "7") or 7)
DAILY_LIMIT = int(os.getenv("WA_HUNTER_DAILY_CAP", "20") or 20)
GATEWAY_URL = os.getenv("WHATSAPP_GATEWAY_SEND_URL", "http://127.0.0.1:3000/send")

try:
    from supervisor import HUNTER_STEPS
except Exception:
    HUNTER_STEPS = {
        1: "Oi, tudo bem? Aqui e a Carol da Mand Digital. Queria falar com quem cuida de marketing ou campanhas promocionais por ai.",
        2: "Carol da Mand por aqui. A gente ajuda empresas a transformar campanhas em experiencias interativas, tipo roleta, raspadinha e QR Code para captar dados. Faz sentido eu falar com marketing?",
        3: "Ultima tentativa por aqui para nao insistir. Se fizer sentido falar sobre campanhas promocionais com captacao de dados e WhatsApp, fico a disposicao.",
    }

from crm.pipedrive_client import PipedriveClient

crm = PipedriveClient()

STOP_STATUSES = {"clicked_warm", "warm", "replied", "respondido", "won", "lost", "opt_out", "wrong_contact", "stopped"}
WARM_LABELS = {"226", "WARM_WHATSAPP", "CLICKED_WARM", "CONVERSANDO"}
STOP_LABELS = {"196", "RESPONDIDO", "SEM_INTERESSE", "NUMERO_ERRADO", "WON", "LOST"}
NURTURING_BLOCK_TERMS = ("step_1", "step_2", "step_3", "step_4", "step_5", "cad1", "cad2", "cad3", "cad4", "cad5", "nutricao", "nutrição")
NURTURING_DONE_TERMS = ("step_6", "cad6", "email_6", "cadencia_finalizada", "cadence_done", "completed", "finalizado", "done")


def now_br() -> datetime:
    return datetime.now(ZoneInfo("America/Sao_Paulo"))


def is_business_window() -> bool:
    current = now_br()
    if current.weekday() >= 5 or not (9 <= current.hour < 18):
        return False
    if BUSINESS_DAY_SCRIPT.exists():
        result = subprocess.run([sys.executable, str(BUSINESS_DAY_SCRIPT)], cwd=str(BASE_DIR), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return result.returncode == 0
    return True


def acquire_lock():
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            pass
        except BlockingIOError:
            os.close(fd)
            print("[WA_HUNTER_LOCK_BUSY]", str(LOCK_FILE))
            return None
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({"pid": os.getpid(), "timestamp": now_br().isoformat(timespec="seconds")}).encode("utf-8"))
        os.fsync(fd)
        print("[WA_HUNTER_LOCK_ACQUIRE]", str(LOCK_FILE), "pid", os.getpid())
        return fd
    except Exception:
        os.close(fd)
        raise


def release_lock(fd):
    if fd is None:
        return
    try:
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
        except ImportError:
            pass
    finally:
        os.close(fd)
    print("[WA_HUNTER_LOCK_RELEASE]", str(LOCK_FILE), "pid", os.getpid())


def load_json(path: Path, fallback):
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8-sig") or "null")
            return payload if isinstance(payload, type(fallback)) else fallback
    except Exception as exc:
        print(LOG_PREFIX, "JSON_READ_FAIL", str(path), str(exc))
    return fallback


def save_json_atomic(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def normalize_phone(value) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def valid_phone(phone: str) -> bool:
    digits = normalize_phone(phone)
    if not re.fullmatch(r"55\d{10,11}", digits or ""):
        return False
    return len(set(digits[-8:])) > 1


def phone_from_deal(deal: dict) -> str:
    person = (deal or {}).get("person_id") or {}
    if isinstance(person, dict):
        for item in list(person.get("phone") or []):
            phone = normalize_phone((item or {}).get("value"))
            if phone:
                return phone
    return ""


def label_tokens(deal: dict) -> set[str]:
    try:
        return {
            str(token or "").strip().upper()
            for token in crm.resolve_label_tokens((deal or {}).get("label"), crm.get_deal_labels())
            if str(token or "").strip()
        }
    except Exception:
        raw = (deal or {}).get("label")
        if isinstance(raw, list):
            return {str(item.get("id") if isinstance(item, dict) else item).strip().upper() for item in raw if str(item or "").strip()}
        return {item.strip().upper() for item in str(raw or "").split(",") if item.strip()}


def blocklist_values() -> set[str]:
    values = set()
    for path in (
        DATA_DIR / "whatsapp_manual_blocklist.json",
        DATA_DIR / "whatsapp_blocklist.json",
        BASE_DIR / "logs" / "whatsapp_manual_blocklist.json",
        BASE_DIR / "invalidos.json",
    ):
        payload = load_json(path, [] if path.suffix == ".json" else {})
        stack = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                stack.extend(item.values())
                stack.extend(item.keys())
            elif isinstance(item, list):
                stack.extend(item)
            else:
                phone = normalize_phone(item)
                if phone:
                    values.add(phone)
                    if phone.startswith("55"):
                        values.add(phone[2:])
    return values


def row_for_deal(rows: list[dict], deal_id: int) -> dict:
    matches = [row for row in rows if int((row or {}).get("deal_id") or 0) == int(deal_id)]
    return dict(matches[-1]) if matches else {}


def deal_text(deal: dict, tokens: set[str]) -> str:
    fields = [
        "stage_name",
        "cadence_status",
        "email_cadence_status",
        "automation_status",
        "status_sdr",
        "status_bot",
        crm.STATUS_BOT_FIELD,
    ]
    return " ".join([str((deal or {}).get(field) or "") for field in fields] + sorted(tokens)).lower()


def nurturing_done(deal: dict, tokens: set[str], queue_rows: list[dict]) -> tuple[bool, str]:
    deal_id = int((deal or {}).get("id") or 0)
    row = row_for_deal(queue_rows, deal_id)
    if row:
        status = str(row.get("status") or "").strip().lower()
        if status in STOP_STATUSES:
            return False, status
        if status in {"done", "completed", "finalizado"}:
            return True, status
        if int(row.get("last_sent_step_email") or 0) >= 6:
            return True, "step6_sent"
        return False, f"email_step_{int(row.get('last_sent_step_email') or row.get('step') or 0)}"
    text = deal_text(deal, tokens)
    print("[EMAIL_QUEUE_MISSING_FALLBACK]", "deal_id", deal_id)
    if any(term in text for term in NURTURING_BLOCK_TERMS):
        return False, "fallback_nurturing_signal"
    if any(term in text for term in NURTURING_DONE_TERMS):
        return True, "fallback_done_signal"
    return False, "no_nurturing_done_signal"


def is_warm_or_stopped(deal: dict, tokens: set[str], phone: str) -> tuple[bool, str]:
    status = str((deal or {}).get("status") or "open").strip().lower()
    if status in {"won", "lost"}:
        return True, status
    status_text = deal_text(deal, tokens)
    if tokens & WARM_LABELS or any(item in status_text for item in ("clicked_warm", "warm", "conversando")):
        return True, "warm"
    if tokens & STOP_LABELS or any(item in status_text for item in STOP_STATUSES):
        return True, "stopped"
    return False, ""


def next_step(record: dict) -> str:
    current = str((record or {}).get("step") or "").strip()
    if current == "hunter_1":
        return "hunter_2"
    if current == "hunter_2":
        return "hunter_3"
    if current in {"hunter_3", "done"} or str((record or {}).get("status")) == "done":
        return ""
    return "hunter_1"


def sent_today_count(state: dict) -> int:
    today = now_br().date().isoformat()
    return sum(1 for item in state.values() if isinstance(item, dict) and str(item.get("last_sent_at") or "").startswith(today))


def send_whatsapp(phone: str, text: str) -> dict:
    response = requests.post(GATEWAY_URL, json={"number": phone, "text": text, "count_towards_daily_limit": True}, timeout=60)
    try:
        payload = response.json()
    except Exception:
        payload = {}
    ok = response.status_code == 200 and str(payload.get("status") or "").lower() == "sent"
    return {
        "ok": ok,
        "status_code": response.status_code,
        "status": str(payload.get("status") or ""),
        "message_id": str(payload.get("message_id") or ""),
        "body": response.text[:200],
    }


def select_candidates(limit: int) -> list[dict]:
    deals = crm.get_deals(status="open", pipeline_id=PIPELINE_ID, limit=max(1, int(os.getenv("WA_HUNTER_SCAN_LIMIT", "500") or 500)))
    queue_rows = load_json(EMAIL_QUEUE_FILE, [])
    blocked = blocklist_values()
    state = load_json(STATE_FILE, {})
    selected = []
    for deal in deals:
        deal_id = int((deal or {}).get("id") or 0)
        phone = phone_from_deal(deal)
        tokens = label_tokens(deal)
        if not valid_phone(phone):
            continue
        if phone in blocked or (phone.startswith("55") and phone[2:] in blocked):
            continue
        stopped, stop_reason = is_warm_or_stopped(deal, tokens, phone)
        if stopped:
            continue
        done, reason = nurturing_done(deal, tokens, queue_rows)
        if not done:
            print("[WA_HUNTER_BLOCKED_BY_NURTURING]", "deal_id", deal_id, "phone", phone, "reason", reason)
            continue
        record = state.get(str(deal_id), {}) if isinstance(state, dict) else {}
        if record.get("status") == "pending_send":
            continue
        step = next_step(record)
        if not step:
            continue
        if record.get("step") == step and record.get("status") == "sent":
            continue
        selected.append({"deal": deal, "deal_id": deal_id, "phone": phone, "step": step, "reason": reason})
        if len(selected) >= limit:
            break
    return selected


def run(*, apply: bool, send: bool, limit: int) -> int:
    dry_run = not (apply and send)
    if not is_business_window():
        print(LOG_PREFIX, "FORA_HORARIO_COMERCIAL_BR")
        return 0
    lock_fd = None if dry_run else acquire_lock()
    if not dry_run and lock_fd is None:
        return 0
    try:
        state = load_json(STATE_FILE, {})
        selected = select_candidates(limit=limit)
        sent_count = sent_today_count(state)
        for item in selected:
            deal_id = str(item["deal_id"])
            phone = item["phone"]
            step = item["step"]
            step_number = int(step.split("_")[-1])
            text = HUNTER_STEPS[step_number]
            print("[WA_HUNTER_SELECTED]", "deal_id", deal_id, "phone", phone, "step", step, "reason", item["reason"])
            if dry_run:
                print("[WA_HUNTER_DRY_RUN]", "deal_id", deal_id, "phone", phone, "step", step)
                continue
            if sent_count >= DAILY_LIMIT:
                print(LOG_PREFIX, "DAILY_LIMIT_REACHED", DAILY_LIMIT)
                break
            state[deal_id] = {
                "phone": phone,
                "step": step,
                "status": "pending_send",
                "pending_send_at": now_br().isoformat(timespec="seconds"),
                "reason": item["reason"],
            }
            save_json_atomic(STATE_FILE, state)
            print("[WA_HUNTER_PENDING_SEND_SET]", "deal_id", deal_id, "phone", phone, "step", step)
            result = send_whatsapp(phone, text)
            if not result["ok"]:
                state[deal_id].update({"status": "pending_send", "send_status": result["status"], "reason": "send_failed"})
                save_json_atomic(STATE_FILE, state)
                print("[WA_HUNTER_SEND_FAIL]", "deal_id", deal_id, "phone", phone, "step", step, "status", result["status"], "http", result["status_code"])
                continue
            state[deal_id] = {
                "phone": phone,
                "step": step,
                "status": "done" if step == "hunter_3" else "sent",
                "last_sent_at": now_br().isoformat(timespec="seconds"),
                "message_id": result["message_id"],
                "reason": item["reason"],
            }
            save_json_atomic(STATE_FILE, state)
            crm.add_note(deal_id=int(deal_id), content=f"[WA_HUNTER_{step_number}] WhatsApp hunter etapa {step_number}/3 enviada.")
            try:
                crm.add_tag("deal", int(deal_id), f"WA_HUNTER_{step_number}")
            except Exception as exc:
                print(LOG_PREFIX, "TAG_FAIL", deal_id, str(exc))
            sent_count += 1
            print("[WA_HUNTER_SENT]", "deal_id", deal_id, "phone", phone, "step", step, "message_id", result["message_id"] or "-")
            time.sleep(2)
        return 0
    finally:
        if lock_fd is not None:
            release_lock(lock_fd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    return run(apply=bool(args.apply and not args.dry_run), send=bool(args.send and not args.dry_run), limit=max(1, int(args.limit or 10)))


if __name__ == "__main__":
    raise SystemExit(main())
