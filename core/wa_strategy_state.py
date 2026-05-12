from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
STATE_FILE = Path(os.getenv("WA_STRATEGY_STATE_FILE") or BASE_DIR / "data" / "wa_strategy_state.json")


@contextmanager
def locked_json_file(path, timeout=10):
    lock_path = Path(str(path) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = None
    started_at = time.time()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, str(os.getpid()).encode("ascii", errors="ignore"))
            break
        except FileExistsError:
            try:
                if time.time() - lock_path.stat().st_mtime > 300:
                    lock_path.unlink()
                    continue
            except Exception:
                pass
            if time.time() - started_at >= timeout:
                raise TimeoutError(str(lock_path))
            time.sleep(0.05)
    try:
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        try:
            lock_path.unlink()
        except Exception:
            pass


def strategy_key(deal_id, phone, strategy=""):
    phone_value = "".join(ch for ch in str(phone or "") if ch.isdigit())
    base = f"{int(deal_id or 0)}:{phone_value}"
    strategy_value = str(strategy or "").strip()
    return f"{base}:{strategy_value}" if strategy_value else base


def load_strategy_state():
    try:
        with locked_json_file(STATE_FILE):
            if STATE_FILE.exists():
                payload = json.loads(STATE_FILE.read_text(encoding="utf-8") or "{}")
                return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print(f"[WA_STRATEGY_STATE_READ_FAIL] {exc}")
    return {}


def save_strategy_state(state):
    with locked_json_file(STATE_FILE):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(state if isinstance(state, dict) else {}, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STATE_FILE)


def get_record(state, deal_id, phone, strategy=""):
    key = strategy_key(deal_id, phone, strategy)
    record = (state or {}).get(key)
    return dict(record or {}) if isinstance(record, dict) else {}


def set_record(state, deal_id, phone, strategy, **fields):
    key = strategy_key(deal_id, phone, strategy)
    previous = (state or {}).get(key)
    previous = dict(previous or {}) if isinstance(previous, dict) else {}
    now = datetime.now().isoformat(timespec="seconds")
    record = {
        **previous,
        "deal_id": int(deal_id or 0),
        "phone": "".join(ch for ch in str(phone or "") if ch.isdigit()),
        "strategy": str(strategy or "").strip(),
        "updated_at": now,
        **dict(fields or {}),
    }
    state[key] = record
    return record


def mark_warm(state, deal_id, phone, reason="warm_trigger"):
    phone_value = "".join(ch for ch in str(phone or "") if ch.isdigit())
    hunter = get_record(state, deal_id, phone_value, "wa_hunter")
    if hunter and hunter.get("status") not in {"warm", "stopped", "blocked", "completed"}:
        set_record(
            state,
            deal_id,
            phone_value,
            "wa_hunter",
            status="warm",
            reason=str(reason or "warm_trigger"),
            stopped_at=datetime.now().isoformat(timespec="seconds"),
        )
        print(f"[WA_HUNTER_STOP_WARM] deal_id={deal_id} phone={phone_value} reason={reason}")
    return set_record(
        state,
        deal_id,
        phone_value,
        "wa_warm",
        status="active",
        reason=str(reason or "warm_trigger"),
        warm_started_at=datetime.now().isoformat(timespec="seconds"),
    )


def has_sent_step(state, deal_id, phone, strategy, step_name):
    record = get_record(state, deal_id, phone, strategy)
    return str(step_name or "") in {str(item) for item in list(record.get("sent_steps") or [])}


def append_sent_step(state, deal_id, phone, strategy, step_name, **fields):
    record = get_record(state, deal_id, phone, strategy)
    sent_steps = [str(item) for item in list(record.get("sent_steps") or []) if str(item)]
    if str(step_name or "") and str(step_name) not in sent_steps:
        sent_steps.append(str(step_name))
    return set_record(state, deal_id, phone, strategy, sent_steps=sent_steps, **fields)
