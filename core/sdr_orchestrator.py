from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parents[1]
EMAIL_QUEUE_FILE = BASE_DIR / "data" / "email_cadence_queue.json"
WA_STATE_FILE = Path(os.getenv("WA_STRATEGY_STATE_FILE") or BASE_DIR / "data" / "wa_strategy_state.json")
BACKUP_DIR = BASE_DIR / "backup" / "sdr_orchestration"

PIPELINE_STAGES = {
    "entrada": 51,
    "nutricao": 62,
    "pronto": 63,
    "tentativa": 52,
}

ACTION_HOLD = "HOLD"
ACTION_EMAIL_SEND = "EMAIL_SEND"
ACTION_WA_HUNTER_SEND = "WA_HUNTER_SEND"
ACTION_WA_WARM_SEND = "WA_WARM_SEND"
ACTION_CREATE_ACTIVITY = "CREATE_ACTIVITY"
ACTION_MOVE_STAGE = "MOVE_STAGE"
ACTION_MARK_LOST = "MARK_LOST"
ACTION_STOP_ALL = "STOP_ALL"

WARM_REASONS = {
    "clicked_warm",
    "replied",
    "respondido",
    "form",
    "form_responded",
    "calendly",
    "meeting",
    "positive_intent",
    "wa_positive",
    "lead_trafego",
    "warm",
}

STOP_REASONS = {"won", "lost", "opt_out", "negative_stop", "wrong_contact", "stopped", "shared_email_blocked", "shared_phone_blocked"}

HUNTER_ALLOWED_STAGE_IDS = {PIPELINE_STAGES["pronto"]}
EMAIL_ALLOWED_STAGE_IDS = {PIPELINE_STAGES["nutricao"]}
WARM_STAGE_IDS = {PIPELINE_STAGES["tentativa"]}
NO_COLD_STAGE_KEYWORDS = {
    "tentativa",
    "conexao",
    "conexão",
    "realizada",
    "reuniao",
    "reunião",
    "diagnostico",
    "diagnóstico",
    "proposta",
    "ganho",
    "perdido",
    "warm",
}

IMPEDITIVE_HUNTER_TAGS = {
    "conversando",
    "respondido",
    "linkedin",
    "indicacao",
    "indicação",
    "caro evento",
    "caro_evento",
    "lead trafego",
    "lead_trafego",
    "lead_tráfego",
    "193",
    "196",
    "226",
    "warm_whatsapp",
    "clicked_warm",
}

LEGACY_WA_TAGS = {"wa_cad4", "wa_cad5", "wa_cad6"}


def now_brt() -> datetime:
    return datetime.now(ZoneInfo("America/Sao_Paulo"))


def utcish_parse(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for candidate in (raw, raw.replace("Z", ""), raw.replace(" ", "T")):
        try:
            return datetime.fromisoformat(candidate)
        except Exception:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            pass
    return None


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8-sig") or "null")
            return payload if payload is not None else default
    except Exception as exc:
        print(f"[ORCH_BLOCK] reason=json_read_fail path={path} error={exc}")
    return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def backup_state_files() -> list[str]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = now_brt().strftime("%Y%m%d_%H%M%S")
    copied: list[str] = []
    for path in (EMAIL_QUEUE_FILE, WA_STATE_FILE):
        if path.exists():
            dest = BACKUP_DIR / f"{path.name}.{stamp}.bak"
            shutil.copy2(path, dest)
            copied.append(str(dest))
    return copied


def load_email_state() -> list[dict[str, Any]]:
    rows = load_json(EMAIL_QUEUE_FILE, [])
    return rows if isinstance(rows, list) else []


def save_email_state(rows: list[dict[str, Any]]) -> None:
    save_json(EMAIL_QUEUE_FILE, rows if isinstance(rows, list) else [])


def load_wa_state() -> dict[str, Any]:
    state = load_json(WA_STATE_FILE, {})
    return state if isinstance(state, dict) else {}


def save_wa_state(state: dict[str, Any]) -> None:
    save_json(WA_STATE_FILE, state if isinstance(state, dict) else {})


def deal_id_of(deal: dict[str, Any] | None) -> int:
    try:
        return int((deal or {}).get("id") or (deal or {}).get("deal_id") or 0)
    except Exception:
        return 0


def person_id_of(deal: dict[str, Any] | None, person: dict[str, Any] | None = None) -> int:
    raw = (person or {}).get("id") or (deal or {}).get("person_id")
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("id")
    try:
        return int(raw or 0)
    except Exception:
        return 0


def org_id_of(deal: dict[str, Any] | None, org: dict[str, Any] | None = None) -> int:
    raw = (org or {}).get("id") or (deal or {}).get("org_id")
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("id")
    try:
        return int(raw or 0)
    except Exception:
        return 0


def stage_id_of(deal: dict[str, Any] | None) -> int:
    raw = (deal or {}).get("stage_id")
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("id")
    try:
        return int(raw or 0)
    except Exception:
        return 0


def stage_name_of(deal: dict[str, Any] | None) -> str:
    stage = (deal or {}).get("stage_id")
    if isinstance(stage, dict):
        return str(stage.get("name") or "").strip().lower()
    return str((deal or {}).get("stage_name") or (deal or {}).get("stage") or "").strip().lower()


def label_tokens(deal: dict[str, Any] | None) -> set[str]:
    raw = (deal or {}).get("label") or (deal or {}).get("labels")
    values: list[Any]
    if isinstance(raw, list):
        values = raw
    else:
        values = str(raw or "").split(",")
    tokens = set()
    for item in values:
        if isinstance(item, dict):
            for key in ("id", "name", "label"):
                if item.get(key) is not None:
                    tokens.add(str(item.get(key)).strip().lower())
        else:
            token = str(item or "").strip().lower()
            if token:
                tokens.add(token)
    return tokens


def phone_from(deal: dict[str, Any] | None, person: dict[str, Any] | None = None, explicit: Any = "") -> str:
    if explicit:
        return normalize_phone(explicit)
    for source in (person or {}, (deal or {}).get("person_id") if isinstance((deal or {}).get("person_id"), dict) else {}):
        for item in list((source or {}).get("phone") or []):
            phone = normalize_phone(item.get("value") if isinstance(item, dict) else item)
            if phone:
                return phone
    return normalize_phone((deal or {}).get("phone"))


def email_from(deal: dict[str, Any] | None, person: dict[str, Any] | None = None, explicit: Any = "") -> str:
    if explicit:
        return normalize_email(explicit)
    for source in (person or {}, (deal or {}).get("person_id") if isinstance((deal or {}).get("person_id"), dict) else {}):
        for item in list((source or {}).get("email") or []):
            email = normalize_email(item.get("value") if isinstance(item, dict) else item)
            if email:
                return email
    return normalize_email((deal or {}).get("email"))


def queue_rows_for_deal(email_state: list[dict[str, Any]], deal_id: int) -> list[dict[str, Any]]:
    return [row for row in list(email_state or []) if int(row.get("deal_id") or 0) == int(deal_id or 0)]


def active_email_rows(email_state: list[dict[str, Any]], deal_id: int) -> list[dict[str, Any]]:
    stop = STOP_REASONS | WARM_REASONS | {"done"}
    return [row for row in queue_rows_for_deal(email_state, deal_id) if str(row.get("status") or "").strip().lower() not in stop]


def last_email_sent_at(email_state: list[dict[str, Any]], deal_id: int) -> datetime | None:
    dates = []
    for row in queue_rows_for_deal(email_state, deal_id):
        parsed = utcish_parse(row.get("last_sent_at") or row.get("updated_at"))
        if parsed:
            dates.append(parsed)
    return max(dates) if dates else None


def wa_records_for_deal_phone(wa_state: dict[str, Any], deal_id: int, phone: str) -> list[dict[str, Any]]:
    phone_value = normalize_phone(phone)
    rows = []
    for record in list((wa_state or {}).values()):
        if not isinstance(record, dict):
            continue
        if int(record.get("deal_id") or 0) != int(deal_id or 0):
            continue
        if phone_value and normalize_phone(record.get("phone")) != phone_value:
            continue
        rows.append(record)
    return rows


def wa_record(wa_state: dict[str, Any], deal_id: int, phone: str, strategy: str) -> dict[str, Any]:
    for record in wa_records_for_deal_phone(wa_state, deal_id, phone):
        if str(record.get("strategy") or "") == str(strategy or ""):
            return dict(record)
    return {}


def sent_steps(record: dict[str, Any], prefix: str = "") -> set[str]:
    steps = {str(item) for item in list(record.get("sent_steps") or []) if str(item)}
    last = int(record.get("hunter_step") or record.get("last_sent_step_whatsapp") or 0)
    if prefix and last:
        for step in range(1, last + 1):
            steps.add(f"{prefix}_{step}")
    return steps


def last_wa_sent_at(wa_state: dict[str, Any], deal_id: int, phone: str = "") -> datetime | None:
    dates = []
    for record in wa_records_for_deal_phone(wa_state, deal_id, phone):
        parsed = utcish_parse(record.get("last_sent_at") or record.get("last_outbound_at"))
        if parsed:
            dates.append(parsed)
    return max(dates) if dates else None


def has_warm_signal(deal: dict[str, Any] | None, email_rows: list[dict[str, Any]], wa_state: dict[str, Any], deal_id: int, phone: str) -> bool:
    tokens = label_tokens(deal)
    status_values = {
        str((deal or {}).get("status_bot") or "").strip().lower(),
        str((deal or {}).get("status_sdr") or "").strip().lower(),
        str((deal or {}).get("automation_status") or "").strip().lower(),
    }
    if tokens & {"193", "196", "226", "respondido", "warm_whatsapp", "clicked_warm", "lead_trafego", "lead_tráfego", "conversando"}:
        return True
    if status_values & WARM_REASONS:
        return True
    if any(str(row.get("status") or "").strip().lower() in WARM_REASONS for row in email_rows):
        return True
    warm = wa_record(wa_state, deal_id, phone, "wa_warm")
    return str(warm.get("status") or "").lower() in {"active", "sent", "warm"}


def is_business_hours_brt() -> bool:
    current = now_brt()
    if current.weekday() >= 5:
        return False
    try:
        import holidays

        if current.date() in holidays.Brazil():
            return False
    except Exception:
        pass
    return 9 <= current.hour < 18


def same_local_day(left: datetime | None, right: datetime | None = None) -> bool:
    if not left:
        return False
    right = right or now_brt()
    return left.date() == right.date()


def hours_since(value: datetime | None) -> float:
    if not value:
        return 999999.0
    return (datetime.now(value.tzinfo) - value).total_seconds() / 3600


def build_action(action: str, *, reason: str, deal_id: int = 0, phone: str = "", email: str = "", **extra: Any) -> dict[str, Any]:
    payload = {
        "action": action,
        "reason": reason,
        "deal_id": deal_id,
        "phone": phone,
        "email": email,
        **extra,
    }
    print(
        "[ORCH_DECISION]",
        "deal_id=", deal_id,
        "action=", action,
        "reason=", reason,
        "strategy=", payload.get("strategy", ""),
        "step=", payload.get("step", ""),
    )
    return payload


def resolve_next_action(
    deal: dict[str, Any] | None,
    person: dict[str, Any] | None = None,
    org: dict[str, Any] | None = None,
    email_state: list[dict[str, Any]] | None = None,
    wa_state: dict[str, Any] | None = None,
    *,
    channel: str = "",
    intent: str = "",
    phone: str = "",
    email: str = "",
    allow_explicit_hunter_in_nutrition: bool | None = None,
) -> dict[str, Any]:
    email_state = email_state if isinstance(email_state, list) else load_email_state()
    wa_state = wa_state if isinstance(wa_state, dict) else load_wa_state()

    deal_id = deal_id_of(deal)
    person_id = person_id_of(deal, person)
    org_id = org_id_of(deal, org)
    phone_value = phone_from(deal, person, phone)
    email_value = email_from(deal, person, email)
    stage_id = stage_id_of(deal)
    stage_name = stage_name_of(deal)
    tokens = label_tokens(deal)
    status = str((deal or {}).get("status") or "open").strip().lower()
    email_rows = queue_rows_for_deal(email_state, deal_id)
    active_email = active_email_rows(email_state, deal_id)
    hunter = wa_record(wa_state, deal_id, phone_value, "wa_hunter")
    warm = wa_record(wa_state, deal_id, phone_value, "wa_warm")
    last_email_at = last_email_sent_at(email_state, deal_id)
    last_wa_at = last_wa_sent_at(wa_state, deal_id, phone_value)
    intent_value = str(intent or "").strip().lower()
    channel_value = str(channel or "").strip().lower()

    base = {
        "person_id": person_id,
        "org_id": org_id,
        "stage_id": stage_id,
        "stage_name": stage_name,
        "labels": sorted(tokens),
        "email_active": bool(active_email),
        "hunter_status": hunter.get("status"),
        "warm_status": warm.get("status"),
    }

    if deal_id <= 0:
        return build_action(ACTION_HOLD, reason="invalid_deal", deal_id=deal_id, phone=phone_value, email=email_value, **base)
    if status in {"won", "lost"}:
        return build_action(ACTION_STOP_ALL, reason=status, deal_id=deal_id, phone=phone_value, email=email_value, **base)
    if intent_value in {"negative_stop", "sem_interesse", "opt_out"}:
        return build_action(ACTION_MARK_LOST, reason="negative_stop", deal_id=deal_id, phone=phone_value, email=email_value, stop_all=True, **base)
    if intent_value in {"wrong_contact", "wrong_number", "numero_errado"}:
        return build_action(ACTION_STOP_ALL, reason="wrong_contact", deal_id=deal_id, phone=phone_value, email=email_value, block_phone=True, **base)
    if intent_value in {"menu_bot", "bot", "bot_menu"}:
        return build_action(ACTION_STOP_ALL, reason="menu_bot", deal_id=deal_id, phone=phone_value, email=email_value, block_phone=True, no_lost=True, **base)
    if intent_value in {"referral", "indicacao"}:
        return build_action(ACTION_STOP_ALL, reason="referral", deal_id=deal_id, phone=phone_value, email=email_value, create_referral=True, **base)

    warm_signal = has_warm_signal(deal, email_rows, wa_state, deal_id, phone_value) or intent_value in {"positive_interest", "scheduling_time_provided"}
    if warm_signal:
        if channel_value == "wa_warm":
            return build_action(ACTION_WA_WARM_SEND, reason="warm_signal", deal_id=deal_id, phone=phone_value, email=email_value, strategy="wa_warm", stop_email=True, stop_hunter=True, **base)
        return build_action(ACTION_CREATE_ACTIVITY, reason="warm_signal", deal_id=deal_id, phone=phone_value, email=email_value, move_stage=PIPELINE_STAGES["tentativa"], stop_email=True, stop_hunter=True, **base)

    if stage_id and stage_id not in EMAIL_ALLOWED_STAGE_IDS and channel_value == "email":
        return build_action(ACTION_HOLD, reason=f"email_not_allowed_stage_{stage_id}", deal_id=deal_id, phone=phone_value, email=email_value, **base)

    if channel_value == "email":
        if same_local_day(last_wa_at):
            return build_action(ACTION_HOLD, reason="wa_sent_today", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if any(keyword in stage_name for keyword in NO_COLD_STAGE_KEYWORDS):
            return build_action(ACTION_HOLD, reason="cold_blocked_by_stage", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        return build_action(ACTION_EMAIL_SEND, reason="email_stage_allowed", deal_id=deal_id, phone=phone_value, email=email_value, strategy="email_cadence", **base)

    if channel_value == "wa_hunter":
        if not is_business_hours_brt():
            return build_action(ACTION_HOLD, reason="outside_business_hours", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if not phone_value:
            return build_action(ACTION_HOLD, reason="missing_phone", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if tokens & IMPEDITIVE_HUNTER_TAGS:
            return build_action(ACTION_HOLD, reason="impeditive_tag", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if stage_id in EMAIL_ALLOWED_STAGE_IDS and not allow_explicit_hunter_in_nutrition:
            return build_action(ACTION_HOLD, reason="email_nutrition_active_stage", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if stage_id and stage_id not in HUNTER_ALLOWED_STAGE_IDS:
            return build_action(ACTION_HOLD, reason=f"hunter_not_allowed_stage_{stage_id}", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if same_local_day(last_email_at):
            return build_action(ACTION_HOLD, reason="email_sent_today", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        if hours_since(last_email_at) < float(os.getenv("SDR_EMAIL_TO_WA_MIN_HOURS", "24") or 24):
            return build_action(ACTION_HOLD, reason="email_to_wa_window", deal_id=deal_id, phone=phone_value, email=email_value, **base)

        steps = sent_steps(hunter, "hunter")
        last_hunter_at = utcish_parse(hunter.get("last_sent_at"))
        if "hunter_1" not in steps:
            return build_action(ACTION_WA_HUNTER_SEND, reason="hunter_step_due", deal_id=deal_id, phone=phone_value, email=email_value, strategy="wa_hunter", step=1, step_name="hunter_1", **base)
        if "hunter_2" not in steps:
            if hours_since(last_hunter_at) < 24:
                return build_action(ACTION_HOLD, reason="hunter_2_not_due", deal_id=deal_id, phone=phone_value, email=email_value, **base)
            return build_action(ACTION_WA_HUNTER_SEND, reason="hunter_step_due", deal_id=deal_id, phone=phone_value, email=email_value, strategy="wa_hunter", step=2, step_name="hunter_2", **base)
        if "hunter_3" not in steps:
            if hours_since(last_hunter_at) < 24:
                return build_action(ACTION_HOLD, reason="hunter_3_not_due", deal_id=deal_id, phone=phone_value, email=email_value, **base)
            return build_action(ACTION_WA_HUNTER_SEND, reason="hunter_step_due", deal_id=deal_id, phone=phone_value, email=email_value, strategy="wa_hunter", step=3, step_name="hunter_3", **base)
        if hours_since(last_hunter_at) >= 24:
            return build_action(ACTION_MARK_LOST, reason="no_response_after_wa_hunter", deal_id=deal_id, phone=phone_value, email=email_value, strategy="wa_hunter", **base)
        return build_action(ACTION_HOLD, reason="hunter_completed_waiting_drop_window", deal_id=deal_id, phone=phone_value, email=email_value, **base)

    if channel_value == "wa_warm":
        if not warm_signal:
            return build_action(ACTION_HOLD, reason="missing_warm_signal", deal_id=deal_id, phone=phone_value, email=email_value, **base)
        return build_action(ACTION_WA_WARM_SEND, reason="warm_allowed", deal_id=deal_id, phone=phone_value, email=email_value, strategy="wa_warm", stop_email=True, stop_hunter=True, **base)

    return build_action(ACTION_HOLD, reason="no_channel_action", deal_id=deal_id, phone=phone_value, email=email_value, **base)


def stop_cold_states_for_warm(deal_id: int, phone: str, email_rows: list[dict[str, Any]] | None = None, wa_state: dict[str, Any] | None = None, *, reason: str = "warm") -> tuple[bool, bool]:
    changed_email = False
    changed_wa = False
    if email_rows is not None:
        terminal_email_statuses = STOP_REASONS | WARM_REASONS | {"done"}
        for row in email_rows:
            if int(row.get("deal_id") or 0) == int(deal_id or 0) and str(row.get("status") or "").lower() not in terminal_email_statuses:
                row["status"] = "warm"
                row["stopped_at"] = now_brt().isoformat(timespec="seconds")
                row["stop_reason"] = reason
                changed_email = True
    if wa_state is not None:
        phone_value = normalize_phone(phone)
        for key, record in list(wa_state.items()):
            if not isinstance(record, dict):
                continue
            if int(record.get("deal_id") or 0) == int(deal_id or 0) and normalize_phone(record.get("phone")) == phone_value and str(record.get("strategy")) == "wa_hunter":
                if record.get("status") not in {"warm", "stopped", "blocked", "completed"}:
                    record["status"] = "warm"
                    record["reason"] = reason
                    record["stopped_at"] = now_brt().isoformat(timespec="seconds")
                    wa_state[key] = record
                    changed_wa = True
    if changed_email or changed_wa:
        print(f"[ORCH_STOP_ALL] deal_id={deal_id} phone={normalize_phone(phone)} reason={reason} email={1 if changed_email else 0} wa={1 if changed_wa else 0}")
    return changed_email, changed_wa


def find_legacy_wa_tags(deal: dict[str, Any] | None) -> set[str]:
    return label_tokens(deal) & LEGACY_WA_TAGS


def action_blocks_send(action: dict[str, Any], expected: str) -> bool:
    return str((action or {}).get("action") or "") != expected
