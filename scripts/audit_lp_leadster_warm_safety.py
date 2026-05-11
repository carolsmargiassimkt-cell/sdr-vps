#!/usr/bin/env python3
"""Read-only audit for LP/Leadster import and WhatsApp warm safety.


def is_paused_or_stopped_record(rec):
    return bool(
        rec.get("stopped")
        or rec.get("paused")
        or rec.get("do_not_advance_without_inbound")
        or str(rec.get("stop_reason") or "").startswith("blocked_step2_without_real_inbound")
    )

This script does not call Pipedrive, IMAP or the WhatsApp gateway. It checks
local state/logs and exits non-zero only when it finds evidence that step 2+
was already advanced without a real inbound reply.
"""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from email_inbox.import_email_leads import extract_lead_info, is_allowed_lead_email, normalize_phone
from scripts.whatsapp_warm_cadence import (
    has_real_inbound_after_wa1,
    is_valid_warm_phone,
    phone_clean,
)


DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"


def is_paused_or_stopped_record(rec):
    return bool(
        rec.get("stopped")
        or rec.get("paused")
        or rec.get("do_not_advance_without_inbound")
        or str(rec.get("stop_reason") or "").startswith(
            "blocked_step2_without_real_inbound"
        )
    )



def load_json(path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print("[AUDIT_WARNING]", "json_read_failed", str(path), str(exc))
    return default


def iter_recent_email_log_lines(limit=5000):
    path = LOG_DIR / "email_leads_import.log"
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    return lines[-limit:]


def audit_email_gate():
    samples = [
        ("Leadster <leads@leadster.com.br>", "Novo lead gerado"),
        ("comercial@manddigital.com.br", "Formulário LP"),
        ("comercial@manddigital.com.br", "Formulario LP"),
        ("comercial@manddigital.com.br", "Home"),
        ("comercial@manddigital.com.br", "Curriculo"),
        ("facebook@facebookmail.com", "Novo lead"),
    ]
    accepted = []
    rejected = []
    for sender, subject in samples:
        item = {"sender": sender, "subject": subject}
        if is_allowed_lead_email(sender, subject):
            accepted.append(item)
        else:
            rejected.append(item)
    return accepted, rejected


def audit_phone_parser():
    andrielle_text = (
        "Nome: Andriele\n"
        "Email: andriele@example.com\n"
        "Telefone: (55) 98421-5377 480100100100480100115768114801155\n"
        "Empresa: Teste"
    )
    cases = {
        "andrielle_html_noise": extract_lead_info(andrielle_text, "comercial@manddigital.com.br").get("phone"),
        "absurd_concat": "480100100100480100115768114801155598421537710052026152946480010000",
        "0800": "0800 123 4567",
        "valid_no_ddi": "(11) 99384-9075",
    }
    return {name: value if name == "andrielle_html_noise" else normalize_phone(value) for name, value in cases.items()}


def audit_warm_state():
    warm_state = load_json(DATA_DIR / "whatsapp_warm_cadence.json", {})
    conversation_state = load_json(DATA_DIR / "whatsapp_conversation_state.json", {})
    sdr_state = load_json(DATA_DIR / "sdr_state.json", {})
    results = {
        "total_state_records": 0,
        "invalid_phone": [],
        "step1_sent": [],
        "blocked_step2_without_inbound": [],
        "violations_step2_already_sent_without_inbound": [],
    }
    if not isinstance(warm_state, dict):
        return results
    for key, rec in warm_state.items():
        if key == "stopped" or not isinstance(rec, dict):
            continue
        deal_id = str(rec.get("deal_id") or key)
        phone = rec.get("phone") or ""
        step = int(rec.get("step") or rec.get("last_sent_step_whatsapp") or 0)
        results["total_state_records"] += 1
        base = {"deal_id": deal_id, "phone": phone_clean(phone), "step": step}
        if phone and not is_valid_warm_phone(phone):
            results["invalid_phone"].append(base)
        if step >= 1 or rec.get("wa1_sent"):
            results["step1_sent"].append(base)
        has_inbound = has_real_inbound_after_wa1(rec, deal_id, phone, conversation_state, sdr_state)
        if step == 1 and not has_inbound:
            results["blocked_step2_without_inbound"].append(base)
        if step >= 2 and not has_inbound and not is_paused_or_stopped_record(rec):
            results["violations_step2_already_sent_without_inbound"].append(base)
    return results


def audit_logs():
    counters = {
        "email_ruido": 0,
        "lead_parse_ok": 0,
        "deal_create_ok": 0,
        "wa_queued": 0,
        "invalid_phone_field": 0,
    }
    for line in iter_recent_email_log_lines():
        low = line.lower()
        if "[email_ruido]" in low:
            counters["email_ruido"] += 1
        if "[lead_parse_ok]" in low:
            counters["lead_parse_ok"] += 1
        if "[deal_create_ok]" in low:
            counters["deal_create_ok"] += 1
        if "[wa_warm_queued]" in low:
            counters["wa_queued"] += 1
        if "[lead_invalid_phone_field]" in low:
            counters["invalid_phone_field"] += 1
    return counters


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    accepted, rejected = audit_email_gate()
    phone_parser = audit_phone_parser()
    warm = audit_warm_state()
    logs = audit_logs()
    payload = {
        "email_gate_accepted_samples": accepted,
        "email_gate_rejected_samples": rejected,
        "phone_parser": phone_parser,
        "warm_state": warm,
        "email_import_log_counters": logs,
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print("[AUDIT_LP_LEADSTER_WARM]")
        print("accepted_samples=", len(accepted), "rejected_samples=", len(rejected))
        print("phone_parser=", phone_parser)
        print("warm_total=", warm["total_state_records"])
        print("step1_sent=", len(warm["step1_sent"]))
        print("invalid_phone=", len(warm["invalid_phone"]))
        print("blocked_step2_without_inbound=", len(warm["blocked_step2_without_inbound"]))
        print("violations_step2_already_sent_without_inbound=", len(warm["violations_step2_already_sent_without_inbound"]))
        print("email_import_log_counters=", logs)

    if warm["violations_step2_already_sent_without_inbound"]:
        print("[AUDIT_CRITICAL] step2_or_more_without_real_inbound")
        return 1
    print("[AUDIT_OK] warm_step2_guard_effective")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
