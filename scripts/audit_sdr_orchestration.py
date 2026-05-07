from __future__ import annotations

from collections import Counter

from core.sdr_orchestrator import (
    ACTION_CREATE_ACTIVITY,
    ACTION_MARK_LOST,
    ACTION_WA_HUNTER_SEND,
    find_legacy_wa_tags,
    load_email_state,
    load_wa_state,
    resolve_next_action,
)
from crm.pipedrive_client import PipedriveClient


def phone_from_deal(deal):
    person = (deal or {}).get("person_id") or {}
    if isinstance(person, dict):
        for item in list(person.get("phone") or []):
            value = item.get("value") if isinstance(item, dict) else item
            if value:
                return value
    return ""


def state_has_today_conflict(email_rows, wa_state, deal_id):
    from core.sdr_orchestrator import last_email_sent_at, last_wa_sent_at, same_local_day

    return bool(same_local_day(last_email_sent_at(email_rows, deal_id)) and same_local_day(last_wa_sent_at(wa_state, deal_id)))


def main():
    crm = PipedriveClient()
    email_state = load_email_state()
    wa_state = load_wa_state()
    deals = crm.get_deals(status="open", pipeline_id=7, limit=1000)
    counts = Counter()
    examples = {
        "email-only": [],
        "wa-hunter": [],
        "warm": [],
        "drop-candidate": [],
        "conflict email+WA": [],
    }

    for deal in list(deals or []):
        deal_id = int((deal or {}).get("id") or 0)
        phone = phone_from_deal(deal)
        counts["total_deals_analisados"] += 1
        if any(int(row.get("deal_id") or 0) == deal_id and str(row.get("status") or "").lower() == "pending" for row in email_state):
            counts["email_active"] += 1
            if len(examples["email-only"]) < 10:
                examples["email-only"].append(deal_id)

        hunter_decision = resolve_next_action(deal, email_state=email_state, wa_state=wa_state, channel="wa_hunter", phone=phone)
        if hunter_decision.get("action") == ACTION_WA_HUNTER_SEND:
            step = int(hunter_decision.get("step") or 0)
            counts["wa_hunter_active"] += 1
            counts[f"deveriam_mandar_hunter_{step}"] += 1
            if len(examples["wa-hunter"]) < 10:
                examples["wa-hunter"].append({"deal_id": deal_id, "step": step})
        elif hunter_decision.get("action") == ACTION_MARK_LOST:
            counts["deveriam_dropar_perdido"] += 1
            if len(examples["drop-candidate"]) < 10:
                examples["drop-candidate"].append(deal_id)

        warm_decision = resolve_next_action(deal, email_state=email_state, wa_state=wa_state, channel="wa_warm", phone=phone)
        if warm_decision.get("action") in {ACTION_CREATE_ACTIVITY, "WA_WARM_SEND"} or warm_decision.get("stop_hunter"):
            counts["warm"] += 1
            counts["deveriam_mover_tentativa"] += 1
            if len(examples["warm"]) < 10:
                examples["warm"].append(deal_id)

        if state_has_today_conflict(email_state, wa_state, deal_id):
            counts["conflitos_email_wa_mesmo_dia"] += 1
            if len(examples["conflict email+WA"]) < 10:
                examples["conflict email+WA"].append(deal_id)

        if find_legacy_wa_tags(deal):
            counts["deals_com_tags_antigas"] += 1

    try:
        open_deal_ids = {int((deal or {}).get("id") or 0) for deal in list(deals or [])}
        for activity in crm.get_activities(limit=500):
            raw_deal = (activity or {}).get("deal_id")
            activity_deal_id = int((raw_deal or {}).get("value") if isinstance(raw_deal, dict) else raw_deal or 0)
            if activity_deal_id and activity_deal_id not in open_deal_ids:
                counts["atividades_possivelmente_desconexas"] += 1
    except Exception as exc:
        print(f"[ORCH_BLOCK] reason=activity_audit_fail error={exc}")

    print("[AUDIT_SDR_ORCH_SUMMARY]")
    for key in sorted(counts):
        print(f"{key}={counts[key]}")
    print("[AUDIT_SDR_ORCH_EXAMPLES]")
    for key, rows in examples.items():
        print(f"{key}: {rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
