from __future__ import annotations

import argparse
from datetime import datetime

from core.sdr_orchestrator import (
    ACTION_CREATE_ACTIVITY,
    ACTION_MARK_LOST,
    ACTION_WA_HUNTER_SEND,
    backup_state_files,
    find_legacy_wa_tags,
    load_email_state,
    load_wa_state,
    normalize_phone,
    resolve_next_action,
    save_email_state,
    save_wa_state,
    stop_cold_states_for_warm,
)
from core.wa_strategy_state import append_sent_step, set_record
from crm.pipedrive_client import PipedriveClient


def phone_from_deal(deal):
    person = (deal or {}).get("person_id") or {}
    if isinstance(person, dict):
        for item in list(person.get("phone") or []):
            value = item.get("value") if isinstance(item, dict) else item
            if value:
                return value
    return ""


def mark_legacy_sent_steps(wa_state, deal, phone):
    tags = find_legacy_wa_tags(deal)
    if not tags:
        return False
    deal_id = int((deal or {}).get("id") or 0)
    changed = False
    mapping = {"wa_cad4": 1, "wa_cad5": 2, "wa_cad6": 3}
    for tag, step in mapping.items():
        if tag in tags:
            append_sent_step(
                wa_state,
                deal_id,
                phone,
                "wa_hunter",
                f"hunter_{step}",
                hunter_step=step,
                status="active" if step < 3 else "completed",
                reason="legacy_tag_migrated",
                migrated_at=datetime.now().isoformat(timespec="seconds"),
            )
            changed = True
    return changed


def create_human_activity(crm, deal):
    deal_id = int((deal or {}).get("id") or 0)
    person = (deal or {}).get("person_id") or {}
    org = (deal or {}).get("org_id") or {}
    person_id = int((person.get("value") if isinstance(person, dict) else person) or 0)
    org_id = int((org.get("value") if isinstance(org, dict) else org) or 0)
    ok = crm.create_activity(
        subject=f"SDR humano - deal {deal_id}",
        type="call",
        deal_id=deal_id,
        person_id=person_id,
        org_id=org_id,
        done=0,
        note="Criada por reconciliacao SDR: lead warm/respondeu, cadencias frias paradas.",
    )
    print(f"[ORCH_ACTIVITY] deal_id={deal_id} ok={1 if ok else 0}")
    return ok


def mark_drop_lost(crm, deal_id):
    ok = bool(crm.update_deal(int(deal_id), {"status": "lost", "lost_reason": "Sem resposta apos cadencia WhatsApp Hunter"}))
    crm.add_note(deal_id=int(deal_id), content="[ORCH_DROP_LOST] Perdido por sem resposta apos WA Hunter 3/3.")
    print(f"[ORCH_DROP_LOST] deal_id={deal_id} ok={1 if ok else 0}")
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="aplica escrita local e CRM; sem esta flag e sempre dry-run")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    dry_run = not args.apply
    crm = PipedriveClient()
    email_state = load_email_state()
    wa_state = load_wa_state()
    deals = crm.get_deals(status="open", pipeline_id=7, limit=max(1, args.limit))
    if not dry_run:
        backups = backup_state_files()
        print(f"[ORCH_BACKUP] files={backups}")

    changed_email = False
    changed_wa = False
    stats = {
        "analisados": 0,
        "legacy_tags_migradas": 0,
        "warm_stopped": 0,
        "hunter_due": 0,
        "drop_lost": 0,
        "activities": 0,
    }

    for deal in list(deals or []):
        stats["analisados"] += 1
        deal_id = int((deal or {}).get("id") or 0)
        phone = normalize_phone(phone_from_deal(deal))
        if mark_legacy_sent_steps(wa_state, deal, phone):
            stats["legacy_tags_migradas"] += 1
            changed_wa = True
            print(f"[ORCH_RECONCILE] deal_id={deal_id} action=migrate_legacy_wa_tags dry_run={1 if dry_run else 0}")

        warm_decision = resolve_next_action(deal, email_state=email_state, wa_state=wa_state, channel="wa_warm", phone=phone)
        if warm_decision.get("stop_email") or warm_decision.get("stop_hunter") or warm_decision.get("action") == ACTION_CREATE_ACTIVITY:
            email_changed, wa_changed = stop_cold_states_for_warm(deal_id, phone, email_state, wa_state, reason="reconcile_warm")
            changed_email = changed_email or email_changed
            changed_wa = changed_wa or wa_changed
            stats["warm_stopped"] += 1
            print(f"[ORCH_RECONCILE] deal_id={deal_id} action=stop_cold_for_warm dry_run={1 if dry_run else 0}")
            if not dry_run:
                create_human_activity(crm, deal)
                stats["activities"] += 1

        hunter_decision = resolve_next_action(deal, email_state=email_state, wa_state=wa_state, channel="wa_hunter", phone=phone)
        if hunter_decision.get("action") == ACTION_WA_HUNTER_SEND:
            stats["hunter_due"] += 1
            print(f"[ORCH_RECONCILE] deal_id={deal_id} action=hunter_due step={hunter_decision.get('step')} dry_run=1")
        elif hunter_decision.get("action") == ACTION_MARK_LOST:
            stats["drop_lost"] += 1
            print(f"[ORCH_RECONCILE] deal_id={deal_id} action=drop_lost dry_run={1 if dry_run else 0}")
            if not dry_run:
                mark_drop_lost(crm, deal_id)
                set_record(wa_state, deal_id, phone, "wa_hunter", status="completed", reason="dropped_lost")
                changed_wa = True

    if not dry_run:
        if changed_email:
            save_email_state(email_state)
        if changed_wa:
            save_wa_state(wa_state)
    print("[ORCH_RECONCILE_SUMMARY]")
    for key, value in stats.items():
        print(f"{key}={value}")
    print(f"dry_run={1 if dry_run else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
