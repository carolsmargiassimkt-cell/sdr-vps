from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import whatsapp_warm_cadence as warm


def explain_skip(deal, state, blocked, meeting_state, business_hours: bool) -> tuple[str, str, int, str]:
    deal_id = str(deal.get("id") or "")
    phone = deal.get("_phone") or warm.get_phone(deal)
    rec = state.get(deal_id, {}) if isinstance(state, dict) else {}
    current_step = int(rec.get("step") or 0)
    next_step = current_step + 1

    if not business_hours:
        return "skip", "outside_business_hours", next_step, ""
    if deal.get("_skip_reason") == "blocklist":
        return "skip", "blocklist", next_step, ""
    if deal.get("_skip_reason") == "dup_batch":
        return "skip", "phone_dup_batch", next_step, ""
    if not phone:
        return "skip", "missing_phone", next_step, ""
    if warm.is_blocked_phone(phone, blocked):
        return "skip", "blocklist", next_step, ""
    if warm.is_auto_reply_blocked(phone):
        return "skip", "auto_reply_blocked", next_step, ""
    stopped_index = state.get("stopped") if isinstance(state.get("stopped"), dict) else {}
    if deal_id in stopped_index or f"phone:{phone}" in stopped_index:
        return "skip", "stopped_index", next_step, ""
    if rec.get("stopped"):
        return "skip", "state_stopped", next_step, ""
    if next_step > warm.MAX_WARM_STEP:
        return "skip", "cadence_done", next_step, ""
    has_meeting, meeting_reason = warm.deal_has_meeting_booked(deal_id, phone, meeting_state)
    if has_meeting:
        return "skip", f"meeting_booked:{meeting_reason}", next_step, ""
    sent_today, last_sent_at, reason = warm.already_sent_today(state, deal_id, phone)
    if sent_today:
        return "skip", f"already_sent_today:{reason}:{last_sent_at}", next_step, ""
    if not warm.due_for_step(rec, next_step):
        return "skip", "not_due_yet", next_step, ""

    labs = warm.labels_of(deal)
    origin = "trafego" if warm.LABEL_LEAD_TRAFEGO in labs else "warm"
    message = warm.render_warm_message(deal, origin, next_step, phone)
    return "would_send", "due", next_step, message


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run read-only audit for WhatsApp warm cadence.")
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()

    state = warm.load_state()
    meeting_state = warm.load_meeting_state()
    blocked = warm.load_blocklist()
    business_hours = warm.is_brazil_business_hours()
    deals = warm.select_batch_deals(warm.get_open_deals(), blocked)

    print(f"[WA_WARM_AUDIT] total_targets={len(deals)} business_hours={business_hours} max_step={warm.MAX_WARM_STEP}")
    shown = 0
    for deal in deals:
        if shown >= args.limit:
            break
        deal_id = str(deal.get("id") or "")
        phone = deal.get("_phone") or warm.get_phone(deal)
        action, reason, next_step, message = explain_skip(deal, state, blocked, meeting_state, business_hours)
        preview = " ".join(str(message or "").split())[:220]
        print(
            "[WA_WARM_AUDIT_ROW]",
            f"deal_id={deal_id}",
            f"phone={phone or '-'}",
            f"current_step={int((state.get(deal_id, {}) or {}).get('step') or 0)}",
            f"next_step={next_step}",
            f"action={action}",
            f"reason={reason}",
            f"preview={preview or '-'}",
        )
        shown += 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
