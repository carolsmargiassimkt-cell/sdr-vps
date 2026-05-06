STAGE_ENTRADA = 51
STAGE_TENTATIVA_CONTATO = 52
STAGE_NUTRICAO = 62
STAGE_PRONTO_PROSPECCAO = 63

STOP_INTENTS = {"negative_stop", "opt_out", "wrong_contact"}
PAUSE_INTENTS = {"pause_not_now", "soft_negative"}

def resolve_pipeline_stage(ctx: dict) -> dict:
    ctx = ctx or {}

    if ctx.get("status") in {"won", "lost"}:
        return {"action": "skip", "reason": "deal_closed"}

    intent = str(ctx.get("intent") or "").lower()
    source = str(ctx.get("source") or "").lower()

    if intent in STOP_INTENTS:
        return {"action": "lost_or_block", "stage_id": None, "reason": intent}

    if intent in PAUSE_INTENTS:
        return {"action": "pause", "stage_id": STAGE_TENTATIVA_CONTATO, "reason": intent}

    if ctx.get("meeting_scheduled") or "calendly" in source or "reuniao" in source:
        return {"action": "move", "stage_id": STAGE_PRONTO_PROSPECCAO, "reason": "meeting_scheduled"}

    if ctx.get("email_clicked") or source in {"email_click", "clicked_warm", "mailchimp_click"}:
        return {"action": "move", "stage_id": STAGE_PRONTO_PROSPECCAO, "reason": "email_click"}

    if ctx.get("form_qualified") and ctx.get("has_phone"):
        return {"action": "move", "stage_id": STAGE_PRONTO_PROSPECCAO, "reason": "qualified_form"}

    if source in {"inbound_lp", "leadster", "lp", "inbound"}:
        return {"action": "move", "stage_id": STAGE_TENTATIVA_CONTATO, "reason": "inbound_attempt_contact"}

    if ctx.get("form_responded") or "form" in source or "trafego" in source or "tráfego" in source:
        return {"action": "move", "stage_id": STAGE_TENTATIVA_CONTATO, "reason": "form_attempt_contact"}

    return {"action": "move", "stage_id": STAGE_NUTRICAO, "reason": "no_warm_evidence"}
