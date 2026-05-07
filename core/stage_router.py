STAGE_ENTRADA = 51
STAGE_TENTATIVA_CONTATO = 52
STAGE_NUTRICAO = 62
STAGE_PRONTO_PROSPECCAO = 63

STOP_INTENTS = {"negative_stop", "opt_out", "wrong_contact"}
PAUSE_INTENTS = {"pause_not_now", "soft_negative"}


def _route(action, stage_id=None, reason=""):
    route = {"action": action, "reason": reason}
    if stage_id is not None:
        route["stage_id"] = stage_id
    print("[STAGE_ROUTE_DECISION]", route)
    return route


def resolve_pipeline_stage(ctx: dict) -> dict:
    ctx = ctx or {}

    if ctx.get("status") in {"won", "lost"}:
        return _route("skip", reason="deal_closed")

    intent = str(ctx.get("intent") or "").lower()
    source = str(ctx.get("source") or "").lower()

    if intent in STOP_INTENTS:
        return _route("lost_or_block", reason=intent)

    if intent in PAUSE_INTENTS:
        return _route("pause", STAGE_TENTATIVA_CONTATO, intent)

    if ctx.get("meeting_scheduled") or "calendly" in source or "reuniao" in source or "reunião" in source:
        return _route("move", STAGE_TENTATIVA_CONTATO, "meeting_scheduled")

    if ctx.get("email_clicked") or source in {"email_click", "clicked_warm", "mailchimp_click"}:
        return _route("move", STAGE_TENTATIVA_CONTATO, "email_click")

    if ctx.get("form_qualified") and ctx.get("has_phone"):
        return _route("move", STAGE_TENTATIVA_CONTATO, "qualified_form")

    if source in {"inbound_lp", "leadster", "lp", "inbound"}:
        return _route("move", STAGE_TENTATIVA_CONTATO, "inbound_attempt_contact")

    if ctx.get("form_responded") or "form" in source or "trafego" in source or "tráfego" in source:
        return _route("move", STAGE_TENTATIVA_CONTATO, "form_attempt_contact")

    return _route("move", STAGE_NUTRICAO, "no_warm_evidence")
