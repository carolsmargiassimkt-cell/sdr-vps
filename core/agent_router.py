from core.agent_state import route_intent, set_state, should_block_cadence

CRM_BLOCK_TAGS = {
    "RESPONDIDO",
    "CONVERSANDO",
    "SEM_INTERESSE",
    "NUMERO_ERRADO",
    "CONTATO_INDICADO",
    "LEAD_ENCERRADO",
    "LEAD_TRÁFEGO",
    "LEAD_TRAFEGO",
    "LINKEDIN",
    "INDICAÇÃO CAROL EVENTO",
    "INDICACAO CAROL EVENTO",
}

def normalize_tags(tags):
    return {str(t or "").strip().upper() for t in list(tags or []) if str(t or "").strip()}

def can_enter_cadence(phone=None, deal_id=None, tags=None):
    tokens = normalize_tags(tags)
    if tokens & CRM_BLOCK_TAGS:
        return False, "blocked_crm_tag"
    if should_block_cadence(phone=phone, deal_id=deal_id):
        return False, "blocked_agent_state"
    return True, "ok"

def register_inbound(phone=None, deal_id=None, intent=None, message=None):
    route = route_intent(intent)
    state = route["state"]
    action = route["action"]
    set_state(
        phone=phone,
        deal_id=deal_id,
        state=state,
        intent=intent,
        summary=message,
        extra={"last_action": action},
    )
    return route
