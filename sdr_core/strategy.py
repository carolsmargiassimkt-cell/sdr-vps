from datetime import datetime, timezone

BLOCK_HUNTER_TAGS = {
    "conversando",
    "respondido",
    "indicação",
    "indicacao",
    "caro evento",
    "linkedin",
    "lead trafego",
    "lead_trafego",
    "menu_bot",
    "whatsapp_bot",
    "sem_interesse",
    "numero_errado",
}

WARM_STAGES = {
    "Conexão Realizada",
    "Conexao Realizada",
    "Qualificado",
    "Reunião Agendada",
    "Reuniao Agendada",
    "Diagnóstico",
    "Diagnostico",
    "Proposta",
}

MEETING_STAGES = {
    "Reunião Agendada",
    "Reuniao Agendada",
}

COMPLETED_STAGES = {
    "Won",
    "Ganho",
    "Perdido",
}

POSITIVE_INTENTS = {
    "positive_interest",
    "lead_interessado",
    "interessado",
    "replied_positive",
    "meeting_requested",
}

BLOCK_INTENTS = {
    "negative_stop",
    "sem_interesse",
    "wrong_contact",
    "numero_errado",
    "menu_bot",
    "bot_menu",
}


def norm(v):
    return str(v or "").strip().lower()


def get_tags(deal):
    tags = deal.get("tags") or deal.get("labels") or []
    if isinstance(tags, str):
        tags = [x.strip() for x in tags.replace(";", ",").split(",") if x.strip()]
    return {norm(x) for x in tags}


def get_field(deal, key, default=None):
    return deal.get(key, default)


def resolve_sdr_strategy(deal):
    """
    Decide quem controla o lead:
    - hunter
    - warm
    - inbound_warm
    - meeting
    - blocked
    - paused
    - completed

    Pipeline = estado macro.
    Tags = bloqueios/contexto.
    Campos = inteligência.
    """
    stage = deal.get("stage_name") or deal.get("stage") or deal.get("etapa") or ""
    status = norm(deal.get("status"))
    tags = get_tags(deal)

    lead_score = int(float(deal.get("lead_score") or 0))
    sdr_status = norm(deal.get("Status SDR") or deal.get("status_sdr"))
    source = norm(deal.get("sdr_lead_source"))
    warm_source = norm(deal.get("sdr_warm_source"))
    last_intent = norm(deal.get("sdr_last_intent"))
    automation_status = norm(deal.get("sdr_automation_status"))
    meeting_status = norm(deal.get("sdr_meeting_status"))

    reasons = []

    if status in {"won", "lost", "ganho", "perdido"} or stage in COMPLETED_STAGES:
        return "completed", ["deal_finalizado"]

    if tags & {norm(x) for x in BLOCK_HUNTER_TAGS}:
        reasons.append("tag_impeditiva")

    if last_intent in BLOCK_INTENTS or sdr_status in BLOCK_INTENTS:
        return "blocked", reasons + ["intent_bloqueio"]

    if "lead trafego" in tags or "lead_trafego" in tags or source in {"trafego", "tráfego", "form", "forms", "inbound"}:
        return "inbound_warm", reasons + ["lead_inbound_trafego"]

    if stage in MEETING_STAGES or meeting_status in {"agendada", "confirmada", "confirmed"}:
        return "meeting", reasons + ["reuniao"]

    if (
        stage in WARM_STAGES
        or warm_source
        or last_intent in POSITIVE_INTENTS
        or sdr_status in {"lead_interessado", "respondido", "conversando"}
        or automation_status == "warm"
    ):
        return "warm", reasons + ["sinal_warm"]

    if automation_status in {"paused", "pausado"}:
        return "paused", reasons + ["automacao_pausada"]

    if reasons:
        return "blocked", reasons

    if lead_score >= 50:
        return "hunter", ["score_ok"]

    return "paused", ["score_baixo"]


def build_sdr_field_updates(deal, event=None):
    """
    Campos que sobraram e devem ser atualizados.
    Não escreve campos deprecated.
    """
    event = event or {}
    strategy, reasons = resolve_sdr_strategy({**deal, **event})

    updates = {
        "sdr_automation_status": strategy,
        "sdr_last_intent": event.get("intent") or deal.get("sdr_last_intent"),
        "sdr_last_touch_channel": event.get("channel") or deal.get("sdr_last_touch_channel"),
        "sdr_warm_source": event.get("warm_source") or deal.get("sdr_warm_source"),
        "sdr_lead_source": event.get("lead_source") or deal.get("sdr_lead_source"),
        "sdr_conversation_phase": event.get("conversation_phase") or strategy,
        "sdr_meeting_status": event.get("meeting_status") or deal.get("sdr_meeting_status"),
        "ultimo_contato": event.get("contact_at") or datetime.now(timezone.utc).isoformat(),
        "canal_ultimo_contato": event.get("channel") or deal.get("canal_ultimo_contato"),
    }

    return {k: v for k, v in updates.items() if v not in (None, "")}, reasons
