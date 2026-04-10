import requests
import time
import random
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

from core.lead_state import LeadStateStore
from core.pipedrive_client import PipedriveClient
from services.pipeline_stage_sync import sync_pipeline_stage_by_pair
from runtime.daily_limit import DailyLimitStore
from runtime.process_lock import ProcessAlreadyRunningError, ProcessLock

PIPEDRIVE_API = os.getenv("PIPEDRIVE_API_TOKEN")
N8N_EMAIL_WEBHOOK_PATH = "sdr-email"
N8N_EMAIL_WEBHOOK_URL = f"http://127.0.0.1:5678/webhook/{N8N_EMAIL_WEBHOOK_PATH}"
EMAIL_SUBJECT = "Gera��o de clientes"
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LEAD_STATE = LeadStateStore(os.path.join(PROJECT_ROOT, "data", "lead_state.json"))
DAILY_LIMITS = DailyLimitStore(Path(PROJECT_ROOT) / "logs" / "daily_send_limits.json")
CRM = PipedriveClient()
MAX_CADENCE_DAY = 6
CHANNEL_DELAY_DAYS = {step: 5 for step in range(1, MAX_CADENCE_DAY + 1)}
BLOCKING_STATUSES = {"conversando", "conversation", "sem_interesse", "encerrado", "stop", "failed"}
DAILY_EMAIL_LIMIT = 50
EMAIL_TAG_PREFIX = "EMAIL_CAD"
EMAIL_CRM_LABELS = [f"{EMAIL_TAG_PREFIX}{idx}" for idx in range(1, MAX_CADENCE_DAY + 1)]
WORKER_LOCK_NAME = "email_worker_main"
AUTHORIZED_PROCESS_NAME = "email_worker.py"
DRY_RUN = str(os.getenv("DRY_RUN", "")).strip().lower() in {"1", "true", "yes", "on"}

# =========================
# BUSCAR LEADS COM PAGINA��O
# =========================
def buscar_leads(limit=30):
    leads = []
    start = 0

    while len(leads) < limit:
        url = f"https://api.pipedrive.com/v1/persons?start={start}&limit=50&api_token={PIPEDRIVE_API}"
        r = requests.get(url)

        if r.status_code != 200:
            print("[ERROR] Falha ao buscar leads")
            break

        data = r.json().get("data", [])
        if not data:
            break

        for p in data:
            email = None

            if p.get("email"):
                email = p["email"][0]["value"]

            if not email:
                continue

            leads.append({
                "id": p["id"],
                "nome": p["name"],
                "email": email,
                "label": p.get("label", []),
            })

            if len(leads) >= limit:
                break

        start += 50

    return leads


def garantir_labels_email():
    ok = CRM.ensure_person_labels(EMAIL_CRM_LABELS)
    if ok:
        print("[CRM_LABELS_OK] EMAIL_CAD1..6")
    else:
        print("[CRM_LABELS_FAIL] EMAIL_CAD1..6")
    return ok

# =========================
# IDPOT�NCIA FORTE
# =========================
def ja_enviado(person_id, cadence_step):
    url = f"https://api.pipedrive.com/v1/notes?person_id={person_id}&api_token={PIPEDRIVE_API}"
    r = requests.get(url)

    if r.status_code != 200:
        return False

    notes = r.json().get("data", [])

    marker = f"EMAIL_SENT_SDR|CAD{int(cadence_step or 0)}"

    for note in notes:
        if marker in str(note.get("content", "") or ""):
            return True

    return False


def etapas_enviadas(person_id):
    url = f"https://api.pipedrive.com/v1/notes?person_id={person_id}&api_token={PIPEDRIVE_API}"
    r = requests.get(url)
    if r.status_code != 200:
        return set()
    notes = r.json().get("data", [])
    found = set()
    for note in notes:
        content = str(note.get("content", "") or "")
        for stage in range(1, MAX_CADENCE_DAY + 1):
            if f"EMAIL_SENT_SDR|CAD{stage}" in content:
                found.add(stage)
    return found


def _label_tokens(raw_labels):
    items = raw_labels if isinstance(raw_labels, list) else [raw_labels]
    tokens = set()
    for item in items:
        if isinstance(item, dict):
            candidates = (item.get("name"), item.get("label"), item.get("value"), item.get("text"), item.get("id"))
        else:
            candidates = (item,)
        for candidate in candidates:
            clean = str(candidate or "").strip().upper()
            if clean:
                tokens.add(clean)
    return tokens


def buscar_persona(person_id):
    return CRM.get_person_details(int(person_id or 0)) if int(person_id or 0) else {}


def crm_email_step(lead):
    person = buscar_persona(int(lead.get("id", 0) or 0))
    if person:
        lead["label"] = person.get("label", [])
    tags = PipedriveClient.resolve_label_tokens(lead.get("label", []), CRM.get_person_labels())
    last_stage = 0
    for stage in range(1, MAX_CADENCE_DAY + 1):
        if f"{EMAIL_TAG_PREFIX}{stage}" in tags:
            last_stage = stage
    return min(MAX_CADENCE_DAY + 1, last_stage + 1) if last_stage else 1


def crm_has_email_stage_tag(lead, cadence_step):
    person = buscar_persona(int(lead.get("id", 0) or 0))
    if person:
        lead["label"] = person.get("label", [])
    tags = PipedriveClient.resolve_label_tokens(lead.get("label", []), CRM.get_person_labels())
    return f"{EMAIL_TAG_PREFIX}{int(cadence_step or 0)}" in tags


def aplicar_tag_email_crm(lead, cadence_step):
    person_id = int(lead.get("id", 0) or 0)
    if not person_id:
        return False
    person = buscar_persona(person_id)
    labels = PipedriveClient.resolve_label_tokens((person or {}).get("label", []), CRM.get_person_labels())
    labels.add(f"{EMAIL_TAG_PREFIX}{int(cadence_step or 0)}")
    ok = CRM.update_person(person_id, {"label": sorted(labels)})
    if ok:
        print(f"[TAG_APLICADA] person={person_id} tag=EMAIL_CAD{int(cadence_step or 0)}")
    else:
        print(f"[CRM_TAG_FAIL] person={person_id} | tag=EMAIL_CAD{int(cadence_step or 0)}")
    return ok


def sincronizar_pipeline_por_par(lead):
    try:
        return bool(sync_pipeline_stage_by_pair(client=CRM, lead=dict(lead or {}), emit=print))
    except Exception as exc:
        print(f"[ERRO_CRM_CRITICO] operacao=pipeline_pair_sync | person={int((lead or {}).get('id', 0) or 0)} | {exc}")
        return False


def registrar_envio(person_id, email, cadence_step):
    ok = CRM.create_note(
        person_id=int(person_id or 0),
        content=f"EMAIL_SENT_SDR|CAD{int(cadence_step or 0)} | {email} | {datetime.now(timezone.utc).isoformat()}",
    )
    if not ok:
        print(f"[ERRO_CRM_CRITICO] operacao=create_note | person={int(person_id or 0)}")
    return ok


def sincronizar_tags_email_crm(leads):
    synced = 0
    for lead in leads or []:
        person_id = int(lead.get("id", 0) or 0)
        if not person_id:
            continue
        stages = sorted(etapas_enviadas(person_id))
        if not stages:
            continue
        target_stage = max(stages)
        if crm_has_email_stage_tag(lead, target_stage):
            continue
        if aplicar_tag_email_crm(lead, target_stage):
            synced += 1
    if synced:
        print(f"[CRM_TAG_SYNC] {synced}")
    return synced

# =========================
# ENVIAR EMAIL
# =========================
def gerar_mensagem(nome):
    return f"""
Ol� {nome},

Analisei sua empresa e identifiquei uma oportunidade de gera��o de clientes.

Hoje voc�s utilizam campanhas ou dependem mais de indica��o?

Se fizer sentido, posso te explicar em 2 minutos.

Abra�o,
Carol
Mand Digital
"""


def resolver_email_webhook_url():
    n8n_root = Path.home() / ".n8n"
    for candidate in (n8n_root / "database.sqlite-wal", n8n_root / "database.sqlite"):
        try:
            content = candidate.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if N8N_EMAIL_WEBHOOK_PATH in content and "n8n-nodes-base.gmail" in content:
            return N8N_EMAIL_WEBHOOK_URL
    return N8N_EMAIL_WEBHOOK_URL


def enviar_email(destino, nome, empresa="", etapa=1):
    if DRY_RUN:
        print(f"[DRY_RUN] canal=email destino={str(destino or '').strip().lower()} etapa={int(etapa or 0)}")
        return False
    payload = {
        "email": str(destino or "").strip().lower(),
        "assunto": EMAIL_SUBJECT,
        "mensagem": gerar_mensagem(nome),
    }

    try:
        response = requests.post(resolver_email_webhook_url(), json=payload, timeout=30)
        if int(response.status_code) == 200:
            print(f"[EMAIL_ENVIADO] destino={destino} etapa={int(etapa or 0)}")
            return True
        print(f"[EMAIL_ERRO_WEBHOOK] {destino} -> status={response.status_code}")
        return False
    except Exception as e:
        print(f"[EMAIL_ERRO_WEBHOOK] {destino} -> {e}")
        return False


def _dispatch_phone_for_email(lead):
    state = _email_state(lead.get("email"))
    phone = str(state.get("telefone") or state.get("phone") or "").strip()
    return LeadStateStore.normalize_phone(phone)


def _wait_email_gap():
    latest = parse_iso_datetime(LEAD_STATE.latest_dispatch_at("email"))
    if latest is None:
        return
    target_gap = random.randint(30, 90)
    elapsed = max(0, int((datetime.now(timezone.utc) - latest).total_seconds()))
    wait_seconds = max(0, target_gap - elapsed)
    if wait_seconds > 0:
        print(f"[DELAY_EMAIL] aguardando {wait_seconds}s")
        time.sleep(wait_seconds)


def enviar_email_controlado(lead, cadence_step):
    email = str(lead.get("email") or "").strip().lower()
    if not email:
        return False
    if has_reply_priority(email):
        print(f"[SKIP] Respondeu -> {email}")
        return False
    if has_blocking_status(email):
        print(f"[SKIP] Status bloqueante -> {email}")
        return False
    if has_recent_interaction(email):
        print(f"[SKIP] Interacao recente -> {email}")
        return False
    if not is_email_due(email, cadence_step):
        print(f"[SKIP] Fora da janela -> {email}")
        return False
    dispatch_key = LEAD_STATE.reserve_dispatch(
        phone=_dispatch_phone_for_email(lead),
        email=email,
        channel="email",
        cadence_step=cadence_step,
        payload={
            "person_id": int(lead.get("id", 0) or 0),
            "email": email,
            "channel": "email",
            "cadence_step": int(cadence_step or 0),
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    if not dispatch_key:
        print(f"[DUPLICADO_BLOQUEADO] email={email} | canal=email | etapa={cadence_step}")
        return False

    if crm_has_email_stage_tag(lead, cadence_step):
        print(f"[DUPLICADO_BLOQUEADO] email={email} | canal=email | etapa={cadence_step}")
        LEAD_STATE.release_dispatch(dispatch_key)
        return False
    if ja_enviado(lead["id"], cadence_step):
        print(f"[DUPLICADO_BLOQUEADO] email={email} | canal=email | etapa={cadence_step}")
        LEAD_STATE.release_dispatch(dispatch_key)
        return False

    channel_lock = ProcessLock("email_send_lock")
    try:
        channel_lock.acquire()
    except ProcessAlreadyRunningError:
        print(f"[EMAIL_LOCK] envio em andamento -> {email}")
        LEAD_STATE.release_dispatch(dispatch_key)
        return False

    try:
        current_count = int(DAILY_LIMITS.current_count("email") or 0)
        print(f"[EMAIL_CONTADOR] {current_count}/{DAILY_EMAIL_LIMIT}")
        if current_count >= int(DAILY_EMAIL_LIMIT * 0.8):
            print(f"[ALERTA_LIMITE_80] canal=email {current_count}/{DAILY_EMAIL_LIMIT}")
        if current_count >= DAILY_EMAIL_LIMIT:
            print(f"[LIMITE_DIARIO_ATINGIDO] canal=email {current_count}/{DAILY_EMAIL_LIMIT}")
            LEAD_STATE.release_dispatch(dispatch_key)
            return False
        _wait_email_gap()
        ok = enviar_email(email, lead["nome"], lead.get("empresa", ""), cadence_step)
        if ok:
            note_ok = registrar_envio(lead["id"], email, cadence_step)
            tag_ok = aplicar_tag_email_crm(lead, cadence_step)
            sincronizar_pipeline_por_par(lead)
            updated_count = int(DAILY_LIMITS.increment("email") or 0)
            LEAD_STATE.increment_daily_send_count("email")
            print(f"[EMAIL_CONTADOR] {updated_count}/{DAILY_EMAIL_LIMIT}")
            print(f"[EMAIL_ENVIADO] destino={email} etapa={cadence_step}")
            if not (note_ok and tag_ok):
                print(f"[ERRO_CRM_CRITICO] operacao=post_send_email | person={int(lead.get('id', 0) or 0)}")
                return True
            advance_email_cadence(lead, cadence_step)
            return True
        LEAD_STATE.release_dispatch(dispatch_key)
        return False
    finally:
        channel_lock.release()

def parse_iso_datetime(raw):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _email_state(email):
    return LEAD_STATE.find_by_email(str(email or "").strip().lower())


def current_email_step(lead):
    crm_step = crm_email_step(lead)
    if crm_step:
        return max(1, min(MAX_CADENCE_DAY + 1, int(crm_step)))
    email = str(lead.get("email") or "").strip().lower()
    state = _email_state(email)
    raw = state.get("email_cadence_step") or state.get("cadence_step") or 1
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return max(1, min(MAX_CADENCE_DAY + 1, int(digits or "1")))


def next_email_at(email):
    state = _email_state(email)
    return parse_iso_datetime(state.get("next_email_at"))


def has_reply_priority(email):
    state = _email_state(email)
    last_inbound = parse_iso_datetime(state.get("last_inbound_at"))
    last_outbound = parse_iso_datetime(state.get("last_outbound_at"))
    if not last_inbound:
        return False
    if last_outbound and last_outbound >= last_inbound:
        return False
    return True


def has_blocking_status(email):
    state = _email_state(email)
    return str(state.get("status", "")).strip().lower() in BLOCKING_STATUSES


def has_recent_interaction(email, minutes=5):
    state = _email_state(email)
    timestamps = [
        parse_iso_datetime(state.get("last_inbound_at")),
        parse_iso_datetime(state.get("last_outbound_at")),
        parse_iso_datetime(state.get("last_message_at")),
    ]
    latest = max((value for value in timestamps if value), default=None)
    if latest is None:
        return False
    return latest >= (datetime.now(timezone.utc) - timedelta(minutes=max(1, int(minutes))))


def is_email_due(email, cadence_step):
    if int(cadence_step or 0) > MAX_CADENCE_DAY:
        return False
    scheduled = next_email_at(email)
    if scheduled is None:
        return True
    return datetime.now(timezone.utc) >= scheduled


def advance_email_cadence(lead, cadence_step):
    email = str(lead.get("email") or "").strip().lower()
    next_step = min(MAX_CADENCE_DAY + 1, int(cadence_step or 1) + 1)
    days = int(CHANNEL_DELAY_DAYS.get(int(cadence_step or 1), CHANNEL_DELAY_DAYS[MAX_CADENCE_DAY]))
    next_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat() if int(cadence_step or 0) < MAX_CADENCE_DAY else ""
    LEAD_STATE.record_outbound(
        "",
        cadence_step=cadence_step,
        lead={
            "email": email,
            "nome": lead.get("nome", ""),
            "email_cadence_step": next_step,
            "next_email_at": next_at,
        },
        status="active",
    )

# =========================
# MAIN LOOP
# =========================
def main(limit=30):
    current_process = os.path.basename(__file__).strip().lower()
    if current_process != AUTHORIZED_PROCESS_NAME:
        print(f"[PROCESSO_NAO_AUTORIZADO] {current_process}")
        return 1
    process_lock = ProcessLock(WORKER_LOCK_NAME)
    try:
        process_lock.acquire()
    except ProcessAlreadyRunningError:
        print("[JA_EM_EXECUCAO] email_worker.py")
        return 0

    print("[PROCESSO_INICIADO] email_worker.py")
    print("[SUPERVISOR_READY] email")
    garantir_labels_email()
    try:
        leads = buscar_leads(limit)
        sincronizar_tags_email_crm(leads)

        print(f"[INFO] Leads v�lidos: {len(leads)}")

        enviados = 0
        pulados = 0

        for lead in leads:
            email = str(lead["email"] or "").strip().lower()
            cadence_step = current_email_step(lead)

            if cadence_step > MAX_CADENCE_DAY:
                print(f"[SKIP] Cadencia finalizada -> {email}")
                pulados += 1
                continue

            sucesso = enviar_email_controlado(lead, cadence_step)
            if sucesso:
                enviados += 1
                continue
            pulados += 1

        print(f"[RESUMO] enviados={enviados} | pulados={pulados}")
        return enviados
    finally:
        process_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
