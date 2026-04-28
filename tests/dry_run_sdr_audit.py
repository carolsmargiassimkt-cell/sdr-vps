from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crm import pipedrive_client as pipedrive_module


pipedrive_module.PipedriveClient.ensure_deal_labels = lambda self, labels: True
pipedrive_module.PipedriveClient.get_deal_labels = lambda self: []

import inbox_handler as inbox_handler_module
import supervisor as supervisor_module
from services.whatsapp_service import WhatsAppService


class FakeCRM:
    STATUS_BOT_FIELD = "status_bot"

    def __init__(self, person_name="Lead"):
        self.person_name = person_name
        self.tags = []
        self.deal_updates = []
        self.person_updates = []
        self.notes = []
        self.stage_updates = []
        self.activities = []
        self.people = {
            101: {
                "id": 101,
                "name": self.person_name,
                "phone": [{"value": "5511999999999"}],
                "email": [{"value": "lead@example.com"}],
            }
        }
        self.deals = {
            501: {
                "id": 501,
                "title": "ACME",
                "org_name": "ACME",
                "stage_id": 1,
                "status_bot": "",
                "label": [],
                "person_id": {"value": 101},
            }
        }
        self.stages = [
            {"id": 1, "pipeline_id": 2, "name": "Entrada"},
            {"id": 22, "pipeline_id": 2, "name": "Perdido"},
            {"id": 33, "pipeline_id": 2, "name": "Reuniao agendada"},
        ]

    def find_person(self, phone=None, email=None, **_kwargs):
        normalized = "".join(ch for ch in str(phone or "") if ch.isdigit())
        target_email = str(email or "").strip().lower()
        return {
            "id": 101,
            "name": self.person_name,
            "phone": [{"value": normalized or "5511999999999"}],
            "email": [{"value": target_email or "lead@example.com"}],
        }

    def find_open_deals_by_person_or_org(self, **_kwargs):
        return [dict(self.deals[501])]

    def add_tag(self, entity_type, entity_id, tag):
        self.tags.append((str(entity_type), int(entity_id), str(tag)))
        self.deals.setdefault(int(entity_id), {"id": int(entity_id), "label": []})
        labels = list(self.deals[int(entity_id)].get("label") or [])
        labels.append(str(tag))
        self.deals[int(entity_id)]["label"] = labels
        return True

    def update_deal(self, deal_id, data):
        target_id = int(deal_id)
        payload = dict(data or {})
        self.deal_updates.append((target_id, payload))
        deal = self.deals.setdefault(target_id, {"id": target_id, "label": []})
        deal.update(payload)
        return True

    def update_person(self, person_id, data):
        self.person_updates.append((int(person_id), dict(data or {})))
        return True

    def add_note(self, deal_id, content):
        self.notes.append((int(deal_id), str(content)))
        return True

    def update_stage(self, *, deal_id, stage_id):
        target_id = int(deal_id)
        self.stage_updates.append((target_id, int(stage_id)))
        self.deals.setdefault(target_id, {"id": target_id, "label": []})["stage_id"] = int(stage_id)
        return True

    def create_activity(self, **kwargs):
        self.activities.append(dict(kwargs or {}))
        return True

    def get_person_details(self, person_id):
        return dict(self.people.get(int(person_id), {}))

    def get_deal_labels(self):
        return []

    def get_stages(self):
        return list(self.stages)

    def resolve_label_tokens(self, raw_labels, _options=None):
        if isinstance(raw_labels, list):
            values = raw_labels
        elif raw_labels in (None, "", {}):
            values = []
        else:
            values = [raw_labels]
        tokens = set()
        for item in values:
            if isinstance(item, dict):
                candidate = item.get("label") or item.get("name") or item.get("value") or item.get("id")
                if candidate:
                    tokens.add(str(candidate).strip().upper())
            else:
                token = str(item or "").strip()
                if token:
                    tokens.add(token.upper())
        return tokens

    def get_deal_details(self, deal_id):
        return dict(self.deals.get(int(deal_id), {"id": int(deal_id), "stage_id": 1, "status_bot": "", "label": []}))

    def get_deal_participants(self, deal_id, *args, **kwargs):
        return []

    def add_deal_participant(self, deal_id, person_id, *args, **kwargs):
        return True


def assert_true(condition, label):
    if not condition:
        raise AssertionError(label)
    print(f"[OK] {label}")


def build_local_service(base_dir: Path) -> WhatsAppService:
    service = WhatsAppService()
    data_dir = base_dir / "data"
    logs_dir = base_dir / "logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    service.data_dir = data_dir
    service.logs_dir = logs_dir
    service.sent_file = str(base_dir / "sent.json")
    service.sent_lock_file = str(base_dir / "sent.json.lock")
    service.invalid_file = str(base_dir / "invalidos.json")
    service.history_file = str(logs_dir / "whatsapp_message_history.json")
    service.manual_blocklist_file = str(logs_dir / "whatsapp_manual_blocklist.json")
    service.channel_map_file = str(data_dir / "whatsapp_channel_map.json")
    service.validation_cache_file = str(data_dir / "whatsapp_validation_cache.json")
    service.after_hours_state_file = str(data_dir / "after_hours_state.json")
    service.after_hours_lock_file = str(data_dir / "after_hours_state.lock")
    return service


def configure_inbox_environment(base_dir: Path, crm: FakeCRM, service: WhatsAppService):
    set_agent_decide_env(enabled=False)
    inbox_handler_module.BASE_DIR = str(base_dir)
    inbox_handler_module.DATA_DIR = str(base_dir / "data")
    inbox_handler_module.LOGS_DIR = str(base_dir / "logs")
    inbox_handler_module.BLOCKLIST_FILE = str(base_dir / "logs" / "whatsapp_manual_blocklist.json")
    inbox_handler_module.HISTORY_FILE = str(base_dir / "logs" / "whatsapp_message_history.json")
    inbox_handler_module.PROCESSED_FILE = str(base_dir / "inbound_processed.json")
    inbox_handler_module.PROCESSED_LOCK_FILE = str(base_dir / "inbound_processed.json.lock")
    inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE = str(base_dir / "data" / "email_pending_confirmations.json")
    inbox_handler_module.EMAIL_QUEUE_FILE = str(base_dir / "data" / "email_handoff_queue.json")
    inbox_handler_module.crm = crm
    inbox_handler_module.whatsapp = service
    inbox_handler_module.lead_cache.clear()
    inbox_handler_module.processed.clear()
    Path(inbox_handler_module.PROCESSED_FILE).write_text("[]", encoding="utf-8")
    Path(inbox_handler_module.HISTORY_FILE).write_text("{}", encoding="utf-8")


def set_agent_decide_env(*, enabled: bool, upstream: str = ""):
    os.environ["AGENT_DECIDE_ENABLED"] = "1" if enabled else "0"
    if upstream:
        os.environ["AGENT_DECIDE_UPSTREAM_URL"] = str(upstream)
    else:
        os.environ.pop("AGENT_DECIDE_UPSTREAM_URL", None)


def test_handler_negative_messages():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        sent_messages = []

        def fake_send(phone, text, **kwargs):
            sent_messages.append({"phone": service.normalize_phone(phone), "text": str(text), "kwargs": dict(kwargs)})
            return True

        service.send_message = fake_send
        service.healthcheck = lambda: True
        service.clear_after_hours_pending = lambda _phone: True
        service.was_after_hours_notice_sent_today = lambda _phone, day_str=None: False
        service.mark_after_hours_notice_sent = lambda _phone, day_str=None: True
        service.upsert_after_hours_pending = lambda *_args, **_kwargs: True

        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)

        client = inbox_handler_module.app.test_client()
        for phone, text in (
            ("5516981598059", "Nao temos interesse. Agradeco!"),
            ("5516981598060", "Nao queremos!"),
        ):
            inbox_handler_module.lead_cache.clear()
            response = client.post("/inbound", json={"phone": phone, "message": text, "messageId": text})
            payload = response.get_json()
            assert_true(bool(payload and payload.get("ok")), f"handler aceita negativo: {text}")

        blocklist = json.loads(Path(inbox_handler_module.BLOCKLIST_FILE).read_text(encoding="utf-8"))
        normalized_entries = {
            service.normalize_phone(item.get("telefone") or item.get("phone") or item.get("number"))
            for item in blocklist
            if isinstance(item, dict)
        }
        assert_true("16981598059" in normalized_entries, "negativo adiciona blocklist")
        assert_true("16981598060" in normalized_entries, "segundo negativo adiciona blocklist")
        assert_true(any(tag[2] == "respondido" for tag in crm.tags), "negativo aplica tag respondido")
        assert_true(any(update[1].get("status_bot") == "lead_sem_interesse" for update in crm.deal_updates), "negativo grava status lead_sem_interesse")
        assert_true(any("encerrar" in item["text"].lower() for item in sent_messages), "negativo envia encerramento")


def test_handler_positive_message():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        sent_messages = []

        def fake_send(phone, text, **kwargs):
            sent_messages.append({"phone": service.normalize_phone(phone), "text": str(text), "kwargs": dict(kwargs)})
            return True

        service.send_message = fake_send
        service.healthcheck = lambda: True
        service.clear_after_hours_pending = lambda _phone: True
        service.was_after_hours_notice_sent_today = lambda _phone, day_str=None: False
        service.mark_after_hours_notice_sent = lambda _phone, day_str=None: True
        service.upsert_after_hours_pending = lambda *_args, **_kwargs: True

        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)

        client = inbox_handler_module.app.test_client()
        response = client.post("/inbound", json={"phone": "5511996096447", "message": "Pode sim", "messageId": "positive-1"})
        payload = response.get_json()
        assert_true(bool(payload and payload.get("ok")), "handler aceita positivo simples")
        assert_true(any("como posso te chamar" in item["text"].lower() for item in sent_messages), "positivo pede nome quando contato e generico")
        assert_true(any(tag[2] == "respondido" for tag in crm.tags), "positivo pausa outbound com tag respondido")


def test_whatsapp_service_blocklist_and_confirmation():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)

        blocklist_payload = [{"phone": "5516981598059", "reason": "manual"}]
        Path(service.manual_blocklist_file).write_text(json.dumps(blocklist_payload), encoding="utf-8")
        assert_true(service.is_phone_in_manual_blocklist("16981598059"), "blocklist reconhece phone/telefone variado")
        assert_true(not service.can_send("16981598059"), "blocklist barra envio antes da fila")

        service._append_history_entry("11999999999", "out", "Mensagem teste", step=1, source="send_api")
        assert_true(not service.wait_for_outbound_sync("11999999999", "Mensagem teste", timeout_seconds=0.5, poll_seconds=0.1), "send_api isolado nao confirma envio")

        service._append_history_entry("11999999999", "out", "Mensagem teste", step=1, source="sync")
        assert_true(service.wait_for_outbound_sync("11999999999", "Mensagem teste", timeout_seconds=0.5, poll_seconds=0.1), "sync confirma envio")


def test_supervisor_opening_guard_and_recovery_skip():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        data_dir = base_dir / "data"
        logs_dir = base_dir / "logs"
        data_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        service = build_local_service(base_dir)
        supervisor_module.DATA_DIR = data_dir
        supervisor_module.LOGS_DIR = logs_dir
        supervisor_module.BLOCKLIST_FILE = str(logs_dir / "whatsapp_manual_blocklist.json")
        supervisor_module.HISTORY_FILE = str(logs_dir / "whatsapp_message_history.json")
        supervisor_module.INBOUND_RECOVERY_FILE = str(data_dir / "inbound_recovery_state.json")
        supervisor_module.CONVERSATION_MEMORY_FILE = str(data_dir / "whatsapp_conversation_memory.json")
        supervisor_module.WhatsAppService = lambda: service
        supervisor_module.PipedriveClient = lambda logger=None: FakeCRM()
        supervisor_module.WhatsAppConversationMemory = None

        bot = supervisor_module.SDRSupervisor()
        bot.blocklist = set()
        bot._inbound_recovery_state = {"handled": []}

        assert_true(not bot._already_sent_opening_for_cadence(501, "11999999999", 1), "opening_guard inicia vazio")
        bot._mark_sent_opening_for_cadence(501, "11999999999", 1)
        assert_true(bot._already_sent_opening_for_cadence(501, "11999999999", 1), "opening_guard trava deal+telefone+cadencia")

        history_payload = {
            "16981598059": [
                {
                    "direction": "in",
                    "message": "Nao queremos!",
                    "created_at": datetime.now().isoformat(),
                }
            ]
        }
        Path(supervisor_module.HISTORY_FILE).write_text(json.dumps(history_payload), encoding="utf-8")
        assert_true(bot._recent_unanswered_inbound_candidates() == [], "recovery ignora inbound terminal negativo")

        history_payload = {
            "7133391500": [
                {
                    "direction": "in",
                    "message": "Aqui e uma contabilidade",
                    "created_at": datetime.now().isoformat(),
                }
            ]
        }
        Path(supervisor_module.HISTORY_FILE).write_text(json.dumps(history_payload), encoding="utf-8")
        assert_true(bot._recent_unanswered_inbound_candidates() == [], "recovery ignora inbound institucional/empresa_incompativel")

        assert_true(bot._next_cadence_step({"WHATSAPP_CAD1"}) == 2, "WHATSAPP_CAD1 avanca cadence_step")
        assert_true(bot._next_cadence_step({"EMAIL_CAD2"}) == 3, "EMAIL_CAD2 segue padrao lido pelo parser")
        assert_true(bot._legacy_cadence_tags({"CAD1"}) == ["CAD1"], "legacy cad1 e detectado como tag antiga")


def test_runtime_uses_gemini_not_openrouter():
    inbox_source = (ROOT / "inbox_handler.py").read_text(encoding="utf-8")
    pitch_source = (ROOT / "logic" / "whatsapp_pitch_engine.py").read_text(encoding="utf-8")
    assert_true("OPENROUTER" not in inbox_source and "openrouter" not in inbox_source, "handler runtime nao usa OpenRouter")
    assert_true("OPENROUTER" not in pitch_source and "openrouter" not in pitch_source, "pitch engine runtime nao usa OpenRouter")
    assert_true("classify_inbound_intent_gemini" in inbox_source, "handler usa classificacao Gemini")
    assert_true("GEMINI_GENERATE_URL_TEMPLATE" in pitch_source, "pitch engine usa endpoint Gemini")


def test_email_callback_confirmation_flow():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)

        pending_payload = {
            "email:501:1:123": {
                "event_id": "email:501:1:123",
                "deal_id": 501,
                "person_id": 101,
                "email": "lead@example.com",
                "cadence_step": 1,
                "cadence_tag": "EMAIL_CAD1",
                "created_at": datetime.now().isoformat(),
            }
        }
        Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).write_text(json.dumps(pending_payload), encoding="utf-8")

        client = inbox_handler_module.app.test_client()
        response = client.post("/email/callback", json={"event_id": "email:501:1:123", "status": "sent"})
        payload = response.get_json()
        assert_true(bool(payload and payload.get("ok") and payload.get("confirmed")), "email callback confirma envio")
        assert_true(any(tag[2] == "EMAIL_CAD1" for tag in crm.tags), "email callback aplica EMAIL_CAD1")
        assert_true(any(update[1].get("status_bot") == "contato_iniciado" for update in crm.deal_updates), "email callback atualiza status apos confirmacao")
        assert_true(any("Confirmado por callback do n8n" in note[1] for note in crm.notes), "email callback adiciona nota")
        history = json.loads(Path(inbox_handler_module.HISTORY_FILE).read_text(encoding="utf-8"))
        history_items = history.get("email:deal=501", [])
        assert_true(any(item.get("message") == "EMAIL_CAD_1" for item in history_items), "email callback grava historico confirmado")
        pending_after = json.loads(Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).read_text(encoding="utf-8"))
        assert_true(not pending_after, "email callback remove pendencia")


def test_email_inbound_negative_and_schedule():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)

        Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).write_text(
            json.dumps(
                {
                    "email:501:1:123": {
                        "event_id": "email:501:1:123",
                        "deal_id": 501,
                        "person_id": 101,
                        "email": "lead@example.com",
                    }
                }
            ),
            encoding="utf-8",
        )
        Path(inbox_handler_module.EMAIL_QUEUE_FILE).write_text(
            json.dumps([{"id": 501, "person_id": 101, "email": "lead@example.com"}]),
            encoding="utf-8",
        )

        client = inbox_handler_module.app.test_client()
        negative = client.post(
            "/email/inbound",
            json={
                "deal_id": 501,
                "person_id": 101,
                "email": "lead@example.com",
                "phone": "5511999999999",
                "text": "Nao temos interesse. Agradeco!",
                "message_id": "email-neg-1",
            },
        ).get_json()
        assert_true(bool(negative and negative.get("ok")), "email inbound negativo responde payload valido")
        assert_true(negative.get("intent") == "nao_interessado", "email inbound negativo classifica corretamente")
        assert_true(any(update[1].get("status_bot") == "lead_sem_interesse" for update in crm.deal_updates), "email inbound negativo atualiza status")
        assert_true(any(tag[2] == "respondido" for tag in crm.tags), "email inbound negativo aplica respondido")
        assert_true(any(stage[1] == 22 for stage in crm.stage_updates), "email inbound negativo move para perdido")
        assert_true(any("Inbound email negativo" in note[1] for note in crm.notes), "email inbound negativo gera nota")
        pending_after_negative = json.loads(Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).read_text(encoding="utf-8"))
        queue_after_negative = json.loads(Path(inbox_handler_module.EMAIL_QUEUE_FILE).read_text(encoding="utf-8"))
        assert_true(not pending_after_negative, "email inbound negativo limpa pendencias")
        assert_true(not queue_after_negative, "email inbound negativo limpa fila")

        schedule = client.post(
            "/email/inbound",
            json={
                "deal_id": 501,
                "person_id": 101,
                "email": "lead@example.com",
                "text": "Podemos falar amanha as 10",
                "message_id": "email-schedule-1",
            },
        ).get_json()
        assert_true(bool(schedule and schedule.get("ok")), "email inbound agendamento responde payload valido")
        assert_true(schedule.get("intent") == "agendamento", "email inbound agendamento classifica corretamente")
        assert_true(any(stage[1] == 33 for stage in crm.stage_updates), "email inbound agendamento move para reuniao")
        assert_true(bool(crm.activities), "email inbound agendamento cria atividade")


def test_supervisor_email_waits_callback_before_crm():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        data_dir = base_dir / "data"
        logs_dir = base_dir / "logs"
        data_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        service = build_local_service(base_dir)
        supervisor_module.DATA_DIR = data_dir
        supervisor_module.LOGS_DIR = logs_dir
        supervisor_module.HISTORY_FILE = str(logs_dir / "whatsapp_message_history.json")
        supervisor_module.BLOCKLIST_FILE = str(logs_dir / "whatsapp_manual_blocklist.json")
        supervisor_module.EMAIL_HANDOFF_QUEUE_FILE = str(data_dir / "email_handoff_queue.json")
        supervisor_module.EMAIL_PENDING_CONFIRMATIONS_FILE = str(data_dir / "email_pending_confirmations.json")
        supervisor_module.WhatsAppService = lambda: service
        crm = FakeCRM()
        supervisor_module.PipedriveClient = lambda logger=None: crm
        supervisor_module.WhatsAppConversationMemory = None

        old_post = supervisor_module.requests.post

        class FakeResponse:
            status_code = 200
            content = b"{}"

            def json(self):
                return {}

        supervisor_module.requests.post = lambda *args, **kwargs: FakeResponse()
        try:
            bot = supervisor_module.SDRSupervisor()
            bot.blocklist = set()
            ok = bot._send_email({"id": 501, "person_id": 101, "email": "lead@example.com", "nome": "Lead", "empresa": "ACME", "cadence_step": 1})
            assert_true(ok, "supervisor dispara email para n8n")
            assert_true(not crm.tags, "supervisor nao aplica tag antes do callback")
            assert_true(not crm.notes, "supervisor nao cria nota antes do callback")
            history = json.loads(Path(supervisor_module.HISTORY_FILE).read_text(encoding="utf-8")) if Path(supervisor_module.HISTORY_FILE).exists() else {}
            assert_true("email:deal=501" not in history, "supervisor nao marca historico confirmado antes do callback")
            pending = json.loads(Path(supervisor_module.EMAIL_PENDING_CONFIRMATIONS_FILE).read_text(encoding="utf-8"))
            assert_true(bool(pending), "supervisor grava pendencia de confirmacao do email")
        finally:
            supervisor_module.requests.post = old_post


def test_supervisor_email_agent_decide_blocks_send():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        data_dir = base_dir / "data"
        logs_dir = base_dir / "logs"
        data_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        service = build_local_service(base_dir)
        supervisor_module.DATA_DIR = data_dir
        supervisor_module.LOGS_DIR = logs_dir
        supervisor_module.HISTORY_FILE = str(logs_dir / "whatsapp_message_history.json")
        supervisor_module.BLOCKLIST_FILE = str(logs_dir / "whatsapp_manual_blocklist.json")
        supervisor_module.EMAIL_HANDOFF_QUEUE_FILE = str(data_dir / "email_handoff_queue.json")
        supervisor_module.EMAIL_PENDING_CONFIRMATIONS_FILE = str(data_dir / "email_pending_confirmations.json")
        supervisor_module.WhatsAppService = lambda: service
        crm = FakeCRM()
        supervisor_module.PipedriveClient = lambda logger=None: crm
        supervisor_module.WhatsAppConversationMemory = None
        set_agent_decide_env(enabled=True)

        old_post = supervisor_module.requests.post

        class FakeResponse:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload
                self.content = json.dumps(payload).encode("utf-8")

            def json(self):
                return dict(self._payload)

        def fake_post(url, json=None, timeout=None):
            if url == supervisor_module.HANDLER_AGENT_DECIDE_URL:
                return FakeResponse(200, {"ok": True, "intent": "lead_encerrado", "action": "hold", "should_send": False})
            return FakeResponse(200, {})

        supervisor_module.requests.post = fake_post
        try:
            bot = supervisor_module.SDRSupervisor()
            bot.blocklist = set()
            ok = bot._send_email({"id": 501, "person_id": 101, "email": "lead@example.com", "nome": "Lead", "empresa": "ACME", "cadence_step": 1})
            assert_true(ok is False, "supervisor bloqueia email quando agent decide manda segurar")
            pending_path = Path(supervisor_module.EMAIL_PENDING_CONFIRMATIONS_FILE)
            pending = json.loads(pending_path.read_text(encoding="utf-8")) if pending_path.exists() else {}
            assert_true(not pending, "supervisor nao registra callback pendente quando agent decide bloqueia")
        finally:
            supervisor_module.requests.post = old_post
            set_agent_decide_env(enabled=False)


def test_email_callback_marks_hybrid_exhaustion():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)

        pending_payload = {
            "email:501:6:999": {
                "event_id": "email:501:6:999",
                "deal_id": 501,
                "person_id": 101,
                "email": "lead@example.com",
                "cadence_step": 6,
                "cadence_tag": "EMAIL_CAD6",
                "created_at": datetime.now().isoformat(),
            }
        }
        Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(inbox_handler_module.EMAIL_PENDING_CONFIRMATIONS_FILE).write_text(json.dumps(pending_payload), encoding="utf-8")

        client = inbox_handler_module.app.test_client()
        response = client.post("/email/callback", json={"event_id": "email:501:6:999", "status": "sent"})
        payload = response.get_json()
        assert_true(bool(payload and payload.get("ok") and payload.get("confirmed")), "email callback cad6 confirma envio")
        assert_true(any(update[1].get("status_bot") == "tentativa_esgotada" for update in crm.deal_updates), "email callback cad6 marca tentativa esgotada")
        assert_true(any(stage[1] == 22 for stage in crm.stage_updates), "email callback cad6 move para perdido")
        assert_true(any("6 cadencias hibridas" in note[1] for note in crm.notes), "email callback cad6 adiciona nota de exaustao")


def test_agent_decide_route():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)

        client = inbox_handler_module.app.test_client()
        negative = client.post("/agent/decide", json={"channel": "whatsapp", "text": "Nao temos interesse. Agradeco!", "deal_id": 501}).get_json()
        assert_true(bool(negative and negative.get("ok")), "agent decide responde payload valido")
        assert_true(negative.get("intent") == "nao_interessado", "agent decide prioriza negativo")
        assert_true(negative.get("action") == "close_and_block", "agent decide define acao terminal negativa")
        assert_true("should_send" in negative and "should_blocklist" in negative, "agent decide retorna contrato expandido")

        positive = client.post("/agent/decide", json={"channel": "whatsapp", "text": "Pode sim", "deal_id": 501, "name": "Lead"}).get_json()
        assert_true(bool(positive and positive.get("ok")), "agent decide responde positivo")
        assert_true(positive.get("intent") == "interesse", "agent decide classifica interesse")


def test_agent_decide_dry_run_cases():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)
        client = inbox_handler_module.app.test_client()

        set_agent_decide_env(enabled=True)
        positive = client.post("/agent/decide", json={"channel": "whatsapp", "message_text": "Pode sim", "deal_id": 501, "name": "Lead"}).get_json()
        assert_true(positive.get("should_send") is True, "dry-run positivo pode sim permite resposta")

        negative = client.post("/agent/decide", json={"channel": "whatsapp", "message_text": "Nao queremos", "deal_id": 501}).get_json()
        assert_true(negative.get("should_blocklist") is True, "dry-run negativo bloqueia contato")

        copa = client.post("/agent/decide", json={"channel": "whatsapp", "message_text": "Como funciona essa campanha da Copa?", "deal_id": 501}).get_json()
        assert_true(copa.get("intent") in {"duvida", "neutro", "interesse"}, "dry-run duvida copa fica em interacao segura")

        reuniao = client.post("/agent/decide", json={"channel": "whatsapp", "message_text": "Podemos marcar uma reuniao amanha as 10?", "deal_id": 501}).get_json()
        assert_true(reuniao.get("should_move_stage") is True and reuniao.get("should_create_activity") is True, "dry-run pedido de reuniao aciona agenda")

        set_agent_decide_env(enabled=True, upstream="http://127.0.0.1:9/agent/decide")
        fallback = client.post("/agent/decide", json={"channel": "whatsapp", "message_text": "Pode sim", "deal_id": 501, "name": "Lead"}).get_json()
        assert_true(fallback.get("intent") == "interesse", "dry-run agentgraph fora do ar cai no fallback local")


def test_botpress_agent_adapter():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)
        client = inbox_handler_module.app.test_client()

        old_post = inbox_handler_module.requests.post

        class FakeResponse:
            def __init__(self, payload, status_code=200):
                self.status_code = status_code
                self._payload = payload
                self.content = json.dumps(payload).encode("utf-8")

            def json(self):
                return dict(self._payload)

        def fake_post(url, json=None, timeout=None, **kwargs):
            if "/converse" not in str(url):
                return old_post(url, json=json, timeout=timeout, **kwargs)
            if str((json or {}).get("message") or "").strip().lower() == "json":
                return FakeResponse(
                    {
                        "responses": [
                            {
                                "text": __import__("json").dumps(
                                    {
                                        "intent": "agendamento",
                                        "action": "schedule",
                                        "reply": "Posso te sugerir quarta às 10h.",
                                        "should_send": True,
                                        "should_move_stage": True,
                                        "should_create_activity": True,
                                    }
                                )
                            }
                        ]
                    }
                )
            return FakeResponse({"responses": [{"text": "Resposta sugerida pelo Botpress."}]})

        inbox_handler_module.requests.post = fake_post
        try:
            text_mode = client.post("/agent/decide/botpress", json={"channel": "whatsapp", "message_text": "Pode sim", "deal_id": 501}).get_json()
            assert_true(bool(text_mode and text_mode.get("ok")), "adapter botpress responde em modo texto")
            assert_true(text_mode.get("reply") == "Resposta sugerida pelo Botpress.", "adapter botpress aproveita reply textual")

            json_mode = client.post("/agent/decide/botpress", json={"channel": "whatsapp", "message_text": "json", "deal_id": 501}).get_json()
            assert_true(bool(json_mode and json_mode.get("ok")), "adapter botpress responde em modo json")
            assert_true(json_mode.get("intent") == "agendamento", "adapter botpress aceita contrato json do botpress")
            assert_true(json_mode.get("should_move_stage") is True, "adapter botpress preserva flags de agenda")
        finally:
            inbox_handler_module.requests.post = old_post


def test_inbound_referral_and_meeting_with_agent_decide():
    with tempfile.TemporaryDirectory() as tmp:
        base_dir = Path(tmp)
        service = build_local_service(base_dir)
        sent_messages = []

        def fake_send(phone, text, **kwargs):
            sent_messages.append({"phone": service.normalize_phone(phone), "text": str(text), "kwargs": dict(kwargs)})
            return True

        service.send_message = fake_send
        service.healthcheck = lambda: True
        service.clear_after_hours_pending = lambda _phone: True
        service.was_after_hours_notice_sent_today = lambda _phone, day_str=None: False
        service.mark_after_hours_notice_sent = lambda _phone, day_str=None: True
        service.upsert_after_hours_pending = lambda *_args, **_kwargs: True

        crm = FakeCRM(person_name="Lead")
        configure_inbox_environment(base_dir, crm, service)
        set_agent_decide_env(enabled=True)
        client = inbox_handler_module.app.test_client()

        referral = client.post("/inbound", json={"phone": "5511999999999", "message": "Fale com o marketing no email compras@acme.com", "messageId": "ref-1"}).get_json()
        assert_true(bool(referral and referral.get("ok")), "dry-run indicacao de contato segue segura no inbound")

        meeting = client.post("/inbound", json={"phone": "5511888888888", "message": "Podemos marcar uma reuniao amanha as 10?", "messageId": "meet-1"}).get_json()
        assert_true(bool(meeting and meeting.get("ok")), "dry-run pedido de reuniao no inbound responde ok")
        assert_true(any(stage[1] == 33 for stage in crm.stage_updates), "dry-run reuniao inbound move etapa")


def main():
    test_handler_negative_messages()
    test_handler_positive_message()
    test_whatsapp_service_blocklist_and_confirmation()
    test_supervisor_opening_guard_and_recovery_skip()
    test_runtime_uses_gemini_not_openrouter()
    test_email_callback_confirmation_flow()
    test_email_inbound_negative_and_schedule()
    test_supervisor_email_waits_callback_before_crm()
    test_supervisor_email_agent_decide_blocks_send()
    test_email_callback_marks_hybrid_exhaustion()
    test_agent_decide_route()
    test_agent_decide_dry_run_cases()
    test_botpress_agent_adapter()
    test_inbound_referral_and_meeting_with_agent_decide()
    print("[OK] dry-run audit concluido")


if __name__ == "__main__":
    main()
