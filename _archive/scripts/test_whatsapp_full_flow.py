from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from core.cadence_manager import CadenceManager
from core.event_logger import UnifiedEventLogger
from core.lead_state import LeadStateStore
from core.queue_manager import QueueManager
from logic.email_handoff_queue import EmailHandoffQueue
from logic.google_sheet_status_sync import GoogleSheetStatusSync
from logic.whatsapp_conversation_memory import WhatsAppConversationMemory
from logs.event_logger import EventLogger
from utils.safe_json import safe_read_json
from services.whatsapp_bot import WhatsAppBot


class FakePipedriveClient:
    def __init__(self) -> None:
        self.person_updates: list[dict] = []
        self.deal_updates: list[dict] = []
        self.notes: list[dict] = []
        self.stage_updates: list[dict] = []

    def update_person(self, *, person_id: int, data: dict) -> None:
        self.person_updates.append({"person_id": person_id, "data": dict(data)})

    def update_deal(self, *, deal_id: int, data: dict) -> None:
        self.deal_updates.append({"deal_id": deal_id, "data": dict(data)})

    def update_stage(self, *, deal_id: int, stage_id: int) -> None:
        self.stage_updates.append({"deal_id": deal_id, "stage_id": stage_id})

    def create_note(self, *, person_id: int, deal_id: int, content: str) -> None:
        self.notes.append({"person_id": person_id, "deal_id": deal_id, "content": content})


class TestWhatsAppBot(WhatsAppBot):
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.fake_client = FakePipedriveClient()
        self.sent_messages: list[dict] = []
        super().__init__(
            "WHATSAPP BOT 1",
            real_send=True,
            logger=EventLogger(str(base_dir / "logs" / "test.log")),
            open_delay_sec=6.0,
            send_confirm_delay_sec=1.2,
            cadence_step="1",
        )
        self.targets_file = base_dir / "config" / "whatsapp_targets.json"
        self.history_file = base_dir / "logs" / "whatsapp_message_history.json"
        self.blocklist_file = base_dir / "logs" / "whatsapp_manual_blocklist.json"
        self.state_file = base_dir / "logs" / "whatsapp_conversation_state.json"
        self.sheet_queue_file = base_dir / "logs" / "whatsapp_sheet_status_queue.json"
        self.email_handoff_queue_file = base_dir / "logs" / "email_handoff_queue.json"
        self.memory = WhatsAppConversationMemory(self.state_file, logger=self.logger)
        self.sheet_sync = GoogleSheetStatusSync(self.sheet_queue_file, logger=self.logger)
        self.email_handoff_queue = EmailHandoffQueue(self.email_handoff_queue_file, logger=self.logger)
        self.lead_state = LeadStateStore(base_dir / "data" / "lead_state.json")
        self.cadence_manager = CadenceManager(
            base_dir / "config" / "cadence_config.json",
            self.lead_state,
            base_dir / "data" / "processed_messages.json",
        )
        self.queue_manager = QueueManager(base_dir / "data" / "incoming_reply_queue.json")
        self.events = UnifiedEventLogger(base_dir / "logs" / "events.log")
        self.config.master_sheet_url = ""

    def _ensure_seed_data(self) -> None:
        return

    def _fetch_targets_from_pipedrive(self, limit: int = 100):
        return [
            {
                "lead_id": "pd-1",
                "deal_id": 1,
                "person_id": 10,
                "nome": "Lead Teste",
                "empresa": "MAND",
                "telefone": "5511999999999",
                "email": "lead@mand.test",
                "source": "pipedrive",
                "origem": "origem_casa_dos_dados",
                "tags": [],
                "status_bot": "",
                "stage_id": 1,
            }
        ][:limit]

    def _build_message(self, lead):
        return "Mensagem inicial de cadencia"

    def _already_sent_csv(self, phone: str) -> bool:
        return False

    def _record_sent_phone(self, lead, status: str = "SUCESSO") -> None:
        return

    def _send_message_real(self, lead, message: str) -> bool:
        phone = self._normalize_phone(lead.get("telefone", ""))
        self.sent_messages.append({"phone": phone, "message": message})
        self.events.log("SEND_MESSAGE", phone=phone, bot=self.bot_slot_name, message=message, result="success")
        return True

    def _build_pipedrive_client(self):
        return self.fake_client

    def suggest_reply_for_incoming(self, phone: str, inbound_messages: list[str], *, auto_send: bool = True):
        lead = self._find_crm_validated_target(phone) or self._fetch_targets_from_pipedrive(limit=1)[0]
        normalized_phone = self._normalize_phone(phone)
        self.lead_state.record_inbound(normalized_phone, lead=lead, status="active")
        self.events.log("INCOMING_REPLY", phone=normalized_phone, bot=self.bot_slot_name, message=inbound_messages[-1], result="detected")
        response = "Resposta AI simulada"
        self.fake_client.create_note(person_id=int(lead["person_id"]), deal_id=int(lead["deal_id"]), content=response)
        if auto_send:
            self._send_message_real(lead, response)
            self.lead_state.record_outbound(
                normalized_phone,
                cadence_step=self.cadence_step,
                bot=self.bot_slot_name,
                lead=lead,
                status="active",
            )
            self.events.log("REPLY_SENT", phone=normalized_phone, bot=self.bot_slot_name, message=response, result="success")
            return {"lead": lead, "response": response, "sent": True, "status": "respondida"}
        return {"lead": lead, "response": response, "sent": False, "status": "aguardando"}


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="wa-full-flow-", ignore_cleanup_errors=True) as tmp:
        base_dir = Path(tmp)
        for folder in ("config", "logs", "data"):
            (base_dir / folder).mkdir(parents=True, exist_ok=True)

        bot = TestWhatsAppBot(base_dir)
        sent = bot.run(limit=1)
        if sent != 0:
            raise AssertionError(f"Retorno inesperado no envio inicial: {sent}")
        if len(bot.sent_messages) != 1:
            raise AssertionError("Envio inicial nao ocorreu.")

        bot.queue_manager.enqueue(
            phone="5511999999999",
            message="Oi pode explicar melhor?",
            crm_mapped=True,
            source="test",
        )
        replies = bot.run_reply_queue(limit=1)
        if replies != 1:
            raise AssertionError(f"Fila de replies nao processou item pendente: {replies}")

        metrics = bot.queue_manager.get_queue_metrics()
        if metrics["pending"] != 0 or metrics["processed"] != 1:
            raise AssertionError(f"Metricas da fila invalidas: {metrics}")

        lead_state = bot.lead_state.get("5511999999999")
        if not lead_state.get("last_outbound_at") or not lead_state.get("last_inbound_at"):
            raise AssertionError(f"Estado persistente incompleto: {lead_state}")

        events_log = (base_dir / "logs" / "events.log").read_text(encoding="utf-8")
        for expected in ("SEND_MESSAGE", "INCOMING_REPLY", "REPLY_SENT"):
            if expected not in events_log:
                raise AssertionError(f"Evento ausente em events.log: {expected}")

        if not bot.fake_client.notes:
            raise AssertionError("CRM fake nao recebeu anotacao da resposta.")

        processed_registry = safe_read_json(base_dir / "data" / "processed_messages.json")
        if "5511999999999_cad1" not in processed_registry:
            raise AssertionError("Protecao de duplicidade nao registrou cadencia enviada.")

        print("FULL_FLOW_OK")
        print(f"QUEUE_METRICS={metrics}")
        print(f"SENT_MESSAGES={len(bot.sent_messages)}")
        print(f"CRM_NOTES={len(bot.fake_client.notes)}")
        logging.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
