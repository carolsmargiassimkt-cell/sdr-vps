from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from utils.safe_json import safe_read_json, safe_write_json


DEFAULT_TEMPLATE = {
    "subject": "Conversa rápida",
    "body": (
        "Olá {nome},\n\n"
        "O {nome_bot} comentou comigo que você pediu contato.\n\n"
        "Sou a Carol da Mand Digital.\n\n"
        "Podemos falar rapidinho essa semana?\n\n"
        "Carol"
    ),
}


class EmailCampaignManager:
    def __init__(self, *, template_file: Path | str, logger=None) -> None:
        self.template_file = Path(template_file)
        self.template_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        if not self.template_file.exists():
            safe_write_json(self.template_file, DEFAULT_TEMPLATE)

    def load_template(self) -> Dict[str, str]:
        payload = safe_read_json(self.template_file)
        if not isinstance(payload, dict):
            return dict(DEFAULT_TEMPLATE)
        return {
            "subject": str(payload.get("subject", DEFAULT_TEMPLATE["subject"])).strip(),
            "body": str(payload.get("body", DEFAULT_TEMPLATE["body"])).strip(),
        }

    def save_template(self, *, subject: str, body: str) -> None:
        safe_write_json(
            self.template_file,
            {
                "subject": str(subject or DEFAULT_TEMPLATE["subject"]).strip(),
                "body": str(body or DEFAULT_TEMPLATE["body"]).strip(),
            },
        )

    def send_with_outlook(self, *, to_email: str, subject: str, body: str) -> bool:
        try:
            import win32com.client  # type: ignore

            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = str(to_email or "").strip()
            mail.Subject = str(subject or "").strip()
            mail.Body = str(body or "").strip()
            mail.Send()
            return True
        except Exception as exc:
            if self.logger:
                self.logger.error(f"erro email | destinatario={to_email} | {exc}")
            return False

    def render(self, *, nome: str, nome_bot: str = "bot") -> Dict[str, str]:
        template = self.load_template()
        context = {"nome": str(nome or "").strip() or "contato", "nome_bot": str(nome_bot or "bot").strip()}
        return {
            "subject": template["subject"].format(**context),
            "body": template["body"].format(**context),
        }
