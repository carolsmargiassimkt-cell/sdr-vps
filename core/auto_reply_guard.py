from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.locked_json_state import locked_load_json, locked_save_json


BASE_DIR = Path(__file__).resolve().parents[1]
AUTO_REPLY_BLOCKLIST_FILE = BASE_DIR / "data" / "whatsapp_auto_reply_blocklist.json"
try:
    TZ = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ = timezone(timedelta(hours=-3))

PATTERNS = [
    r"\bbem[- ]?vindo",
    r"\batendimento\b",
    r"\bdigite\b",
    r"escolha uma op[cç][aã]o",
    r"\bmenu\b",
    r"associar[- ]?se",
    r"nossos servi[cç]os",
    r"equipe de atendimento",
    r"atendimento virtual",
    r"central de atendimento",
    r"chatbot",
    r"op[cç][aã]o\s*1",
    r"op[cç][aã]o\s*2",
    r"fale conosco",
    r"atendimento autom[aá]tico",
    r"auto atendimento",
    r"whatsapp business",
    r"resposta autom[aá]tica",
    r"prefere receber informa[cç][oõ]es",
    r"conhecer nossos servi[cç]os",
]


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) in {10, 11}:
        digits = "55" + digits
    return digits


def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    text = text.replace("\n", " ")
    return re.sub(r"\s+", " ", text).strip()


def detect_auto_reply(text: Any) -> tuple[bool, str]:
    normalized = normalize_text(text)
    if not normalized:
        return False, ""
    for pattern in PATTERNS:
        if re.search(pattern, normalized, flags=re.I):
            return True, pattern
    numbered_options = len(re.findall(r"(?:^|\s)(?:\d+|[1-9][\.)-])\s*(?:-|\.|\))?\s*[a-záéíóúãõç]", normalized))
    if numbered_options >= 2 and any(token in normalized for token in ("atendimento", "opção", "opcao", "serviços", "servicos")):
        return True, "numbered_institutional_menu"
    return False, ""


def is_auto_reply_blocked(phone: Any) -> bool:
    normalized = normalize_phone(phone)
    if not normalized:
        return False
    data = locked_load_json(AUTO_REPLY_BLOCKLIST_FILE, {})
    return normalized in data


def add_auto_reply_block(phone: Any, sample: Any, reason: str = "auto_reply_detected") -> bool:
    normalized = normalize_phone(phone)
    if not normalized:
        return False
    data = locked_load_json(AUTO_REPLY_BLOCKLIST_FILE, {})
    data[normalized] = {
        "detected_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "reason": reason,
        "sample": str(sample or "")[:500],
    }
    locked_save_json(AUTO_REPLY_BLOCKLIST_FILE, data)
    print(f"[AUTO_REPLY_BLOCKLIST_ADD] phone={normalized} reason={reason}")
    return True
