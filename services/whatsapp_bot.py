from __future__ import annotations

from typing import Any

from whatsapp_bot import WhatsAppBot as RootWhatsAppBot
from whatsapp_bot import build_default_bot, executar_campanha_whatsapp


WhatsAppBot = RootWhatsAppBot


def validar_numero(numero: Any) -> bool:
    return WhatsAppBot(real_send=False).validar_numero(numero)


def enviar_mensagem(numero: Any, mensagem: str) -> bool:
    bot = build_default_bot(real_send=True)
    return bool(bot.enviar_mensagem(numero, mensagem))


__all__ = [
    "WhatsAppBot",
    "enviar_mensagem",
    "executar_campanha_whatsapp",
    "validar_numero",
]
