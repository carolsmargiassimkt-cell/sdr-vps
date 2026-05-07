from __future__ import annotations

from core.auto_reply_guard import detect_auto_reply


SAMPLES = [
    "🎉 Bem-vindo(a) ao CDL Betim! Prefere receber informações sobre associar-se, conhecer nossos serviços ou falar diretamente com nossa equipe de atendimento?",
    "Digite 1 para financeiro, opção 2 para atendimento.",
    "Olá, este é nosso atendimento virtual. Escolha uma opção.",
    "Resposta automática do WhatsApp Business: fale conosco em horário comercial.",
    "Para falar com nossa equipe de atendimento, selecione uma opção.",
    "Oi Carol, pode me mandar um exemplo?",
]


def main() -> int:
    failures = 0
    for sample in SAMPLES:
        detected, reason = detect_auto_reply(sample)
        expected = "pode me mandar" not in sample.lower()
        status = "DETECTED" if detected else "NOT_DETECTED"
        print(f"{status} reason={reason or '-'} text={sample[:100]}")
        if expected and not detected:
            failures += 1
        if not expected and detected:
            failures += 1
    print("GO" if failures == 0 else "NO-GO")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
