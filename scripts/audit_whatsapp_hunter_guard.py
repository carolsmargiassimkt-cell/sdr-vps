from __future__ import annotations

import argparse
from collections import Counter

from core.whatsapp_hunter_guard import (
    duplicate_day_log,
    mark_hunter_sent_today,
    render_hunter_message,
    should_send_hunter_today,
    today_brt,
)


def sample_deal(index: int) -> dict:
    return {
        "id": 9000 + index,
        "title": f"Empresa Teste {index}",
        "org_id": {"id": 7000 + index, "name": f"Empresa Teste {index}"},
        "person_id": {"id": 8000 + index, "name": f"Lead {index}"},
    }


def audit_messages(total: int = 30) -> int:
    messages = []
    for index in range(max(1, int(total or 30))):
        deal = sample_deal(index)
        phone = f"5531999{index:06d}"
        messages.append(render_hunter_message(deal, deal.get("person_id"), deal.get("org_id"), step="hunter_1", phone=phone, date_value=today_brt()))

    counts = Counter(messages)
    duplicated = sum(count - 1 for count in counts.values() if count > 1)
    print(f"total_exemplos={len(messages)}")
    print(f"mensagens_unicas={len(counts)}")
    print(f"duplicadas={duplicated}")
    print("exemplos_gerados:")
    for index, message in enumerate(messages[: max(1, min(30, len(messages)))], start=1):
        print(f"{index:02d}. {message}")
    if len(counts) <= 1:
        print("[WA_HUNTER_MESSAGE_AUDIT_FAIL] reason=all_messages_equal")
        return 1
    return 0


def simulate_guard() -> int:
    guard = {"date": today_brt(), "sent_by_phone": {}, "sent_by_deal": {}}
    deal_id = 12345
    phone = "5535999999999"
    allowed, last_sent_at, reason = should_send_hunter_today(deal_id, phone, guard)
    print(f"simulacao_primeiro_envio_allowed={1 if allowed else 0}")
    if not allowed:
        print(duplicate_day_log(deal_id, phone, last_sent_at, reason))
        return 1
    guard = mark_hunter_sent_today(deal_id, phone, sent_at=f"{today_brt()}T10:00:00-03:00", guard=guard, persist=False)
    allowed, last_sent_at, reason = should_send_hunter_today(deal_id, phone, guard)
    print(f"simulacao_segundo_envio_allowed={1 if allowed else 0}")
    if allowed:
        print("[WA_HUNTER_GUARD_AUDIT_FAIL] reason=second_send_not_blocked")
        return 1
    print(duplicate_day_log(deal_id, phone, last_sent_at, reason))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--examples", type=int, default=30)
    parser.add_argument("--simulate-guard", action="store_true")
    args = parser.parse_args()
    code = audit_messages(args.examples)
    if args.simulate_guard:
        code = max(code, simulate_guard())
    return code


if __name__ == "__main__":
    raise SystemExit(main())
