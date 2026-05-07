from pathlib import Path

p = Path("supervisor.py")
txt = p.read_text(encoding="utf-8")
bak = p.with_suffix(".py.bak_strategy")
if not bak.exists():
    bak.write_text(txt, encoding="utf-8")

imports = """
# SDR_STRATEGY_PATCH
try:
    from sdr_core.strategy import resolve_sdr_strategy
    from sdr_core.wa_state import already_sent, mark_sent, stop_hunter
except Exception as _sdr_patch_err:
    resolve_sdr_strategy = None
    already_sent = mark_sent = stop_hunter = None
"""

if "SDR_STRATEGY_PATCH" not in txt:
    lines = txt.splitlines()
    insert_at = 0
    for i, line in enumerate(lines[:80]):
        if line.startswith("import ") or line.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, imports)
    txt = "\n".join(lines) + "\n"

guard = r'''
def sdr_should_send_hunter(deal, phone, step="hunter_1"):
    """
    Guard central para WA Hunter.
    Pipeline = estado macro; tags/campos/estado interno decidem envio.
    """
    deal_id = deal.get("id") or deal.get("deal_id")
    if not deal_id or not phone:
        print("[WA_HUNTER_SKIP] missing_deal_or_phone")
        return False

    if resolve_sdr_strategy:
        strategy, reasons = resolve_sdr_strategy(deal)
        print(f"[SDR_STRATEGY_RESOLVED] deal={deal_id} strategy={strategy} reasons={reasons}")
        if strategy != "hunter":
            if strategy in ("warm", "inbound_warm", "meeting") and stop_hunter:
                stop_hunter(deal_id, phone, "warm")
                print(f"[WA_HUNTER_STOP_WARM] deal={deal_id} phone={phone}")
            else:
                print(f"[WA_HUNTER_SKIP] deal={deal_id} strategy={strategy}")
            return False

    if already_sent and already_sent(deal_id, phone, step):
        print(f"[WA_HUNTER_DUP_GUARD] deal={deal_id} phone={phone} step={step}")
        return False

    return True


def sdr_mark_hunter_sent(deal, phone, step="hunter_1"):
    deal_id = deal.get("id") or deal.get("deal_id")
    if mark_sent and deal_id and phone:
        mark_sent(deal_id, phone, "hunter", step)
        print(f"[WA_HUNTER_SENT] deal={deal_id} phone={phone} step={step}")
'''

if "def sdr_should_send_hunter" not in txt:
    txt += "\n\n" + guard + "\n"

p.write_text(txt, encoding="utf-8")
print("SUPERVISOR_PATCH_OK")
