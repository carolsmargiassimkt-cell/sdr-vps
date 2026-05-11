import json
from datetime import datetime
from pathlib import Path

p = Path("/root/sdr-vps/data/whatsapp_warm_cadence.json")
data = json.load(open(p))

fix = {
    "3430": ("Adenilsa Batista", "adenilsa1104@gmail.com", "5581973318566"),
    "3424": ("Andriele Andrade de moura", "andrieleandradedemouraandriele@gmail.com", "5555984215377"),
    "3431": ("Daniele Vieira", "danieletrendnordeste@gmail.com", "5586981161858"),
}

for did, (name, email, phone) in fix.items():
    row = data.get(did) or {}
    row.update({
        "deal_id": did,
        "name": name,
        "email": email,
        "phone": phone,
        "wa1_sent": False,
        "step": 0,
        "last_sent_step_whatsapp": 0,
        "send_status": "gateway_false_positive_reset",
        "paused": True,
        "paused_reason": "gateway_returned_sent_but_message_not_visible",
        "manual_action_needed": True,
        "fixed_at": datetime.now().isoformat(timespec="seconds"),
    })
    data[did] = row

json.dump(data, open(p, "w"), ensure_ascii=False, indent=2)
print("[RESET_OK] false positives resetados e telefones corrigidos")
