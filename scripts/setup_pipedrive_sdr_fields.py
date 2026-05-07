import os, requests, json
from pathlib import Path

TOKEN=os.getenv("PIPEDRIVE_API_TOKEN") or os.getenv("PIPEDRIVE_TOKEN")
BASE="https://api.pipedrive.com/v1"
OUT=Path("data/pipedrive_sdr_fields.json")
OUT.parent.mkdir(exist_ok=True)

FIELDS=[
  ("sdr_lead_source","varchar"),
  ("sdr_warm_source","varchar"),
  ("sdr_last_intent","varchar"),
  ("sdr_last_touch_channel","varchar"),
  ("sdr_automation_status","varchar"),
  ("sdr_conversation_phase","varchar"),
  ("sdr_email_clicked","varchar"),
  ("sdr_clicked_step","varchar"),
  ("sdr_meeting_status","varchar"),
  ("sdr_contamination_status","varchar"),
  ("sdr_last_event_at","varchar"),
]

existing=requests.get(f"{BASE}/dealFields",params={"api_token":TOKEN},timeout=30).json().get("data") or []
by_name={f["name"]:f for f in existing}

mapping={}

for name,field_type in FIELDS:
    if name in by_name:
        mapping[name]=by_name[name]["key"]
        print("EXISTS",name,by_name[name]["key"])
        continue

    r=requests.post(
        f"{BASE}/dealFields",
        params={"api_token":TOKEN},
        json={"name":name,"field_type":field_type},
        timeout=30
    )
    print("CREATE",name,r.status_code,r.text[:200])
    data=r.json().get("data") or {}
    if data.get("key"):
        mapping[name]=data["key"]

OUT.write_text(json.dumps(mapping,ensure_ascii=False,indent=2))
print("SAVED",OUT)
print(json.dumps(mapping,indent=2,ensure_ascii=False))
