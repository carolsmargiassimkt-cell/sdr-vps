from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crm.pipedrive_client import PipedriveClient


PIPELINE_ID = 2
MAX_KEEP_PARTICIPANTS = 3
OVERLOADED_PARTICIPANT_THRESHOLD = 20
REPORT_JSON = Path("/root/sdr-vps/logs/pipeline2_holiday_hygiene_report.json")


def normalize_cnpj(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) == 14 else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) not in {10, 11}:
        return ""
    if len(set(digits)) <= 2 or digits.startswith("00"):
        return ""
    return digits


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text.partition("@")[2] else ""


def normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        for key in ("value", "id"):
            try:
                parsed = int(value.get(key) or 0)
            except Exception:
                parsed = 0
            if parsed > 0:
                return parsed
        return 0
    try:
        return int(value or 0)
    except Exception:
        return 0


def parse_created_at(value: Any) -> tuple[str, int]:
    token = str(value or "").strip()
    if not token:
        return ("9999-12-31 23:59:59", 0)
    normalized = token.replace("T", " ").replace("Z", "").split(".")[0].strip()
    return (normalized or "9999-12-31 23:59:59", 0)


def select_oldest_master(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    items = [dict(item or {}) for item in list(records or []) if extract_id((item or {}).get("id")) > 0]
    if not items:
        return {}
    return sorted(
        items,
        key=lambda item: (
            parse_created_at(item.get("add_time") or item.get("created_at") or item.get("update_time"))[0],
            extract_id(item.get("id")),
        ),
    )[0]


def merge_contact_values(*collections: Iterable[str]) -> List[str]:
    merged: List[str] = []
    for collection in collections:
        for item in list(collection or []):
            token = str(item or "").strip()
            if token and token not in merged:
                merged.append(token)
    return merged


def person_contact_summary(person: Dict[str, Any], target_org_id: int = 0) -> Dict[str, Any]:
    phones = []
    for item in list((person or {}).get("phone") or []):
        value = item.get("value") if isinstance(item, dict) else item
        phone = normalize_phone(value)
        if phone and phone not in phones:
            phones.append(phone)
    emails = []
    for item in list((person or {}).get("email") or []):
        value = item.get("value") if isinstance(item, dict) else item
        email = normalize_email(value)
        if email and email not in emails:
            emails.append(email)
    person_org_id = extract_id((person or {}).get("org_id"))
    valid = bool(phones or emails)
    org_match = not target_org_id or person_org_id in {0, int(target_org_id)}
    return {
        "phones": phones,
        "emails": emails,
        "person_org_id": person_org_id,
        "valid": bool(valid and org_match),
        "org_match": org_match,
    }


def organization_group_key(client: PipedriveClient, organization: Dict[str, Any]) -> str:
    cnpj = normalize_cnpj(client.extract_cnpj(organization))
    if cnpj:
        return f"cnpj:{cnpj}"
    return f"name:{normalize_name((organization or {}).get('name'))}"


def person_group_key(person: Dict[str, Any]) -> str:
    summary = person_contact_summary(person)
    if summary["emails"]:
        return f"email:{summary['emails'][0]}"
    if summary["phones"]:
        return f"phone:{summary['phones'][0]}"
    return f"name:{normalize_name((person or {}).get('name'))}"


def consistent_group_cnpj(client: PipedriveClient, organizations: Iterable[Dict[str, Any]]) -> str:
    values = {normalize_cnpj(client.extract_cnpj(item)) for item in list(organizations or []) if normalize_cnpj(client.extract_cnpj(item))}
    return next(iter(values)) if len(values) == 1 else ""


def build_deal_hygiene_plan(
    *,
    client: PipedriveClient,
    deal: Dict[str, Any],
    organization: Dict[str, Any],
    primary_person: Dict[str, Any],
    participants: List[Dict[str, Any]],
    org_group: List[Dict[str, Any]],
    person_group_index: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    deal_id = extract_id((deal or {}).get("id"))
    current_org_id = extract_id((deal or {}).get("org_id"))
    current_person_id = extract_id((deal or {}).get("person_id"))
    stage_id = extract_id((deal or {}).get("stage_id"))
    if int((deal or {}).get("pipeline_id") or 0) != PIPELINE_ID:
        return {"deal_id": deal_id, "safe": False, "skip_reason": "outside_pipeline_2"}

    master_org = select_oldest_master(org_group)
    master_org_id = extract_id(master_org.get("id"))
    target_cnpj = consistent_group_cnpj(client, org_group)
    if not master_org_id:
        return {"deal_id": deal_id, "safe": False, "skip_reason": "org_master_unclear"}
    if not target_cnpj and any(normalize_cnpj(client.extract_cnpj(item)) for item in org_group):
        return {"deal_id": deal_id, "safe": False, "skip_reason": "org_cnpj_conflict"}

    candidate_people: List[Dict[str, Any]] = []
    if primary_person:
        candidate_people.append(dict(primary_person))
    for participant in list(participants or []):
        person = dict(participant.get("person") or {})
        if person:
            candidate_people.append(person)

    grouped_people: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for person in candidate_people:
        summary = person_contact_summary(person, target_org_id=master_org_id)
        if not summary["valid"]:
            continue
        key = person_group_key(person)
        if key:
            grouped_people[key].append(dict(person))

    for key, items in (person_group_index or {}).items():
        if key in grouped_people:
            grouped_people[key] = merge_people_groups(grouped_people[key], items, target_org_id=master_org_id)

    master_people = [select_oldest_master(items) for items in grouped_people.values() if items]
    master_people = [item for item in master_people if extract_id(item.get("id")) > 0]
    master_people = sorted(
        master_people,
        key=lambda item: (
            parse_created_at(item.get("add_time") or item.get("created_at") or item.get("update_time"))[0],
            extract_id(item.get("id")),
        ),
    )[:MAX_KEEP_PARTICIPANTS]
    if not master_people:
        return {"deal_id": deal_id, "safe": False, "skip_reason": "person_master_unclear"}

    master_person = master_people[0]
    master_person_id = extract_id(master_person.get("id"))
    keep_person_ids = {extract_id(item.get("id")) for item in master_people if extract_id(item.get("id")) > 0}
    existing_participants = {extract_id((item.get("person_id") or {}).get("id") if isinstance(item.get("person_id"), dict) else item.get("person_id")) for item in participants}

    participant_count = len(list(participants or []))
    remove_participants = []
    for participant in list(participants or []):
        participant_row_id = extract_id(participant.get("id"))
        participant_person_id = extract_id(participant.get("person_id") or participant.get("person"))
        person_payload = dict(participant.get("person") or {})
        summary = person_contact_summary(person_payload, target_org_id=master_org_id)
        if participant_person_id not in keep_person_ids or not summary["valid"] or participant_count > OVERLOADED_PARTICIPANT_THRESHOLD:
            if participant_row_id > 0:
                remove_participants.append(participant_row_id)

    add_participants = [person_id for person_id in keep_person_ids if person_id not in existing_participants and person_id != master_person_id]
    deal_update: Dict[str, Any] = {}
    if current_org_id != master_org_id:
        deal_update["org_id"] = master_org_id
    if current_person_id != master_person_id:
        deal_update["person_id"] = master_person_id
    deal_update.pop("stage_id", None)

    org_update: Dict[str, Any] = {}
    current_master_cnpj = normalize_cnpj(client.extract_cnpj(master_org))
    if target_cnpj and current_master_cnpj in {"", target_cnpj} and current_master_cnpj != target_cnpj:
        org_update[client.CNPJ_FIELD_KEY] = target_cnpj

    person_updates: Dict[int, Dict[str, Any]] = {}
    for person in master_people:
        person_id = extract_id(person.get("id"))
        if person_id <= 0:
            continue
        summary = person_contact_summary(person, target_org_id=master_org_id)
        if not summary["valid"]:
            continue
        if summary["person_org_id"] != master_org_id:
            person_updates[person_id] = {"org_id": master_org_id}

    return {
        "deal_id": deal_id,
        "pipeline_id": PIPELINE_ID,
        "stage_id_preserved": stage_id,
        "safe": True,
        "skip_reason": "",
        "target_org_id": master_org_id,
        "target_person_id": master_person_id,
        "deal_update": deal_update,
        "org_update": org_update,
        "person_updates": person_updates,
        "add_participants": sorted(add_participants),
        "remove_participants": sorted(set(remove_participants)),
        "participant_count": participant_count,
        "reason": "pipeline2_validated_holiday_hygiene",
        "master_entity": {
            "deal_id": deal_id,
            "org_id": master_org_id,
            "person_id": master_person_id,
        },
        "consolidated_entities": {
            "org_ids": sorted({extract_id(item.get("id")) for item in org_group if extract_id(item.get("id")) > 0}),
            "person_ids": sorted(keep_person_ids),
        },
    }


def merge_people_groups(
    left: Iterable[Dict[str, Any]],
    right: Iterable[Dict[str, Any]],
    *,
    target_org_id: int,
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen = set()
    for person in list(left or []) + list(right or []):
        person_id = extract_id((person or {}).get("id"))
        summary = person_contact_summary(person, target_org_id=target_org_id)
        if person_id <= 0 or not summary["valid"] or person_id in seen:
            continue
        seen.add(person_id)
        merged.append(dict(person or {}))
    return merged


def execute_plan(client: PipedriveClient, plan: Dict[str, Any], *, apply: bool) -> Dict[str, Any]:
    result = {
        "deal_id": int(plan.get("deal_id") or 0),
        "applied": False,
        "safe": bool(plan.get("safe")),
        "skip_reason": str(plan.get("skip_reason") or "").strip(),
        "before_after": {
            "deal_update": dict(plan.get("deal_update") or {}),
            "org_update": dict(plan.get("org_update") or {}),
            "person_updates": dict(plan.get("person_updates") or {}),
            "add_participants": list(plan.get("add_participants") or []),
            "remove_participants": list(plan.get("remove_participants") or []),
            "stage_id_preserved": int(plan.get("stage_id_preserved") or 0),
        },
        "master_entity": dict(plan.get("master_entity") or {}),
        "consolidated_entities": dict(plan.get("consolidated_entities") or {}),
    }
    if not bool(plan.get("safe")):
        return result
    if not apply:
        result["applied"] = False
        return result

    deal_id = int(plan.get("deal_id") or 0)
    target_org_id = int(plan.get("target_org_id") or 0)
    if int(plan.get("pipeline_id") or 0) != PIPELINE_ID:
        result["skip_reason"] = "outside_pipeline_2"
        return result

    for participant_id in list(plan.get("remove_participants") or []):
        client.remove_deal_participant(deal_id, int(participant_id))
    for person_id in list(plan.get("add_participants") or []):
        client.add_deal_participant(deal_id, int(person_id))
    for person_id, payload in dict(plan.get("person_updates") or {}).items():
        client.update_person(int(person_id), dict(payload or {}))
    if target_org_id and dict(plan.get("org_update") or {}):
        client.update_organization(target_org_id, dict(plan.get("org_update") or {}))
    if dict(plan.get("deal_update") or {}):
        sanitized = {key: value for key, value in dict(plan.get("deal_update") or {}).items() if key != "stage_id"}
        if sanitized:
            client.update_deal(deal_id, sanitized)

    client.add_note(
        deal_id=deal_id,
        content=(
            f"Higienizacao segura pipeline 2.\n"
            f"Motivo: {str(plan.get('reason') or '').strip()}\n"
            f"Master: {json.dumps(plan.get('master_entity') or {}, ensure_ascii=False)}\n"
            f"Consolidadas: {json.dumps(plan.get('consolidated_entities') or {}, ensure_ascii=False)}\n"
            f"Antes/depois: {json.dumps(result['before_after'], ensure_ascii=False)}"
        ),
    )
    result["applied"] = True
    return result


def collect_live_snapshot(client: PipedriveClient, *, limit: int) -> Dict[str, Any]:
    deals = [dict(item or {}) for item in list(client.get_deals(status="open", limit=max(1, int(limit)), pipeline_id=PIPELINE_ID) or [])]
    org_cache: Dict[int, Dict[str, Any]] = {}
    person_cache: Dict[int, Dict[str, Any]] = {}
    participants_cache: Dict[int, List[Dict[str, Any]]] = {}
    for deal in deals:
        org_id = extract_id(deal.get("org_id"))
        person_id = extract_id(deal.get("person_id"))
        deal_id = extract_id(deal.get("id"))
        if org_id > 0 and org_id not in org_cache:
            org_cache[org_id] = client.get_organization(org_id)
        if person_id > 0 and person_id not in person_cache:
            person_cache[person_id] = client.get_person_details(person_id)
        if deal_id > 0 and deal_id not in participants_cache:
            rows = []
            for participant in list(client.get_deal_participants(deal_id, limit=100) or []):
                person_id = extract_id(participant.get("person_id") or participant.get("person"))
                person = client.get_person_details(person_id) if person_id > 0 else {}
                rows.append({**dict(participant or {}), "person": dict(person or {})})
                if person_id > 0 and person_id not in person_cache:
                    person_cache[person_id] = dict(person or {})
            participants_cache[deal_id] = rows
    return {
        "deals": deals,
        "org_cache": org_cache,
        "person_cache": person_cache,
        "participants_cache": participants_cache,
    }


def build_hygiene_plans(client: PipedriveClient, snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    deals = list(snapshot.get("deals") or [])
    org_cache = dict(snapshot.get("org_cache") or {})
    person_cache = dict(snapshot.get("person_cache") or {})
    participants_cache = dict(snapshot.get("participants_cache") or {})
    org_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    person_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for organization in org_cache.values():
        key = organization_group_key(client, organization)
        if key:
            org_groups[key].append(dict(organization or {}))
    for person in person_cache.values():
        key = person_group_key(person)
        if key:
            person_groups[key].append(dict(person or {}))

    plans = []
    for deal in deals:
        org_id = extract_id(deal.get("org_id"))
        person_id = extract_id(deal.get("person_id"))
        deal_id = extract_id(deal.get("id"))
        organization = dict(org_cache.get(org_id) or {})
        org_key = organization_group_key(client, organization) if organization else ""
        plan = build_deal_hygiene_plan(
            client=client,
            deal=deal,
            organization=organization,
            primary_person=dict(person_cache.get(person_id) or {}),
            participants=list(participants_cache.get(deal_id) or []),
            org_group=list(org_groups.get(org_key) or ([organization] if organization else [])),
            person_group_index=person_groups,
        )
        plans.append(plan)
    return plans


def main() -> int:
    parser = argparse.ArgumentParser(description="Higienizacao segura do CRM restrita ao funil 2.")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--report", default=str(REPORT_JSON))
    args = parser.parse_args()

    client = PipedriveClient()
    snapshot = collect_live_snapshot(client, limit=max(1, int(args.limit or 500)))
    plans = build_hygiene_plans(client, snapshot)
    counters: Counter[str] = Counter()
    results = []
    for plan in plans:
        if bool(plan.get("safe")):
            counters["safe"] += 1
        else:
            counters[f"skip_{str(plan.get('skip_reason') or 'unknown')}"] += 1
        result = execute_plan(client, plan, apply=bool(args.apply))
        if result.get("applied"):
            counters["applied"] += 1
        results.append(result)

    report = {
        "generated_at": datetime.now().isoformat(),
        "apply": bool(args.apply),
        "pipeline_id": PIPELINE_ID,
        "counters": dict(counters),
        "results": results,
    }
    target = Path(str(args.report or REPORT_JSON))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        "[PIPELINE2_HOLIDAY_HYGIENE] "
        + " ".join(f"{key}={value}" for key, value in sorted(counters.items()))
        + f" report={target}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
