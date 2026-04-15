from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient


INPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_offline_writeback_20260410.csv")
STATE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\runtime\super_minas_rescue_ready_rows_state.json")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\super_minas_rescue_ready_rows_report.json")
OUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\super_minas_rescued_writeback_targets_20260410.csv")
CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
PIPELINE_ID = 2
EXCLUDED_COMPANIES = {"Forno de Minas", "Grupo Vibra", "Produtos Búfalo"}
BLOCKED_PHONES = {
    "18043309723",
    "1804330972",
    "11999999999",
    "11999998888",
    "00999999977",
    "1155555555",
    "999999999",
}


def log(message: str) -> None:
    print(str(message), flush=True)


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_state() -> Dict[str, Any]:
    if not STATE_JSON.exists():
        return {"done": []}
    try:
        payload = json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"done": []}
    return payload if isinstance(payload, dict) else {"done": []}


def save_state(state: Dict[str, Any]) -> None:
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def save_report(counters: Counter[str], done_count: int) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(
        json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "done_count": done_count,
                "counters": dict(sorted(counters.items())),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def write_output(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    merged: Dict[str, Dict[str, Any]] = {}
    if OUT_CSV.exists():
        for row in read_csv(OUT_CSV):
            merged[row_key(row)] = dict(row)
    for row in rows:
        merged[row_key(row)] = dict(row)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(list(merged.values()))


def row_key(row: Dict[str, Any]) -> str:
    return str(row.get("company_input") or "").strip().lower()


def normalize_cnpj(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) == 14 else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) not in {10, 11}:
        return ""
    if digits in BLOCKED_PHONES or len(set(digits)) <= 2:
        return ""
    return digits


def normalize_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text or text == "nan":
        return ""
    return text if "@" in text and "." in text.split("@")[-1] else ""


def split_pipe(value: Any) -> List[str]:
    return [str(item).strip() for item in str(value or "").split("|") if str(item).strip() and str(item).strip().lower() != "nan"]


def collect_phones(*values: Any) -> List[str]:
    output: List[str] = []
    for value in values:
        for item in split_pipe(value) or [value]:
            phone = normalize_phone(item)
            if phone and phone not in output:
                output.append(phone)
    return output


def collect_emails(*values: Any) -> List[str]:
    output: List[str] = []
    for value in values:
        for item in split_pipe(value) or [value]:
            email = normalize_email(item)
            if email and email not in output:
                output.append(email)
    return output


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        try:
            return int(value.get("value") or value.get("id") or 0)
        except Exception:
            return 0
    try:
        return int(value or 0)
    except Exception:
        return 0


def deal_title(row: Dict[str, Any]) -> str:
    return str(row.get("company_input") or row.get("matched_name") or "Super Minas").strip()[:120]


def normalize_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def choose_stage_id(client: PipedriveClient) -> int:
    stages = [stage for stage in client.get_stages() if int(stage.get("pipeline_id") or 0) == PIPELINE_ID]
    stages.sort(key=lambda stage: (int(stage.get("order_nr") or 0), int(stage.get("id") or 0)))
    return int((stages[0] or {}).get("id") or 0) if stages else 0


def build_org_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"name": str(row.get("matched_name") or row.get("company_input") or "").strip()[:120]}
    cnpj = normalize_cnpj(row.get("cnpj"))
    address = str(row.get("address") or "").strip()
    if cnpj:
        payload[CNPJ_FIELD_KEY] = cnpj
    if address and address.lower() != "nan":
        payload["address"] = address[:255]
    return payload


def build_person_payload(row: Dict[str, Any], org_id: int) -> Dict[str, Any]:
    phones = collect_phones(row.get("phones_exclusive"))
    emails = collect_emails(row.get("emails_exclusive"))
    payload: Dict[str, Any] = {
        "name": str(row.get("company_input") or row.get("matched_name") or "Responsável").strip()[:120],
        "org_id": int(org_id),
    }
    if phones:
        payload["phone"] = [{"value": phone, "primary": idx == 0, "label": "work"} for idx, phone in enumerate(phones[:5])]
    if emails:
        payload["email"] = [{"value": email, "primary": idx == 0, "label": "work"} for idx, email in enumerate(emails[:5])]
    return payload


def build_note(row: Dict[str, Any]) -> str:
    lines = ["Resgate offline Super Minas 2026-04-10"]
    cnpj = normalize_cnpj(row.get("cnpj"))
    if cnpj:
        lines.append(f"CNPJ: {cnpj}")
    phones = collect_phones(row.get("phones_exclusive"))
    emails = collect_emails(row.get("emails_exclusive"))
    websites = split_pipe(row.get("websites"))
    decision_makers = split_pipe(row.get("decision_makers"))
    address = str(row.get("address") or "").strip()
    if phones:
        lines.append("Telefones: " + " | ".join(phones[:10]))
    if emails:
        lines.append("Emails: " + " | ".join(emails[:10]))
    if websites:
        lines.append("Sites: " + " | ".join(websites[:10]))
    if decision_makers:
        lines.append("Sócios/decisores: " + " | ".join(decision_makers[:10]))
    if address and address.lower() != "nan":
        lines.append("Endereço: " + address[:800])
    return "\n".join(lines)


def ensure_person(client: PipedriveClient, row: Dict[str, Any], org_id: int) -> int:
    phones = collect_phones(row.get("phones_exclusive"))
    emails = collect_emails(row.get("emails_exclusive"))
    for email in emails:
        found = client.find_person(org_id=org_id, email=email)
        found_id = extract_id(found.get("id"))
        if found_id:
            return found_id
    for phone in phones:
        found = client.find_person(org_id=org_id, phone=phone)
        found_id = extract_id(found.get("id"))
        if found_id:
            return found_id
    people = client.get_organization_persons(org_id, limit=100)
    if people:
        return extract_id(people[0].get("id"))
    payload = build_person_payload(row, org_id)
    if not payload.get("phone") and not payload.get("email"):
        return 0
    created = client.create_person(payload)
    return extract_id(created.get("id"))


def find_org_by_name(client: PipedriveClient, *names: Any) -> Dict[str, Any]:
    for name in names:
        clean = str(name or "").strip()
        if not clean:
            continue
        for item in client.find_organization_by_name(clean):
            org = dict(item.get("item") or {})
            org_id = extract_id(org.get("id"))
            if not org_id:
                continue
            full = client.get_organization(org_id)
            if full and normalize_name(full.get("name")) == normalize_name(clean):
                return full
    return {}


def find_open_pipeline2_deal(client: PipedriveClient, org_id: int, title: str) -> Dict[str, Any]:
    for deal in client.find_open_deals_by_person_or_org(org_id=org_id):
        if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            continue
        if str(deal.get("title") or "").strip().lower() == str(title or "").strip().lower():
            return deal
    for deal in client.find_open_deals_by_person_or_org(org_id=org_id):
        if int(deal.get("pipeline_id") or 0) == PIPELINE_ID:
            return deal
    return {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=60)
    parser.add_argument("--delay", type=float, default=1.2)
    args = parser.parse_args()

    all_rows = read_csv(INPUT_CSV)
    target_rows = [
        row
        for row in all_rows
        if str(row.get("writeback_ready") or "").strip().lower() == "yes"
        and not str(row.get("deal_id") or "").strip()
        and str(row.get("company_input") or "").strip() not in EXCLUDED_COMPANIES
    ]
    target_rows.sort(key=lambda row: str(row.get("company_input") or "").strip().lower())

    state = load_state()
    done = set(str(item) for item in list(state.get("done") or []))
    client = PipedriveClient()
    counters: Counter[str] = Counter()
    output_rows: List[Dict[str, Any]] = []

    if not client.test_connection():
        log(f"[PIPEDRIVE_FAIL] status={client.last_http_status} body={client.last_http_body[:200]}")
        return 2

    stage_id = choose_stage_id(client)
    planned_rows = [row for row in target_rows if row_key(row) not in done]
    log(f"[SUPER_MINAS_RESCUE_START] rows={len(planned_rows)} apply={int(args.apply)} stage_id={stage_id}")
    for index, row in enumerate(planned_rows[: max(1, int(args.limit or 1))], start=1):
        company = str(row.get("company_input") or "").strip()
        cnpj = normalize_cnpj(row.get("cnpj"))
        org = client.find_organization_by_cnpj(cnpj) if cnpj else {}
        org_id = extract_id(org.get("id"))
        action = "reuse_org"
        if not org_id:
            org = find_org_by_name(client, row.get("matched_name"), row.get("company_input"))
            org_id = extract_id(org.get("id"))
        if not org_id:
            org = client.create_organization(build_org_payload(row)) if args.apply else {}
            org_id = extract_id(org.get("id"))
            action = "create_org"
        person_id = ensure_person(client, row, org_id) if org_id and args.apply else 0
        current_deal = find_open_pipeline2_deal(client, org_id, deal_title(row)) if org_id else {}
        deal_id = extract_id(current_deal.get("id"))
        if not deal_id and args.apply and org_id:
            payload: Dict[str, Any] = {
                "title": deal_title(row),
                "org_id": org_id,
                "pipeline_id": PIPELINE_ID,
                "label": ["SUPER_MINAS"],
            }
            if stage_id:
                payload["stage_id"] = stage_id
            if person_id:
                payload["person_id"] = person_id
            created = client.create_deal(payload)
            deal_id = extract_id(created.get("id"))
            if deal_id:
                current_deal = created
                action = "create_deal" if action != "create_org" else "create_org_deal"
        elif deal_id:
            action = "reuse_deal" if action != "create_org" else "create_org_reuse_deal"

        plan_line = (
            f"[PLAN] {index}/{min(len(planned_rows), int(args.limit or 1))} "
            f"company={company} cnpj={cnpj} org_id={org_id or 0} deal_id={deal_id or 0} action={action}"
        )
        log(plan_line)

        if not args.apply:
            counters["dry_run"] += 1
            continue

        if not org_id:
            counters["org_error"] += 1
            continue
        if action.startswith("create_org"):
            counters["org_created"] += 1
        else:
            counters["org_reused"] += 1
        if person_id:
            counters["person_ready"] += 1
        if not deal_id:
            counters["deal_error"] += 1
            continue
        if action.endswith("create_deal") or action == "create_deal":
            counters["deal_created"] += 1
        else:
            counters["deal_reused"] += 1

        note_content = build_note(row)
        if note_content and client.add_note(deal_id=deal_id, content=note_content):
            counters["notes_added"] += 1

        row["deal_id"] = str(deal_id)
        row["org_id"] = str(org_id)
        output_rows.append(dict(row))
        counters["ok"] += 1
        done.add(row_key(row))
        if counters["ok"] % 10 == 0:
            save_state({"done": sorted(done)})
            save_report(counters, len(done))
            write_output(output_rows)
        time.sleep(float(args.delay))

    save_state({"done": sorted(done)})
    save_report(counters, len(done))
    write_output(output_rows)
    log("[SUPER_MINAS_RESCUE_END] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
