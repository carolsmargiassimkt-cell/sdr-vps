from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.local_crm_cache import LocalCRMCache
from crm.pipedrive_client import PipedriveClient
from utils.safe_json import safe_read_json, safe_write_json

LOCAL_BASE_FILE = PROJECT_ROOT / "data" / "local_crm_base.json"
REPORT_FILE = PROJECT_ROOT / "logs" / "weekly_crm_cleanup_report.json"
PROSPECCAO_PIPELINE_ID = 2
ALLOW_DEAL_CHANGES = False
ALLOW_NOTE_CREATION = False


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_records() -> List[Dict[str, Any]]:
    payload = safe_read_json(LOCAL_BASE_FILE) if LOCAL_BASE_FILE.exists() else {}
    if not isinstance(payload, dict):
        return []
    return [dict(item) for item in payload.get("records", []) if isinstance(item, dict)]


def merge_list(values: Iterable[Any], *, normalize=None) -> List[str]:
    output: List[str] = []
    seen = set()
    for value in values:
        clean = str(value or "").strip()
        if normalize is not None:
            clean = normalize(clean)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        output.append(clean)
    return output


def _clean_company_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"^\s*neg[oó]cio\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*-\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\[ENRIQUECIDO\]\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\|\s*\d{14}\s*$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" -|")
    return text


def canonical_company_name(record: Dict[str, Any]) -> str:
    candidates = [
        record.get("empresa"),
        record.get("nome"),
        record.get("organization_name"),
        record.get("org_name"),
    ]
    aliases = record.get("aliases") or []
    if isinstance(aliases, list):
        candidates.extend(aliases)
    cleaned = [_clean_company_token(item) for item in candidates if _clean_company_token(item)]
    if cleaned:
        cleaned.sort(key=lambda item: (len(item), item.lower()))
        return cleaned[0]
    return _clean_company_token(record.get("empresa")) or str(record.get("empresa") or "").strip()


def canonical_deal_title(company_name: str) -> str:
    clean = _clean_company_token(company_name)
    return f"Negócio {clean}".strip() if clean else ""


def build_conflict_index(records: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        values = []
        if field == "phone":
            values = merge_list([record.get("telefone")] + list(record.get("phones") or []), normalize=LocalCRMCache.normalize_phone)
        elif field == "email":
            values = merge_list([record.get("email")] + list(record.get("emails") or []), normalize=LocalCRMCache.normalize_email)
        elif field == "cnpj":
            values = merge_list([record.get("cnpj")], normalize=LocalCRMCache.normalize_cnpj)
        else:
            values = merge_list([record.get(field)])
        seen_record = set()
        for value in values:
            signature = (int(record.get("deal_id", 0) or 0), int(record.get("person_id", 0) or 0))
            if signature in seen_record:
                continue
            seen_record.add(signature)
            index[value].append(record)
    return index


def duplicate_report(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {"phone": [], "email": [], "cnpj": [], "deal_company": []}
    for field in ("phone", "email", "cnpj"):
        index = build_conflict_index(records, field)
        for value, items in index.items():
            signatures = {(int(item.get("deal_id", 0) or 0), int(item.get("person_id", 0) or 0)) for item in items}
            if len(signatures) <= 1:
                continue
            buckets[field].append(
                {
                    "key": value,
                    "records": [
                        {
                            "empresa": item.get("empresa"),
                            "deal_id": item.get("deal_id"),
                            "person_id": item.get("person_id"),
                            "source": item.get("source"),
                        }
                        for item in items
                    ],
                }
            )
    company_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        company_key = LocalCRMCache.normalize_cnpj(record.get("cnpj")) or str(record.get("empresa") or "").strip().upper()
        if not company_key:
            continue
        company_index[company_key].append(record)
    for value, items in company_index.items():
        deal_ids = {int(item.get("deal_id", 0) or 0) for item in items if int(item.get("deal_id", 0) or 0)}
        if len(deal_ids) <= 1:
            continue
        buckets["deal_company"].append(
            {
                "key": value,
                "records": [
                    {
                        "empresa": item.get("empresa"),
                        "deal_id": item.get("deal_id"),
                        "person_id": item.get("person_id"),
                        "source": item.get("source"),
                    }
                    for item in items
                ],
            }
        )
    return buckets


def _deal_signature(record: Dict[str, Any]) -> Tuple[int, int]:
    return (int(record.get("deal_id", 0) or 0), int(record.get("person_id", 0) or 0))


def _record_merge_score(record: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    status_bot = str(record.get("status_bot") or "").strip().lower()
    phones = merge_list([record.get("telefone")] + list(record.get("phones") or []), normalize=LocalCRMCache.normalize_phone)
    emails = merge_list([record.get("email")] + list(record.get("emails") or []), normalize=LocalCRMCache.normalize_email)
    has_terminal_positive = 1 if status_bot == "agendado" else 0
    has_active = 1 if status_bot not in {"perdido"} else 0
    richness = len(phones) + len(emails) + len(merge_list(record.get("decision_makers") or []))
    has_person = 1 if int(record.get("person_id", 0) or 0) else 0
    deal_rank = -int(record.get("deal_id", 0) or 0)
    return (has_terminal_positive, has_active, has_person, richness, deal_rank)


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _deal_created_sort_key(client: PipedriveClient, record: Dict[str, Any]) -> Tuple[datetime, int]:
    deal_id = int(record.get("deal_id", 0) or 0)
    deal = client.get_deal_details(deal_id) if deal_id else {}
    created_at = (
        deal.get("add_time")
        or deal.get("created")
        or deal.get("created_at")
        or record.get("deal_created_at")
        or record.get("created_at")
    )
    parsed = _parse_iso_datetime(created_at) or datetime.max.replace(tzinfo=timezone.utc)
    return (parsed, deal_id if deal_id > 0 else 10**9)


def _build_merge_groups(records: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    deal_records = [dict(item) for item in records if int(item.get("deal_id", 0) or 0)]
    if not deal_records:
        return []
    signatures = {_deal_signature(record) for record in deal_records}
    parent = {signature: signature for signature in signatures}
    signature_to_record = {_deal_signature(record): record for record in deal_records}

    def find(node: Tuple[int, int]) -> Tuple[int, int]:
        root = parent[node]
        while root != parent[root]:
            root = parent[root]
        while node != root:
            next_node = parent[node]
            parent[node] = root
            node = next_node
        return root

    def union(a: Tuple[int, int], b: Tuple[int, int]) -> None:
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    key_to_signatures: Dict[str, Set[Tuple[int, int]]] = defaultdict(set)
    for record in deal_records:
        signature = _deal_signature(record)
        cnpj = LocalCRMCache.normalize_cnpj(record.get("cnpj"))
        if cnpj:
            key_to_signatures[f"cnpj:{cnpj}"].add(signature)
        company = canonical_company_name(record)
        if company:
            key_to_signatures[f"company:{company.strip().upper()}"].add(signature)

    for signatures_for_key in key_to_signatures.values():
        items = list(signatures_for_key)
        if len(items) <= 1:
            continue
        anchor = items[0]
        for item in items[1:]:
            union(anchor, item)

    grouped: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)
    for signature, record in signature_to_record.items():
        grouped[find(signature)].append(record)

    output: List[List[Dict[str, Any]]] = []
    for group in grouped.values():
        deal_ids = {int(item.get("deal_id", 0) or 0) for item in group if int(item.get("deal_id", 0) or 0)}
        if len(deal_ids) <= 1:
            continue
        output.append(sorted(group, key=_record_merge_score, reverse=True))
    return output


def merge_duplicate_group(client: PipedriveClient, group: List[Dict[str, Any]], *, dry_run: bool) -> Dict[str, Any]:
    ranked = sorted([dict(item) for item in group], key=_record_merge_score, reverse=True)
    master = dict(min(ranked, key=lambda item: _deal_created_sort_key(client, item)) or {})
    duplicates = [dict(item) for item in ranked if int(item.get("deal_id", 0) or 0) != int(master.get("deal_id", 0) or 0)]
    ordered = [master] + duplicates
    master_deal_id = int(master.get("deal_id", 0) or 0)
    master_person_id = int(master.get("person_id", 0) or 0)
    phone_values: List[Any] = []
    email_values: List[Any] = []
    decision_maker_values: List[Any] = []
    website_values: List[Any] = []
    for item in ordered:
        phone_values.extend([item.get("telefone")] + list(item.get("phones") or []))
        email_values.extend([item.get("email")] + list(item.get("emails") or []))
        decision_maker_values.extend(list(item.get("decision_makers") or []))
        website_values.extend(list(item.get("websites") or []))
    all_phones = merge_list(phone_values, normalize=LocalCRMCache.normalize_phone)
    all_emails = merge_list(email_values, normalize=LocalCRMCache.normalize_email)
    all_decision_makers = merge_list(decision_maker_values)
    all_websites = merge_list(website_values)
    master_company = canonical_company_name(master)
    merged_from = sorted({int(item.get("deal_id", 0) or 0) for item in duplicates if int(item.get("deal_id", 0) or 0)})
    outcome = {
        "master_deal_id": master_deal_id,
        "master_person_id": master_person_id,
        "merged_deal_ids": merged_from,
        "empresa": master_company or master.get("empresa"),
        "actions": [],
    }
    if not master_deal_id:
        outcome["actions"].append("skip_missing_master_deal")
        return outcome
    master_detail = client.get_deal_details(master_deal_id)
    if int(master_detail.get("pipeline_id", 0) or 0) != PROSPECCAO_PIPELINE_ID:
        outcome["actions"].append("skip_non_prospection_pipeline")
        return outcome
    duplicate_person_ids = sorted({int(item.get("person_id", 0) or 0) for item in duplicates if int(item.get("person_id", 0) or 0)})
    if dry_run:
        if master_person_id and (all_phones or all_emails):
            outcome["actions"].append("master_person_update")
        if duplicate_person_ids:
            outcome["actions"].append("master_participants_sync")
        return outcome

    if master_person_id:
        person_payload: Dict[str, Any] = {}
        if all_phones:
            person_payload["phone"] = all_phones
        if all_emails:
            person_payload["email"] = all_emails
        if person_payload:
            client.update_person(master_person_id, person_payload)
            outcome["actions"].append("master_person_update")

    participants = client.get_deal_participants(master_deal_id, limit=100)
    participant_person_ids = {
        int(((item.get("person_id") or {}).get("value") if isinstance(item.get("person_id"), dict) else item.get("person_id")) or 0)
        for item in participants
        if isinstance(item, dict)
    }
    added = 0
    for person_id in duplicate_person_ids:
        if person_id == master_person_id or person_id in participant_person_ids:
            continue
        client.add_deal_participant(master_deal_id, person_id)
        participant_person_ids.add(person_id)
        added += 1
    if duplicate_person_ids:
        outcome["actions"].append("master_participants_sync")
        outcome["participants_added"] = added
    return outcome


def _same_person_conflict(records: List[Dict[str, Any]]) -> bool:
    return len({int(item.get("person_id", 0) or 0) for item in records if int(item.get("person_id", 0) or 0)}) > 1


def _same_deal_conflict(records: List[Dict[str, Any]]) -> bool:
    return len({int(item.get("deal_id", 0) or 0) for item in records if int(item.get("deal_id", 0) or 0)}) > 1


def sync_record(
    client: PipedriveClient,
    record: Dict[str, Any],
    *,
    dry_run: bool,
    phone_conflicts: Dict[str, List[Dict[str, Any]]],
    email_conflicts: Dict[str, List[Dict[str, Any]]],
    cnpj_conflicts: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    deal_id = int(record.get("deal_id", 0) or 0)
    person_id = int(record.get("person_id", 0) or 0)
    outcome = {
        "empresa": record.get("empresa"),
        "deal_id": deal_id,
        "person_id": person_id,
        "actions": [],
    }
    if not deal_id and not person_id:
        outcome["actions"].append("skip_no_crm_ids")
        return outcome
    deal = client.get_deal_details(deal_id) if deal_id else {}
    if deal_id and int(deal.get("pipeline_id", 0) or 0) != PROSPECCAO_PIPELINE_ID:
        outcome["actions"].append("skip_non_prospection_pipeline")
        return outcome

    normalized_phones = merge_list([record.get("telefone")] + list(record.get("phones") or []), normalize=LocalCRMCache.normalize_phone)
    normalized_emails = merge_list([record.get("email")] + list(record.get("emails") or []), normalize=LocalCRMCache.normalize_email)
    websites = merge_list(record.get("websites") or [])
    decision_makers = merge_list(record.get("decision_makers") or [])
    cnpj = LocalCRMCache.normalize_cnpj(record.get("cnpj"))
    company_name = canonical_company_name(record)
    deal_title = canonical_deal_title(company_name)
    current_deal_title = str(deal.get("title") or "").strip()
    phone_conflicted = any(_same_person_conflict(phone_conflicts.get(phone, [])) for phone in normalized_phones)
    email_conflicted = any(_same_person_conflict(email_conflicts.get(email, [])) for email in normalized_emails)
    cnpj_conflicted = bool(cnpj and _same_deal_conflict(cnpj_conflicts.get(cnpj, [])))
    note_lines = [
        "WEEKLY_CRM_CLEANUP",
        f"CNPJ: {record.get('cnpj')}",
        f"EMPRESA: {company_name or record.get('empresa')}",
        f"DEAL_TITLE_PADRAO: {deal_title}",
        f"TELEFONES: {' | '.join(normalized_phones)}",
        f"EMAILS: {' | '.join(normalized_emails)}",
        f"SITES: {' | '.join(websites[:5])}",
        f"DECISORES: {' | '.join(decision_makers[:10])}",
    ]
    note = "\n".join(line for line in note_lines if str(line).strip())

    if dry_run:
        if person_id and not phone_conflicted and not email_conflicted:
            outcome["actions"].append("person_update")
        if deal_id and person_id:
            outcome["actions"].append("participant_check")
        if phone_conflicted:
            outcome["actions"].append("skip_person_phone_conflict")
        if email_conflicted:
            outcome["actions"].append("skip_person_email_conflict")
        return outcome

    if person_id:
        person_payload: Dict[str, Any] = {}
        if normalized_phones and not phone_conflicted:
            person_payload["phone"] = normalized_phones
        elif normalized_phones:
            outcome["actions"].append("skip_person_phone_conflict")
        if normalized_emails and not email_conflicted:
            person_payload["email"] = normalized_emails
        elif normalized_emails:
            outcome["actions"].append("skip_person_email_conflict")
        if person_payload:
            client.update_person(person_id, person_payload)
            outcome["actions"].append("person_update")
    if deal_id:
        participants = client.get_deal_participants(deal_id, limit=50) if person_id else []
        participant_person_ids = {
            int(((item.get("person_id") or {}).get("value") if isinstance(item.get("person_id"), dict) else item.get("person_id")) or 0)
            for item in participants
            if isinstance(item, dict)
        }
        if person_id and person_id not in participant_person_ids:
            client.add_deal_participant(deal_id, person_id)
            outcome["actions"].append("participant_added")
        else:
            outcome["actions"].append("participant_ok")
    return outcome


def save_report(report: Dict[str, Any]) -> None:
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    safe_write_json(REPORT_FILE, report)


def build_summary(
    *,
    records: List[Dict[str, Any]],
    processed: int,
    duplicates: Dict[str, List[Dict[str, Any]]],
    merge_results: List[Dict[str, Any]],
    sync_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    summary = {
        "total_records": len(records),
        "total_com_crm": sum(1 for item in records if int(item.get("deal_id", 0) or 0) or int(item.get("person_id", 0) or 0)),
        "total_processados": processed,
        "total_grupos_merge": len(merge_results),
        "total_deals_absorvidos": sum(len(item.get("merged_deal_ids") or []) for item in merge_results),
        "total_pessoa_atualizada": 0,
        "total_participante_ok": 0,
        "total_participante_adicionado": 0,
        "total_participante_sync_master": 0,
        "total_conflito_telefone": 0,
        "total_conflito_email": 0,
        "total_dup_phone": len(duplicates.get("phone") or []),
        "total_dup_email": len(duplicates.get("email") or []),
        "total_dup_cnpj": len(duplicates.get("cnpj") or []),
        "total_dup_company": len(duplicates.get("deal_company") or []),
    }
    for item in sync_results:
        actions = set(item.get("actions") or [])
        if "person_update" in actions:
            summary["total_pessoa_atualizada"] += 1
        if "participant_ok" in actions or "participant_check" in actions:
            summary["total_participante_ok"] += 1
        if "participant_added" in actions:
            summary["total_participante_adicionado"] += 1
        if "skip_person_phone_conflict" in actions:
            summary["total_conflito_telefone"] += 1
        if "skip_person_email_conflict" in actions:
            summary["total_conflito_email"] += 1
    for item in merge_results:
        actions = set(item.get("actions") or [])
        if "master_person_update" in actions:
            summary["total_pessoa_atualizada"] += 1
        if "master_participants_sync" in actions:
            summary["total_participante_sync_master"] += 1
            summary["total_participante_adicionado"] += int(item.get("participants_added", 0) or 0)
    return summary


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)

    records = load_records()
    client = PipedriveClient()
    duplicates = duplicate_report(records)
    merge_groups = _build_merge_groups(records)
    phone_conflicts = build_conflict_index(records, "phone")
    email_conflicts = build_conflict_index(records, "email")
    cnpj_conflicts = build_conflict_index(records, "cnpj")
    sync_results: List[Dict[str, Any]] = []
    merge_results: List[Dict[str, Any]] = []
    processed = 0
    processed_signatures: Set[Tuple[int, int]] = set()
    for group in merge_groups:
        merge_result = merge_duplicate_group(client, group, dry_run=args.dry_run)
        merge_results.append(merge_result)
        for item in group[1:]:
            processed_signatures.add(_deal_signature(item))
    for record in records:
        if args.limit and processed >= int(args.limit):
            break
        if not record.get("deal_id") and not record.get("person_id"):
            continue
        if _deal_signature(record) in processed_signatures:
            continue
        signature = hashlib.sha1(
            f"{record.get('deal_id')}|{record.get('person_id')}|{record.get('cnpj')}|{record.get('empresa')}".encode("utf-8")
        ).hexdigest()[:12]
        result = sync_record(
            client,
            record,
            dry_run=args.dry_run,
            phone_conflicts=phone_conflicts,
            email_conflicts=email_conflicts,
            cnpj_conflicts=cnpj_conflicts,
        )
        result["signature"] = signature
        sync_results.append(result)
        processed += 1

    summary = build_summary(
        records=records,
        processed=processed,
        duplicates=duplicates,
        merge_results=merge_results,
        sync_results=sync_results,
    )
    report = {
        "generated_at": utcnow_iso(),
        "dry_run": bool(args.dry_run),
        "processed": processed,
        "merged_groups": len(merge_results),
        "summary": summary,
        "duplicates": duplicates,
        "merge_results": merge_results,
        "sync_results": sync_results,
    }
    save_report(report)
    print(
        f"[WEEKLY_CRM_CLEANUP] dry_run={args.dry_run} processed={processed} "
        f"merged_groups={len(merge_results)} "
        f"person_update={summary['total_pessoa_atualizada']} "
        f"participant_added={summary['total_participante_adicionado']} "
        f"phone_conflict={summary['total_conflito_telefone']} "
        f"dup_phone={len(duplicates['phone'])} dup_email={len(duplicates['email'])} "
        f"dup_cnpj={len(duplicates['cnpj'])} dup_deal_company={len(duplicates['deal_company'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
