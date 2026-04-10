from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_phone, parse_xlsx


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_writeback_ready.xlsx")
STATE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\runtime\writeback_enriched_to_pipedrive_state.json")
PIPELINE_ID = 2
ARCHIVE_STAGE_ID = 50
CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
BATCH_LIMIT = 80
BATCH_DELAY_SEC = 8
BLOCKED_PHONES = {"18043309723", "11999999999", "00999999977", "1155555555"}


def log(message: str) -> None:
    print(str(message), flush=True)


def load_state() -> Dict[str, Any]:
    if not STATE_JSON.exists():
        return {"done": []}
    try:
        data = json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"done": []}
    return data if isinstance(data, dict) else {"done": []}


def save_state(state: Dict[str, Any]) -> None:
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def row_key(row: Dict[str, Any]) -> str:
    return f"{str(row.get('deal_id') or '').strip()}:{str(row.get('org_id') or '').strip()}:{str(row.get('acao') or '').strip().upper()}"


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        return int(value.get("value") or value.get("id") or 0)
    try:
        return int(value or 0)
    except Exception:
        return 0


def clean_phone(value: Any) -> str:
    phone = normalize_phone(value)
    if phone in BLOCKED_PHONES:
        return ""
    return phone if 10 <= len(phone) <= 11 else ""


def clean_email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if "@" in text and "." in text else ""


def get_first_phone(person: Dict[str, Any]) -> str:
    raw = person.get("phone")
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                phone = clean_phone(item.get("value"))
            else:
                phone = clean_phone(item)
            if phone:
                return phone
    return clean_phone(raw)


def get_first_email(person: Dict[str, Any]) -> str:
    raw = person.get("email")
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                email = clean_email(item.get("value"))
            else:
                email = clean_email(item)
            if email:
                return email
    return clean_email(raw)


def cnpj_is_empty(value: Any) -> bool:
    return not normalize_cnpj(value)


def name_is_dirty(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return not text or text.startswith("lead mand digital") or text in {"negocio", "negócio"}


def build_person_payload(person: Dict[str, Any], phone: str, email: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    current_phone = get_first_phone(person)
    current_email = get_first_email(person)
    if phone and (not current_phone or len(current_phone) < 10):
        payload["phone"] = [{"value": phone, "primary": True, "label": "work"}]
    elif phone:
        log("[SKIP_DADO_MELHOR_EXISTENTE] pessoa=telefone")
    if email and not current_email:
        payload["email"] = [{"value": email, "primary": True, "label": "work"}]
    elif email:
        log("[SKIP_DADO_MELHOR_EXISTENTE] pessoa=email")
    return payload


def build_org_payload(org: Dict[str, Any], nome_final: str, cnpj: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    current_name = str(org.get("name") or "").strip()
    if nome_final and current_name.strip().lower() != nome_final.strip().lower() and name_is_dirty(current_name):
        payload["name"] = nome_final
    elif nome_final and current_name.strip().lower() != nome_final.strip().lower():
        log("[SKIP_DADO_MELHOR_EXISTENTE] org=nome")
    if cnpj and cnpj_is_empty(org.get(CNPJ_FIELD_KEY)):
        payload[CNPJ_FIELD_KEY] = cnpj
    elif cnpj:
        log("[SKIP_DADO_MELHOR_EXISTENTE] org=cnpj")
    return payload


def main() -> int:
    rows = parse_xlsx(INPUT_XLSX)
    updates = [row for row in rows if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}]
    archives = [row for row in rows if str(row.get("acao") or "").strip().upper() == "ARCHIVE"]
    rows_to_process = updates + archives
    state = load_state()
    done = set(state.get("done") or [])
    client = PipedriveClient()
    if not client.test_connection():
        log(f"[PIPEDRIVE_ERRO] status={client.last_http_status} body={client.last_http_body[:180]}")
        return 2
    processed = 0
    update_ok = 0
    archive_ok = 0
    skip = 0
    errors = 0
    log(f"[WRITEBACK_INICIO] updates={len(updates)} archives={len(archives)} ja_feitos={len(done)}")
    for row in rows_to_process:
        key = row_key(row)
        if key in done:
            continue
        deal_id = int(str(row.get("deal_id") or "0") or 0)
        org_id = int(str(row.get("org_id") or "0") or 0)
        action = str(row.get("acao") or "").strip().upper()
        if not deal_id or not org_id:
            done.add(key)
            skip += 1
            continue
        log(f"[PROCESSANDO_WRITEBACK] deal={deal_id} org={org_id} acao={action}")
        deal = client.get_deal_details(deal_id)
        if not deal:
            log(f"[WRITEBACK_ERRO] deal={deal_id} status={client.last_http_status} body={client.last_http_body[:140]}")
            errors += 1
            if client.last_http_status == 429:
                break
            continue
        if int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
            log(f"[SKIP_FORA_PIPELINE] deal={deal_id} pipeline={deal.get('pipeline_id')}")
            done.add(key)
            save_state({"done": sorted(done)})
            skip += 1
            continue
        if action == "ARCHIVE":
            if int(deal.get("stage_id") or 0) == ARCHIVE_STAGE_ID:
                log(f"[ARCHIVE_OK] deal={deal_id} ja_estava=True")
                archive_ok += 1
            elif client.update_stage(deal_id=deal_id, stage_id=ARCHIVE_STAGE_ID):
                log(f"[ARCHIVE_OK] deal={deal_id}")
                archive_ok += 1
            else:
                log(f"[ARCHIVE_ERRO] deal={deal_id} status={client.last_http_status} body={client.last_http_body[:140]}")
                errors += 1
                if client.last_http_status == 429:
                    break
                continue
        else:
            current_org_id = extract_id(deal.get("org_id"))
            if current_org_id and current_org_id != org_id:
                log(f"[SKIP_ORG_MISMATCH] deal={deal_id} planilha_org={org_id} crm_org={current_org_id}")
                done.add(key)
                save_state({"done": sorted(done)})
                skip += 1
                continue
            org = client.get_organization(org_id)
            if not org:
                log(f"[ORG_ERRO] org={org_id} status={client.last_http_status} body={client.last_http_body[:140]}")
                errors += 1
                if client.last_http_status == 429:
                    break
                continue
            org_payload = build_org_payload(org, str(row.get("nome_final") or "").strip(), normalize_cnpj(row.get("cnpj")))
            person_payload: Dict[str, Any] = {}
            person_id = extract_id(deal.get("person_id"))
            if person_id:
                person = client.get_person_details(person_id)
                if person:
                    person_payload = build_person_payload(person, clean_phone(row.get("telefone")), clean_email(row.get("email")))
            if org_payload and not client.update_organization(org_id, org_payload):
                log(f"[ORG_UPDATE_ERRO] org={org_id} status={client.last_http_status} body={client.last_http_body[:140]}")
                errors += 1
                if client.last_http_status == 429:
                    break
                continue
            if person_payload and person_id and not client.update_person(person_id, person_payload):
                log(f"[PERSON_UPDATE_ERRO] person={person_id} status={client.last_http_status} body={client.last_http_body[:140]}")
                errors += 1
                if client.last_http_status == 429:
                    break
                continue
            log(f"[UPDATE_OK] deal={deal_id} org_fields={list(org_payload.keys())} person_fields={list(person_payload.keys())}")
            update_ok += 1
        done.add(key)
        processed += 1
        if processed % 10 == 0:
            save_state({"done": sorted(done)})
            log(f"[PROGRESSO_WRITEBACK] processados_nesta_execucao={processed} total_done={len(done)} update_ok={update_ok} archive_ok={archive_ok} erros={errors}")
        if processed >= BATCH_LIMIT:
            save_state({"done": sorted(done)})
            log(f"[PAUSA_LOTE] limite={BATCH_LIMIT} delay={BATCH_DELAY_SEC}s total_done={len(done)}")
            time.sleep(BATCH_DELAY_SEC)
            processed = 0
    save_state({"done": sorted(done)})
    log(f"[WRITEBACK_FIM] total_done={len(done)} update_ok={update_ok} archive_ok={archive_ok} skip={skip} erros={errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
