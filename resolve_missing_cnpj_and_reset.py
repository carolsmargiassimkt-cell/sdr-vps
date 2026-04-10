from __future__ import annotations

import argparse
import json
import re
import time
from html import unescape
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List

import requests

from config.config_loader import get_config_value
from enrich_missing_cnpj_from_xlsx import normalize_cnpj, normalize_name, parse_xlsx
from logic.whatsapp_pitch_engine import WhatsAppPitchEngine
from reset_pipeline2_participants import (
    INPUT_XLSX,
    PIPELINE_ID,
    clean_email,
    clean_phone,
    contact_candidates,
    create_or_update_person,
    extract_id,
    fetch_casa_cnpj,
    log,
    person_org_id,
    should_remove_person,
)
from crm.pipedrive_client import PipedriveClient


CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\missing_cnpj_name_resolution_cache.json")
MAX_PARTICIPANTS_PER_DEAL = 3
MIN_NAME_CONFIDENCE = 0.78
DDG_TIMEOUT_SEC = 12


def load_cache() -> Dict[str, Any]:
    if not CACHE_JSON.exists():
        return {"casa_name": {}, "openrouter": {}, "casa_cnpj": {}}
    try:
        payload = json.loads(CACHE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"casa_name": {}, "openrouter": {}, "casa_cnpj": {}}
    if not isinstance(payload, dict):
        return {"casa_name": {}, "openrouter": {}, "casa_cnpj": {}}
    payload.setdefault("casa_name", {})
    payload.setdefault("openrouter", {})
    payload.setdefault("casa_cnpj", {})
    return payload


def save_cache(cache: Dict[str, Any]) -> None:
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def openrouter_key() -> str:
    configured = str(get_config_value("openrouter_api_key", "") or "").strip()
    if configured:
        return configured
    return str(getattr(WhatsAppPitchEngine, "OPENROUTER_TOKEN", "") or "").strip()


def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


def variants_from_name(name: str, cache: Dict[str, Any]) -> List[str]:
    base = normalize_name(name)
    if not base:
        return []
    variants = [base]
    fixes = {
        "alintos": "alimentos",
        "ntos": "santos",
        "alida": "almeida",
        "ndes": "fernandes",
        "movintacao": "movimentacao",
        "construção": "construcao",
    }
    fixed = base
    for old, new in fixes.items():
        fixed = re.sub(rf"\b{re.escape(old)}\b", new, fixed)
    if fixed != base:
        variants.append(fixed)
    ai_cache = cache.setdefault("openrouter", {})
    if base in ai_cache:
        for item in ai_cache.get(base) or []:
            item = normalize_name(item)
            if item and item not in variants:
                variants.append(item)
        return variants[:6]
    key = openrouter_key()
    if not key:
        ai_cache[base] = []
        return variants[:6]
    prompt = (
        "Corrija o nome de empresa brasileira abaixo para buscar CNPJ. "
        "Responda somente JSON com uma lista chamada variants, sem explicação. "
        "Inclua razão social provável, nome fantasia provável e correções ortográficas. "
        f"Nome: {base!r}"
    )
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": "openrouter/auto",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 180,
            },
            timeout=20,
        )
        response.raise_for_status()
        content = str(response.json()["choices"][0]["message"]["content"] or "")
        match = re.search(r"\{.*\}", content, re.S)
        parsed = json.loads(match.group(0) if match else content)
        ai_variants = parsed.get("variants") if isinstance(parsed, dict) else []
    except Exception as exc:
        ai_cache[f"erro::{base}"] = str(exc)[:300]
        ai_variants = []
    cleaned = []
    for item in ai_variants or []:
        item = normalize_name(item)
        if item and item not in variants and item not in cleaned:
            cleaned.append(item)
    ai_cache[base] = cleaned[:5]
    variants.extend(cleaned[:5])
    return variants[:6]


def search_casa_name(name: str, cache: Dict[str, Any], tipo_busca: str = "exata") -> Dict[str, Any]:
    normalized = normalize_name(name)
    name_cache = cache.setdefault("casa_name", {})
    cache_key = f"{tipo_busca}::{normalized}"
    legacy = name_cache.get(normalized) if tipo_busca == "exata" else None
    if cache_key in name_cache:
        return dict(name_cache[cache_key] or {})
    if isinstance(legacy, dict):
        return dict(legacy)
    api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
    body = {
        "busca_textual": [
            {
                "texto": [normalized],
                "tipo_busca": tipo_busca,
                "razao_social": True,
                "nome_fantasia": True,
                "nome_socio": False,
            }
        ],
        "situacao_cadastral": ["ATIVA"],
        "limite": 5,
        "pagina": 1,
    }
    try:
        response = requests.post(
            "https://api.casadosdados.com.br/v5/cnpj/pesquisa",
            params={"tipo_resultado": "completo"},
            headers={"api-key": api_key, "accept": "application/json", "Content-Type": "application/json", "user-agent": "bot-sdr-ai/1.0"},
            json=body,
            timeout=15,
        )
        payload: Dict[str, Any] = {"status_code": response.status_code, "raw": response.json() if response.status_code == 200 else response.text[:300]}
    except Exception as exc:
        payload = {"status_code": 0, "raw": str(exc)[:300]}
    name_cache[cache_key] = payload
    save_cache(cache)
    return payload


def search_duckduckgo_cnpj(name: str, cache: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_name(name)
    ddg_cache = cache.setdefault("duckduckgo", {})
    if normalized in ddg_cache:
        return dict(ddg_cache[normalized] or {})
    query = f"{normalized} CNPJ"
    try:
        response = requests.get(
            "https://duckduckgo.com/html/",
            params={"q": query},
            headers={"user-agent": "Mozilla/5.0 bot-sdr-ai/1.0"},
            timeout=DDG_TIMEOUT_SEC,
        )
        html = unescape(response.text or "")
        matches = re.findall(r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}", html)
        cnpjs = []
        for match in matches:
            cnpj = normalize_cnpj(match)
            if cnpj and cnpj not in cnpjs:
                cnpjs.append(cnpj)
        payload = {"status_code": response.status_code, "cnpjs": cnpjs[:5], "query": query}
    except Exception as exc:
        payload = {"status_code": 0, "cnpjs": [], "query": query, "error": str(exc)[:300]}
    ddg_cache[normalized] = payload
    save_cache(cache)
    return payload


def choose_cnpj(original_name: str, variants: List[str], cache: Dict[str, Any]) -> Dict[str, Any]:
    best: Dict[str, Any] = {}
    best_score = 0.0
    for variant in variants:
        for tipo_busca in ("exata", "radical"):
            result = search_casa_name(variant, cache, tipo_busca=tipo_busca)
            if int(result.get("status_code") or 0) != 200:
                log(f"[CASA_NOME_ERRO] nome={variant} tipo={tipo_busca} status={result.get('status_code')}")
                continue
            cnpjs = ((result.get("raw") or {}).get("cnpjs") or []) if isinstance(result.get("raw"), dict) else []
            log(f"[CASA_NOME_OK] nome={variant} tipo={tipo_busca} total={len(cnpjs)}")
            for item in cnpjs[:5]:
                if not isinstance(item, dict):
                    continue
                cnpj = normalize_cnpj(item.get("cnpj"))
                company = str(item.get("razao_social") or item.get("nome_fantasia") or "")
                score = max(similar(original_name, company), similar(variant, company))
                if str(item.get("matriz_filial") or "").upper() == "MATRIZ":
                    score += 0.05
                if cnpj and score > best_score:
                    best_score = score
                    best = {"cnpj": cnpj, "company": company, "score": round(score, 3), "variant": variant, "tipo_busca": tipo_busca}
    if best and best_score >= MIN_NAME_CONFIDENCE:
        return best
    for variant in variants:
        result = search_duckduckgo_cnpj(variant, cache)
        cnpjs = list(result.get("cnpjs") or [])
        log(f"[DDG_CNPJ] nome={variant} total={len(cnpjs)} status={result.get('status_code')}")
        for cnpj in cnpjs[:3]:
            casa = fetch_casa_cnpj(cnpj, str(get_config_value("casa_dos_dados_api_key", "") or "").strip())
            if int(casa.get("status_code") or 0) != 200:
                continue
            company = str(casa.get("company_name") or "")
            score = max(similar(original_name, company), similar(variant, company))
            if score >= MIN_NAME_CONFIDENCE:
                cache.setdefault("casa_cnpj", {})[cnpj] = casa
                save_cache(cache)
                return {"cnpj": cnpj, "company": company, "score": round(score, 3), "variant": variant, "source": "duckduckgo"}
    return {}


def apply_deal_contacts(client: PipedriveClient, row: Dict[str, Any], casa: Dict[str, Any], *, apply: bool) -> Dict[str, int]:
    counters = {"deal_corrigido": 0, "primary_ok": 0, "remover": 0, "removido": 0, "sem_person_ok": 0}
    deal_id = int(str(row.get("deal_id") or "0") or 0)
    org_id = int(str(row.get("org_id") or "0") or 0)
    fallback_name = str(row.get("nome_final") or "").strip()
    candidates = contact_candidates(casa, fallback_name)
    allowed_phones = {clean_phone(item.get("phone")) for item in candidates if clean_phone(item.get("phone"))}
    allowed_emails = {clean_email(item.get("email")) for item in candidates if clean_email(item.get("email"))}
    log(f"[DEAL_RESET_SEM_CNPJ] deal={deal_id} org={org_id} candidates={len(candidates)}")
    deal = client.get_deal_details(deal_id)
    if not deal or int(deal.get("pipeline_id") or 0) != PIPELINE_ID:
        return counters
    live_org_id = extract_id(deal.get("org_id"))
    if live_org_id != org_id:
        log(f"[ORG_MISMATCH] deal={deal_id} live_org={live_org_id} expected_org={org_id}")
        if apply and client.update_deal(deal_id, {"org_id": org_id}):
            log(f"[ORG_VINCULO_OK] deal={deal_id} org={org_id}")
        elif apply:
            return counters
    target_person_ids: List[int] = []
    for candidate in candidates[:MAX_PARTICIPANTS_PER_DEAL]:
        person_id = create_or_update_person(client, org_id, candidate, apply=apply)
        if person_id > 0 and person_id not in target_person_ids:
            target_person_ids.append(person_id)
        elif person_id == -1:
            target_person_ids.append(-1)
    if not target_person_ids:
        counters["sem_person_ok"] += 1
    elif apply:
        first_id = target_person_ids[0]
        if first_id > 0 and extract_id(deal.get("person_id")) != first_id and client.update_deal(deal_id, {"person_id": first_id}):
            counters["primary_ok"] += 1
            log(f"[VINCULO_OK] deal={deal_id} primary_person={first_id}")
    participants = client.get_deal_participants(deal_id, limit=100)
    for participant in participants:
        part_id = int(participant.get("id") or 0)
        p_id = extract_id(participant.get("person_id") or participant.get("person"))
        if p_id in target_person_ids:
            continue
        person = client.get_person_details(p_id)
        if should_remove_person(person, org_id, allowed_phones, allowed_emails):
            counters["remover"] += 1
            if apply and part_id and client.remove_deal_participant(deal_id, part_id):
                counters["removido"] += 1
                log(f"[REMOVIDO_CONTAMINADO] deal={deal_id} participant={part_id} person={p_id}")
            elif not apply:
                log(f"[REMOVIDO_CONTAMINADO] deal={deal_id} participant={part_id} person={p_id} dry=1")
    counters["deal_corrigido"] += 1
    return counters


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    rows = [
        row for row in parse_xlsx(INPUT_XLSX)
        if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}
        and not normalize_cnpj(row.get("cnpj"))
    ][: max(1, args.limit)]
    cache = load_cache()
    client = PipedriveClient()
    totals: Dict[str, int] = {}
    for idx, row in enumerate(rows, start=1):
        name = str(row.get("nome_final") or "").strip()
        variants = variants_from_name(name, cache)
        save_cache(cache)
        chosen = choose_cnpj(name, variants, cache)
        if not chosen:
            log(f"[CNPJ_NAO_ENCONTRADO] {idx}/{len(rows)} deal={row.get('deal_id')} nome={name}")
            continue
        cnpj = chosen["cnpj"]
        log(f"[CNPJ_ENCONTRADO_NOME] {idx}/{len(rows)} deal={row.get('deal_id')} cnpj={cnpj} score={chosen['score']} nome={chosen['company']}")
        casa_cache = cache.setdefault("casa_cnpj", {})
        if cnpj not in casa_cache:
            casa_cache[cnpj] = fetch_casa_cnpj(cnpj, str(get_config_value("casa_dos_dados_api_key", "") or "").strip())
            save_cache(cache)
        casa = casa_cache.get(cnpj) or {}
        if int(casa.get("status_code") or 0) != 200:
            log(f"[CASA_CNPJ_ERRO] deal={row.get('deal_id')} cnpj={cnpj} status={casa.get('status_code')}")
            continue
        applied = apply_deal_contacts(client, row, casa, apply=args.apply)
        for key, value in applied.items():
            totals[key] = totals.get(key, 0) + int(value or 0)
        time.sleep(1.5)
    log(f"[RESOLVE_MISSING_CNPJ_RESUMO] apply={int(args.apply)} {totals}")


if __name__ == "__main__":
    main()
