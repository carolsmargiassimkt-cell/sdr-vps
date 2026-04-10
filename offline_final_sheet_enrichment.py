from __future__ import annotations

import argparse
import json
import re
import time
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import parse_qs, unquote, urlparse

import requests

from config.config_loader import get_config_value
from enrich_missing_cnpj_from_xlsx import (
    BLOCKED_GENERIC_PHONES,
    best_phone,
    bucket,
    fetch_casa,
    find_cnpj_from_brave,
    find_cnpj_from_query,
    load_cache,
    normalize_cnpj,
    normalize_name,
    normalize_phone,
    parse_xlsx,
    save_cache,
    search_casa_by_name,
    suggest_names_openrouter,
    validate_casa_auth,
    xml_escape,
    excel_column_name,
)


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\LISTAS-MAND\deals_enriquecido_writeback_ready.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_enriquecido_offline_master.xlsx")
OUTPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_enriquecido_offline_master.csv")
SUMMARY_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\offline_final_sheet_enrichment_summary.json")
CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\offline_final_sheet_enrichment_cache.json")
LOCAL_CRM_BASE = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\local_crm_base.json")

DUCK_TIMEOUT_SEC = 15
OPENROUTER_TIMEOUT_SEC = 20
LOOKUP_DELAY_SEC = 1.0
OPENROUTER_MODEL = "openrouter/auto"
TARGET_CNPJ_DOMAINS = ("cnpj.biz", "consultacnpj.com")
ROLE_PATTERNS = (
    "CEO",
    "Diretor de Marketing",
    "Diretor Comercial",
    "Diretor de Vendas",
    "Head de Marketing",
    "Head de Vendas",
    "Gerente de Marketing",
    "Gerente Comercial",
    "Gerente de Vendas",
)


def log(message: str) -> None:
    print(str(message), flush=True)


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def dynamic_write_xlsx(rows: List[Dict[str, Any]], path: Path) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    data = [headers]
    for row in rows:
        data.append([row.get(header, "") for header in headers])

    sheet_rows: List[str] = []
    for row_idx, row in enumerate(data, start=1):
        cells: List[str] = []
        for col_idx, value in enumerate(row, start=1):
            ref = f"{excel_column_name(col_idx)}{row_idx}"
            as_text = str(value or "")
            if row_idx > 1 and as_text.isdigit() and len(as_text) < 12 and col_idx <= 3:
                cells.append(f'<c r="{ref}"><v>{int(as_text)}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{xml_escape(as_text)}</t></is></c>')
        sheet_rows.append(f'<row r="{row_idx}">{"".join(cells)}</row>')

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>"""
    rels_root = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>"""
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="offline_master" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""
    worksheet = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{''.join(sheet_rows)}</sheetData></worksheet>"""
    core = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator><cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>
</cp:coreProperties>"""
    app = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>Codex</Application></Properties>"""

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels_root)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/worksheets/sheet1.xml", worksheet)
        zf.writestr("docProps/core.xml", core)
        zf.writestr("docProps/app.xml", app)


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    import csv

    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def canonical_title(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text


def openrouter_standardize_name(name: str, cache: Dict[str, Any]) -> Dict[str, str]:
    base = normalize_name(name)
    cached = cache.get(base)
    if isinstance(cached, dict):
        return {k: str(v or "") for k, v in cached.items()}
    api_key = str(get_config_value("openrouter_api_key", "") or "").strip()
    if not api_key or not base:
        payload = {"nome_padrao": canonical_title(name), "razao_social_sugerida": ""}
        cache[base] = payload
        return payload
    prompt = (
        "Retorne JSON puro com chaves nome_padrao e razao_social_sugerida. "
        "Padronize o nome empresarial em portugues do Brasil, corrigindo acentuacao, "
        "pontuacao e grafias obvias. Nao invente marca se nao houver confianca. "
        f"Nome recebido: {name!r}."
    )
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 160,
            },
            timeout=OPENROUTER_TIMEOUT_SEC,
        )
        response.raise_for_status()
        content = str(response.json()["choices"][0]["message"]["content"] or "")
        match = re.search(r"\{.*\}", content, re.S)
        parsed = json.loads(match.group(0) if match else content)
        payload = {
            "nome_padrao": str(parsed.get("nome_padrao") or canonical_title(name)).strip(),
            "razao_social_sugerida": str(parsed.get("razao_social_sugerida") or "").strip(),
        }
    except Exception as exc:
        cache[f"erro::{base}"] = str(exc)[:300]
        payload = {"nome_padrao": canonical_title(name), "razao_social_sugerida": ""}
    cache[base] = payload
    return payload


def duckduckgo_search_html(query: str) -> str:
    response = requests.get(
        "https://html.duckduckgo.com/html/",
        params={"q": query},
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
        timeout=DUCK_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.text


def extract_search_urls(html: str, host_filter: str = "") -> List[str]:
    urls: List[str] = []
    seen = set()
    for raw in re.findall(r'href="([^"]+)"', str(html or "")):
        candidate = raw
        if raw.startswith("/l/?"):
            parsed = urlparse(raw)
            candidate = parse_qs(parsed.query).get("uddg", [""])[0]
        candidate = unquote(candidate)
        if not candidate.startswith("http"):
            continue
        host = urlparse(candidate).netloc.lower()
        if host_filter and host_filter not in host:
            continue
        if candidate not in seen:
            seen.add(candidate)
            urls.append(candidate)
    return urls


def discover_cnpj_multi(name: str, cache: Dict[str, Any]) -> str:
    lookup_cache = bucket(cache, "cnpj_lookup")
    ai_cache = bucket(cache, "openrouter_lookup")
    normalized = normalize_name(name)
    if not normalized:
        return ""
    queries = [
        f"{normalized} CNPJ site:cnpj.biz",
        f"{normalized} CNPJ site:consultacnpj.com",
        f"{normalized} razao social CNPJ",
    ]
    for query in queries:
        cnpj = find_cnpj_from_query(query, lookup_cache)
        save_json(CACHE_JSON, cache)
        if cnpj:
            return cnpj
        time.sleep(LOOKUP_DELAY_SEC)
    for query in (f"{normalized} CNPJ", f"{normalized} empresa CNPJ"):
        cnpj = find_cnpj_from_brave(query, lookup_cache)
        save_json(CACHE_JSON, cache)
        if cnpj:
            return cnpj
        time.sleep(LOOKUP_DELAY_SEC)
    for suggested in suggest_names_openrouter(normalized, ai_cache):
        for domain in TARGET_CNPJ_DOMAINS:
            cnpj = find_cnpj_from_query(f"{suggested} CNPJ site:{domain}", lookup_cache)
            save_json(CACHE_JSON, cache)
            if cnpj:
                return cnpj
            time.sleep(LOOKUP_DELAY_SEC)
    return ""


def extract_casa_address(payload: Dict[str, Any]) -> Dict[str, str]:
    raw = payload.get("raw") if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    endereco = raw.get("endereco") if isinstance(raw.get("endereco"), dict) else {}
    route = " ".join(
        part for part in [str(endereco.get("tipo_logradouro") or "").strip(), str(endereco.get("logradouro") or "").strip()] if part
    ).strip()
    number = str(endereco.get("numero") or "").strip()
    district = str(endereco.get("bairro") or "").strip()
    city = str(endereco.get("municipio") or "").strip()
    state = str(endereco.get("uf") or "").strip()
    cep = re.sub(r"\D+", "", str(endereco.get("cep") or ""))
    complement = str(endereco.get("complemento") or "").strip()
    full = ", ".join(part for part in [route, number, complement, district, city, state, cep] if part)
    return {
        "casa_endereco": full,
        "casa_logradouro": route,
        "casa_numero": number,
        "casa_bairro": district,
        "casa_cidade": city,
        "casa_uf": state,
        "casa_cep": cep,
        "casa_endereco_mossoro": "1" if "mossoro" in normalize_name(full) else "0",
    }


def local_records() -> List[Dict[str, Any]]:
    payload = read_json(LOCAL_CRM_BASE)
    records = payload.get("records")
    return [dict(item) for item in records] if isinstance(records, list) else []


def build_phone_registry(records: List[Dict[str, Any]], current_rows: List[Dict[str, Any]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for record in records:
        for raw in [record.get("telefone")] + list(record.get("phones") or []):
            phone = normalize_phone(raw)
            if phone and phone not in BLOCKED_GENERIC_PHONES:
                counter[phone] += 1
    for row in current_rows:
        for raw in [row.get("telefone"), row.get("casa_telefone_principal"), row.get("google_phone")]:
            phone = normalize_phone(raw)
            if phone and phone not in BLOCKED_GENERIC_PHONES:
                counter[phone] += 1
    return counter


def public_role_links(company_name: str, cache: Dict[str, Any]) -> Dict[str, str]:
    normalized = canonical_title(company_name)
    search_cache = bucket(cache, "role_search")
    key = normalize_name(normalized)
    cached = search_cache.get(key)
    if isinstance(cached, dict):
        return {k: str(v or "") for k, v in cached.items()}
    output = {
        "public_role_links": "",
        "public_role_notes": "",
        "linkedin_public_only": "1",
    }
    queries = []
    for role in ROLE_PATTERNS:
        queries.append(f'"{normalized}" "{role}" site:linkedin.com/in')
    links: List[str] = []
    notes: List[str] = []
    for query in queries[:4]:
        try:
            html = duckduckgo_search_html(query)
            found = extract_search_urls(html, host_filter="linkedin.com")
            for url in found[:2]:
                if url not in links:
                    links.append(url)
            if found:
                notes.append(query)
            time.sleep(LOOKUP_DELAY_SEC)
        except Exception as exc:
            search_cache[f"erro::{key}::{query}"] = str(exc)[:200]
    output["public_role_links"] = " | ".join(links[:6])
    output["public_role_notes"] = " | ".join(notes[:4])
    search_cache[key] = output
    return output


def google_phone_public(company_name: str, cache: Dict[str, Any]) -> str:
    phone_cache = bucket(cache, "google_phone")
    key = normalize_name(company_name)
    cached = normalize_phone(phone_cache.get(key))
    if cached and cached not in BLOCKED_GENERIC_PHONES:
        return cached
    phone = ""
    try:
        html = duckduckgo_search_html(f'"{company_name}" telefone')
        matches = re.findall(r"(\(?\d{2}\)?\s?\d{4,5}-?\d{4})", html)
        phone = best_phone([normalize_phone(match) for match in matches])
    except Exception as exc:
        phone_cache[f"erro::{key}"] = str(exc)[:200]
    phone_cache[key] = phone
    return phone


def enrich_rows(rows: List[Dict[str, Any]], apply_network: bool) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache = read_json(CACHE_JSON)
    cache.setdefault("meta", {})
    local_base_records = local_records()
    casa_cache = bucket(cache, "casa")
    casa_search_cache = bucket(cache, "casa_search")
    ai_name_cache = bucket(cache, "name_standardization")
    summary: Counter[str] = Counter()
    output: List[Dict[str, Any]] = []

    casa_auth_ok = validate_casa_auth(casa_cache) if apply_network else False
    save_json(CACHE_JSON, cache)

    for index, row in enumerate(rows, start=1):
        current = dict(row)
        company_raw = str(current.get("nome_final") or "").strip()
        deal_id = str(current.get("deal_id") or "").strip()
        standardized = openrouter_standardize_name(company_raw, ai_name_cache) if apply_network else {
            "nome_padrao": canonical_title(company_raw),
            "razao_social_sugerida": "",
        }
        target_name = standardized.get("nome_padrao") or canonical_title(company_raw)
        rationale_name = standardized.get("razao_social_sugerida") or ""
        current["nome_original"] = company_raw
        current["nome_padronizado"] = target_name
        current["razao_social_sugerida"] = rationale_name
        current["nome_limpo_sem_tokens"] = canonical_title(company_raw)

        cnpj = normalize_cnpj(current.get("cnpj"))
        if not cnpj and apply_network:
            cnpj = search_casa_by_name(rationale_name or target_name, casa_search_cache)
            if not cnpj:
                cnpj = discover_cnpj_multi(rationale_name or target_name, cache)
            if cnpj:
                summary["cnpj_encontrado_offline"] += 1
        current["cnpj_enriquecido"] = cnpj
        current["cnpj_faltante"] = "0" if cnpj else "1"

        casa_payload: Dict[str, Any] = {}
        if cnpj and apply_network and casa_auth_ok:
            casa_payload = fetch_casa(cnpj, casa_cache)
            save_json(CACHE_JSON, cache)
            if int(casa_payload.get("status_code") or 0) == 200:
                summary["casa_ok"] += 1
        raw_casa = casa_payload.get("raw") if isinstance(casa_payload.get("raw"), dict) else {}
        casa_phones = [normalize_phone(item) for item in list(casa_payload.get("telefones") or [])]
        casa_phones = [item for item in casa_phones if item and item not in BLOCKED_GENERIC_PHONES]
        current["casa_status_code"] = str(casa_payload.get("status_code") or "")
        current["casa_razao_social"] = str(casa_payload.get("razao_social") or raw_casa.get("razao_social") or "")
        current["casa_nome_fantasia"] = str(casa_payload.get("nome_fantasia") or raw_casa.get("nome_fantasia") or "")
        current["casa_telefones"] = " | ".join(casa_phones)
        current["casa_emails"] = " | ".join(list(casa_payload.get("emails") or []))
        current["casa_socios"] = " | ".join(list(casa_payload.get("socios") or []))
        current.update(extract_casa_address(casa_payload))

        google_phone = google_phone_public(rationale_name or target_name, cache) if apply_network else ""
        current["google_phone"] = google_phone
        current["google_phone_faltante"] = "0" if google_phone else "1"

        role_data = public_role_links(rationale_name or target_name, cache) if apply_network else {
            "public_role_links": "",
            "public_role_notes": "",
            "linkedin_public_only": "1",
        }
        current.update(role_data)

        current["telefone_principal_sugerido"] = best_phone(
            [normalize_phone(current.get("telefone")), *casa_phones, normalize_phone(google_phone)]
        )

        output.append(current)
        if index % 25 == 0:
            log(f"[OFFLINE_ENRICH_PROGRESS] {index}/{len(rows)}")

    phone_registry = build_phone_registry(local_base_records, output)
    address_counter = Counter(normalize_name(row.get("casa_endereco")) for row in output if normalize_name(row.get("casa_endereco")))
    mossoro_count = 0
    duplicate_phone_rows = 0
    duplicated_address_rows = 0
    for row in output:
        phone = normalize_phone(row.get("telefone_principal_sugerido"))
        row["telefone_duplicado_global"] = "1" if phone and phone_registry.get(phone, 0) > 1 else "0"
        row["telefone_duplicado_count"] = str(phone_registry.get(phone, 0) if phone else 0)
        if phone and phone_registry.get(phone, 0) > 1:
            duplicate_phone_rows += 1
        addr_key = normalize_name(row.get("casa_endereco"))
        row["endereco_repetido_count"] = str(address_counter.get(addr_key, 0) if addr_key else 0)
        row["endereco_suspeito_contaminacao"] = "1" if addr_key and address_counter.get(addr_key, 0) >= 5 else "0"
        if row["endereco_suspeito_contaminacao"] == "1":
            duplicated_address_rows += 1
        if row.get("casa_endereco_mossoro") == "1":
            mossoro_count += 1

    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_rows": len(rows),
        "apply_network": bool(apply_network),
        "casa_auth_ok": bool(casa_auth_ok),
        "summary": {
            **dict(summary),
            "sem_cnpj": sum(1 for row in output if row.get("cnpj_faltante") == "1"),
            "com_cnpj": sum(1 for row in output if row.get("cnpj_faltante") == "0"),
            "telefone_duplicado_rows": duplicate_phone_rows,
            "endereco_repetido_rows": duplicated_address_rows,
            "endereco_mossoro_rows": mossoro_count,
            "linkedin_public_links_rows": sum(1 for row in output if str(row.get("public_role_links") or "").strip()),
        },
    }
    save_json(CACHE_JSON, cache)
    return output, summary_payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_XLSX))
    parser.add_argument("--output", default=str(OUTPUT_XLSX))
    parser.add_argument("--no-network", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    rows = parse_xlsx(Path(args.input))
    if args.limit:
        rows = rows[: max(1, int(args.limit))]
    apply_network = not bool(args.no_network)
    enriched, summary_payload = enrich_rows(rows, apply_network=apply_network)
    dynamic_write_xlsx(enriched, Path(args.output))
    write_csv(enriched, OUTPUT_CSV)
    save_json(SUMMARY_JSON, summary_payload)
    log(f"[OFFLINE_ENRICH_OK] output={args.output}")
    log("[OFFLINE_ENRICH_SUMMARY] " + " ".join(f"{k}={v}" for k, v in sorted(summary_payload["summary"].items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
