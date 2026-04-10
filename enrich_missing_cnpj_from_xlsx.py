from __future__ import annotations

import json
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import parse_qs, unquote, urlparse

import requests

from config.config_loader import get_config_value


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido_final.xlsx")
CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\incremental_enrichment_cache.json")

CASA_BASE_URL = "https://api.casadosdados.com.br"
CASA_ENDPOINT = "/v4/cnpj/{cnpj}"
CASA_SEARCH_ENDPOINT = "/v5/cnpj/pesquisa"
GOOGLE_TIMEOUT_SEC = 12
CASA_TIMEOUT_SEC = 15
OPENROUTER_TIMEOUT_SEC = 20
LOOKUP_DELAY_SEC = 1.0
OPENROUTER_MODEL = "openrouter/auto"
TARGET_DOMAINS = ("cnpj.biz", "consultacnpj.com")
BLOCKED_GENERIC_PHONES = {
    "18043309723",
    "1804330972",
    "11999999999",
    "11999998888",
    "00999999977",
    "999999999",
}


def log(message: str) -> None:
    print(str(message), flush=True)


def normalize_cnpj(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) == 14 else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    return digits if 10 <= len(digits) <= 11 and len(set(digits)) > 1 else ""


def phone_is_mobile(value: str) -> bool:
    return len(value) == 11 and value[2] == "9"


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^lead mand digital\s*-\s*", "", text)
    text = re.sub(r"^lead mand digital\s*", "", text)
    text = re.sub(r"\bnegocio\b|\bnegócio\b", " ", text)
    text = re.sub(r"[^a-z0-9à-ÿ\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def load_cache() -> Dict[str, Any]:
    if not CACHE_JSON.exists():
        return {}
    try:
        data = json.loads(CACHE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_cache(cache: Dict[str, Any]) -> None:
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def bucket(cache: Dict[str, Any], key: str) -> Dict[str, Any]:
    current = cache.get(key)
    if isinstance(current, dict):
        return current
    current = {}
    cache[key] = current
    return current


def parse_xlsx(path: Path) -> List[Dict[str, Any]]:
    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    with zipfile.ZipFile(path) as zf:
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = workbook.find("a:sheets", ns)
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", ns):
                shared_strings.append("".join(text.text or "" for text in si.iterfind(".//a:t", ns)))
        first_sheet = list(sheets)[0]
        rel_target = relmap[first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]].lstrip("/")
        target = rel_target if rel_target.startswith("xl/") else "xl/" + rel_target
        worksheet = ET.fromstring(zf.read(target))
        sheet_data = worksheet.find("a:sheetData", ns)
        headers: Dict[str, str] = {}
        rows: List[Dict[str, Any]] = []
        for row_idx, row in enumerate(sheet_data, start=1):
            values: Dict[str, str] = {}
            for cell in row.findall("a:c", ns):
                ref = cell.attrib.get("r", "")
                col = re.sub(r"\d+", "", ref)
                if cell.attrib.get("t") == "inlineStr":
                    values[col] = "".join(text.text or "" for text in cell.iterfind(".//a:t", ns))
                    continue
                value_node = cell.find("a:v", ns)
                value = "" if value_node is None else (value_node.text or "")
                if cell.attrib.get("t") == "s" and value.isdigit():
                    value = shared_strings[int(value)]
                values[col] = value
            if row_idx == 1:
                headers = values
                continue
            item: Dict[str, Any] = {}
            for col, header in headers.items():
                item[str(header or "").strip()] = str(values.get(col, "") or "").strip()
            rows.append(item)
        return rows


def google_search_html(query: str) -> str:
    response = requests.get(
        "https://www.google.com/search",
        params={"q": query, "hl": "pt-BR", "num": "8"},
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
        timeout=GOOGLE_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.text


def brave_search_html(query: str) -> str:
    response = requests.get(
        "https://search.brave.com/search",
        params={"q": query},
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
        timeout=GOOGLE_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.text


def extract_google_urls(html: str, *, target_domains: Iterable[str] | None = None) -> List[str]:
    domains = tuple(target_domains or ())
    urls: List[str] = []
    seen = set()
    for raw in re.findall(r'href="([^"]+)"', str(html or "")):
        candidate = ""
        if raw.startswith("/url?"):
            parsed = urlparse(raw)
            candidate = parse_qs(parsed.query).get("q", [""])[0]
        elif raw.startswith("http://") or raw.startswith("https://"):
            candidate = raw
        candidate = unquote(candidate)
        if not candidate:
            continue
        host = urlparse(candidate).netloc.lower()
        if domains and not any(domain in host for domain in domains):
            continue
        if candidate not in seen:
            seen.add(candidate)
            urls.append(candidate)
    return urls


def extract_cnpj(text: str) -> str:
    match = re.search(r"\b(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})\b", str(text or ""))
    return normalize_cnpj(match.group(1)) if match else ""


def fetch_page_text(url: str) -> str:
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
        timeout=GOOGLE_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.text


def find_cnpj_from_query(query: str, lookup_cache: Dict[str, Any]) -> str:
    cached = normalize_cnpj(lookup_cache.get(query))
    if cached:
        return cached
    cnpj = ""
    try:
        html = google_search_html(query)
        for url in extract_google_urls(html, target_domains=TARGET_DOMAINS)[:4]:
            page = fetch_page_text(url)
            cnpj = extract_cnpj(page)
            if cnpj:
                break
    except Exception as exc:
        lookup_cache[f"erro::{query}"] = str(exc)[:300]
    lookup_cache[query] = cnpj
    return cnpj


def find_cnpj_from_brave(query: str, lookup_cache: Dict[str, Any]) -> str:
    cache_key = f"brave::{query}"
    cached = normalize_cnpj(lookup_cache.get(cache_key))
    if cached:
        return cached
    cnpj = ""
    try:
        html = brave_search_html(query)
        cnpj = extract_cnpj(html)
    except Exception as exc:
        lookup_cache[f"erro::{cache_key}"] = str(exc)[:300]
    lookup_cache[cache_key] = cnpj
    return cnpj


def suggest_names_openrouter(name: str, ai_cache: Dict[str, Any]) -> List[str]:
    base = normalize_name(name)
    cached = ai_cache.get(base)
    if isinstance(cached, list):
        return [normalize_name(item) for item in cached if normalize_name(item)]
    api_key = str(get_config_value("openrouter_api_key", "") or "").strip()
    if not api_key:
        ai_cache[base] = []
        return []
    prompt = (
        "Retorne JSON puro com razao_social e nome_completo para buscar CNPJ. "
        f"Empresa informada: {base!r}."
    )
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 120,
            },
            timeout=OPENROUTER_TIMEOUT_SEC,
        )
        response.raise_for_status()
        content = str(response.json()["choices"][0]["message"]["content"] or "")
        match = re.search(r"\{.*\}", content, re.S)
        parsed = json.loads(match.group(0) if match else content)
    except Exception as exc:
        ai_cache[f"erro::{base}"] = str(exc)[:300]
        ai_cache[base] = []
        return []
    names = []
    for key in ("razao_social", "nome_completo"):
        value = normalize_name(parsed.get(key))
        if value and value not in names:
            names.append(value)
    ai_cache[base] = names
    return names


def find_missing_cnpj(row: Dict[str, Any], cache: Dict[str, Any]) -> str:
    lookup_cache = bucket(cache, "cnpj_lookup")
    ai_cache = bucket(cache, "openrouter_lookup")
    name = normalize_name(row.get("nome_final"))
    if not name:
        return ""
    queries = [
        f"{name} CNPJ site:cnpj.biz",
        f"{name} CNPJ site:consultacnpj.com",
        f"{name} razão social",
    ]
    for query in queries:
        cnpj = find_cnpj_from_query(query, lookup_cache)
        save_cache(cache)
        if cnpj:
            return cnpj
        time.sleep(LOOKUP_DELAY_SEC)
    brave_queries = [f"{name} CNPJ", f"{name} CNPJ empresa", f"{name} razão social CNPJ"]
    for query in brave_queries:
        cnpj = find_cnpj_from_brave(query, lookup_cache)
        save_cache(cache)
        if cnpj:
            return cnpj
        time.sleep(LOOKUP_DELAY_SEC)
    for suggested in suggest_names_openrouter(name, ai_cache):
        for domain in TARGET_DOMAINS:
            query = f"{suggested} CNPJ site:{domain}"
            cnpj = find_cnpj_from_query(query, lookup_cache)
            save_cache(cache)
            if cnpj:
                return cnpj
            time.sleep(LOOKUP_DELAY_SEC)
        cnpj = find_cnpj_from_brave(f"{suggested} CNPJ", lookup_cache)
        save_cache(cache)
        if cnpj:
            return cnpj
        time.sleep(LOOKUP_DELAY_SEC)
    save_cache(cache)
    return ""


def search_casa_by_name(name: str, casa_search_cache: Dict[str, Any]) -> str:
    normalized = normalize_name(name)
    if not normalized:
        return ""
    cached = casa_search_cache.get(normalized)
    if isinstance(cached, dict):
        return normalize_cnpj(cached.get("cnpj"))
    api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
    body = {
        "busca_textual": [
            {
                "texto": [normalized],
                "tipo_busca": "exata",
                "razao_social": True,
                "nome_fantasia": True,
                "nome_socio": False,
            }
        ],
        "situacao_cadastral": ["ATIVA"],
        "limite": 3,
        "pagina": 1,
    }
    try:
        response = requests.post(
            CASA_BASE_URL + CASA_SEARCH_ENDPOINT,
            params={"tipo_resultado": "completo"},
            headers={"api-key": api_key, "accept": "application/json", "Content-Type": "application/json", "user-agent": "bot-sdr-ai/1.0"},
            json=body,
            timeout=CASA_TIMEOUT_SEC,
        )
        log(f"[CASA_BUSCA_NOME_STATUS] nome={normalized} status_code={response.status_code}")
        if response.status_code != 200:
            casa_search_cache[normalized] = {"status_code": response.status_code, "erro": response.text[:300], "cnpj": ""}
            log(f"[CASA_BUSCA_NOME_ERRO_{response.status_code}] nome={normalized} erro={response.text[:180]}")
            return ""
        payload = response.json()
    except Exception as exc:
        casa_search_cache[normalized] = {"status_code": 0, "erro": str(exc)[:300], "cnpj": ""}
        log(f"[CASA_BUSCA_NOME_ERRO_0] nome={normalized} erro={exc}")
        return ""
    cnpjs = payload.get("cnpjs") if isinstance(payload, dict) else []
    chosen = ""
    chosen_payload: Dict[str, Any] = {}
    if isinstance(cnpjs, list) and cnpjs:
        for item in cnpjs:
            if not isinstance(item, dict):
                continue
            candidate = normalize_cnpj(item.get("cnpj"))
            if not candidate:
                continue
            if str(item.get("matriz_filial") or "").upper() == "MATRIZ":
                chosen = candidate
                chosen_payload = item
                break
            if not chosen:
                chosen = candidate
                chosen_payload = item
    casa_search_cache[normalized] = {"status_code": 200, "cnpj": chosen, "raw": chosen_payload, "total": payload.get("total") if isinstance(payload, dict) else ""}
    if chosen:
        log(f"[CNPJ_ENCONTRADO_CASA_NOME] nome={normalized} cnpj={chosen}")
    else:
        log(f"[CNPJ_NAO_ENCONTRADO_CASA_NOME] nome={normalized}")
    return chosen


def walk_json(node: Any) -> Iterable[Any]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk_json(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk_json(item)


def first_text(payload: Dict[str, Any], *keys: str) -> str:
    lowered = {str(key).lower(): value for key, value in payload.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def value_candidates(value: Any, keys: Iterable[str]) -> List[str]:
    output: List[str] = []
    if isinstance(value, dict):
        lowered = {str(key).lower(): raw for key, raw in value.items()}
        for wanted in keys:
            for key, raw in lowered.items():
                if wanted in key:
                    output.extend(value_candidates(raw, ()))
        if not output:
            for raw in value.values():
                output.extend(value_candidates(raw, ()))
        return output
    if isinstance(value, list):
        for item in value:
            output.extend(value_candidates(item, keys))
        return output
    text = str(value or "").strip()
    return [text] if text else []


def extract_casa_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    phones: List[str] = []
    emails: List[str] = []
    sites: List[str] = []
    socios: List[str] = []
    seen_phone = set()
    seen_email = set()
    seen_site = set()
    seen_socio = set()
    for node in walk_json(payload):
        if not isinstance(node, dict):
            continue
        for key, value in node.items():
            lowered = str(key).lower()
            if any(token in lowered for token in ("telefone", "telefon", "celular", "fone", "whatsapp")):
                for candidate in value_candidates(value, ("telefone", "numero", "completo", "valor", "value", "whatsapp")):
                    phone = normalize_phone(candidate)
                    if phone and phone not in seen_phone:
                        seen_phone.add(phone)
                        phones.append(phone)
            if "mail" in lowered:
                for candidate in value_candidates(value, ("email", "endereco", "address", "valor", "value")):
                    email = str(candidate or "").strip().lower()
                    if "@" in email and email not in seen_email:
                        seen_email.add(email)
                        emails.append(email)
            if any(token in lowered for token in ("site", "website", "dominio", "url")):
                for candidate in value_candidates(value, ("site", "website", "dominio", "url", "valor", "value")):
                    site = str(candidate or "").strip()
                    if site and site not in seen_site:
                        seen_site.add(site)
                        sites.append(site)
            if any(token in lowered for token in ("socio", "quadro_societario", "administrador", "responsavel")):
                for candidate in value_candidates(value, ("nome", "nome_socio", "nome_completo", "razao_social")):
                    socio = str(candidate or "").strip()
                    if socio and len(socio) > 3 and socio.lower() not in seen_socio:
                        seen_socio.add(socio.lower())
                        socios.append(socio)
    return {
        "nome_final": first_text(payload, "nome_fantasia", "fantasia", "razao_social", "nome_empresarial", "nome"),
        "razao_social": first_text(payload, "razao_social", "nome_empresarial"),
        "nome_fantasia": first_text(payload, "nome_fantasia", "fantasia"),
        "telefones": phones,
        "emails": emails,
        "sites": sites,
        "socios": socios,
    }


def fetch_casa(cnpj: str, casa_cache: Dict[str, Any]) -> Dict[str, Any]:
    cached = casa_cache.get(cnpj)
    if isinstance(cached, dict) and "status_code" in cached:
        return cached
    api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
    url = CASA_BASE_URL + CASA_ENDPOINT.format(cnpj=cnpj)
    try:
        response = requests.get(
            url,
            headers={"api-key": api_key, "accept": "application/json", "user-agent": "bot-sdr-ai/1.0"},
            timeout=CASA_TIMEOUT_SEC,
        )
        log(f"[CASA_DADOS_STATUS] cnpj={cnpj} status_code={response.status_code}")
        if response.status_code != 200:
            payload = {"status_code": response.status_code, "erro": response.text[:500]}
            casa_cache[cnpj] = payload
            log(f"[CASA_DADOS_ERRO_{response.status_code}] cnpj={cnpj} erro={response.text[:180]}")
            return payload
        raw = response.json()
    except Exception as exc:
        payload = {"status_code": 0, "erro": str(exc)[:500]}
        casa_cache[cnpj] = payload
        log(f"[CASA_DADOS_ERRO_0] cnpj={cnpj} erro={exc}")
        return payload
    fields = extract_casa_fields(raw if isinstance(raw, dict) else {})
    payload = {"status_code": 200, "raw": raw, **fields}
    casa_cache[cnpj] = payload
    log(f"[CASA_DADOS_OK] cnpj={cnpj}")
    return payload


def validate_casa_auth(casa_cache: Dict[str, Any]) -> bool:
    api_key = str(get_config_value("casa_dos_dados_api_key", "") or "").strip()
    key_fingerprint = f"{len(api_key)}:{api_key[:6]}:{api_key[-6:]}"
    auth_status = casa_cache.get("__auth_status")
    cached_fingerprint = casa_cache.get("__auth_key_fingerprint")
    if cached_fingerprint == key_fingerprint and auth_status == 200:
        return True
    if cached_fingerprint == key_fingerprint and auth_status in {401, 403}:
        return False
    try:
        response = requests.get(
            CASA_BASE_URL + "/v5/saldo",
            headers={"api-key": api_key, "accept": "application/json", "user-agent": "bot-sdr-ai/1.0"},
            timeout=CASA_TIMEOUT_SEC,
        )
        casa_cache["__auth_status"] = response.status_code
        casa_cache["__auth_key_fingerprint"] = key_fingerprint
        casa_cache["__auth_body"] = response.text[:300]
        log(f"[CASA_DADOS_AUTH_STATUS] status_code={response.status_code}")
        if response.status_code != 200:
            log(f"[CASA_DADOS_AUTH_ERRO_{response.status_code}] erro={response.text[:180]}")
        return response.status_code == 200
    except Exception as exc:
        casa_cache["__auth_status"] = 0
        casa_cache["__auth_body"] = str(exc)[:300]
        log(f"[CASA_DADOS_AUTH_ERRO_0] erro={exc}")
        return False


def best_phone(phones: List[str]) -> str:
    valid = [phone for phone in phones if normalize_phone(phone) and normalize_phone(phone) not in BLOCKED_GENERIC_PHONES]
    if not valid:
        return ""
    counts = Counter(valid)
    mobiles = [phone for phone in valid if phone_is_mobile(phone)]
    if mobiles:
        mobiles.sort(key=lambda phone: (counts[phone], phone))
        return mobiles[0]
    valid.sort(key=lambda phone: (counts[phone], -len(phone), phone))
    return valid[0]


def fallback_phone_google(name: str, phone_cache: Dict[str, Any]) -> str:
    key = normalize_name(name)
    cached = normalize_phone(phone_cache.get(key))
    if cached and cached not in BLOCKED_GENERIC_PHONES:
        return cached
    phone = ""
    try:
        html = google_search_html(f"{key} telefone")
        matches = re.findall(r"(\(?\d{2}\)?\s?\d{4,5}-?\d{4})", html)
        phone = best_phone([normalize_phone(match) for match in matches])
    except Exception as exc:
        phone_cache[f"erro::{key}"] = str(exc)[:300]
    phone_cache[key] = phone
    return phone


def excel_column_name(index: int) -> str:
    output = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        output = chr(65 + remainder) + output
    return output


def xml_escape(value: Any) -> str:
    return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def write_xlsx(rows: List[Dict[str, Any]], path: Path) -> None:
    headers = ["deal_id", "org_id", "nome_final", "cnpj", "telefone", "email", "score", "acao"]
    data = [headers]
    for row in rows:
        data.append([row.get(header, "") for header in headers])
    sheet_rows: List[str] = []
    for row_idx, row in enumerate(data, start=1):
        cells: List[str] = []
        for col_idx, value in enumerate(row, start=1):
            ref = f"{excel_column_name(col_idx)}{row_idx}"
            if str(value).isdigit() and col_idx in {1, 2, 7}:
                cells.append(f'<c r="{ref}"><v>{int(value)}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{xml_escape(value)}</t></is></c>')
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
  <sheets><sheet name="deals_enriquecido_final" sheetId="1" r:id="rId1"/></sheets>
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


def main() -> int:
    rows = parse_xlsx(INPUT_XLSX)
    cache = load_cache()
    casa_cache = bucket(cache, "casa")
    casa_search_cache = bucket(cache, "casa_search")
    phone_cache = bucket(cache, "phone_google")
    casa_auth_ok = validate_casa_auth(casa_cache)
    save_cache(cache)
    for row in rows:
        if normalize_phone(row.get("telefone")) in BLOCKED_GENERIC_PHONES:
            row["telefone"] = ""
    targets = [row for row in rows if str(row.get("acao") or "").strip().upper() in {"UPDATE", "RESGATAR"}]
    targets.sort(key=lambda row: (1 if normalize_cnpj(row.get("cnpj")) else 0, int(str(row.get("deal_id") or "0") or 0)))
    log(f"[FILTRO] processar_update={len(targets)} total={len(rows)} casa_auth_ok={casa_auth_ok}")
    cnpj_found = 0
    casa_ok = 0
    phone_google = 0
    for index, row in enumerate(targets, start=1):
        deal_id = row.get("deal_id", "")
        name = row.get("nome_final", "")
        log(f"[PROCESSANDO] {index}/{len(targets)} deal={deal_id} empresa={name}")
        cnpj = normalize_cnpj(row.get("cnpj"))
        if not cnpj and casa_auth_ok:
            cnpj = search_casa_by_name(name, casa_search_cache)
            save_cache(cache)
        if not cnpj:
            cnpj = find_missing_cnpj(row, cache)
        if cnpj and not normalize_cnpj(row.get("cnpj")):
            row["cnpj"] = cnpj
            cnpj_found += 1
            log(f"[CNPJ_ENCONTRADO] deal={deal_id} cnpj={cnpj}")
        if cnpj and casa_auth_ok:
            payload = fetch_casa(cnpj, casa_cache)
            save_cache(cache)
            if int(payload.get("status_code") or 0) == 200:
                casa_ok += 1
                casa_phone = best_phone(list(payload.get("telefones") or []))
                if not normalize_phone(row.get("telefone")) and casa_phone:
                    row["telefone"] = casa_phone
                if not str(row.get("email") or "").strip() and payload.get("emails"):
                    row["email"] = payload["emails"][0]
                if payload.get("nome_final") and not normalize_name(row.get("nome_final")):
                    row["nome_final"] = normalize_name(payload["nome_final"])
        elif cnpj:
            log(f"[CASA_DADOS_SKIP_AUTH] deal={deal_id} cnpj={cnpj}")
        if not normalize_phone(row.get("telefone")):
            fallback = fallback_phone_google(name, phone_cache)
            save_cache(cache)
            if fallback:
                row["telefone"] = fallback
                phone_google += 1
                log(f"[TELEFONE_GOOGLE] deal={deal_id} telefone={fallback}")
        row["score"] = str(int(str(row.get("score") or "0") or 0) + (2 if normalize_cnpj(row.get("cnpj")) else 0) + (2 if normalize_phone(row.get("telefone")) else 0) + (1 if str(row.get("email") or "").strip() else 0))
        save_cache(cache)
    write_xlsx(rows, OUTPUT_XLSX)
    remaining = sum(1 for row in rows if not normalize_cnpj(row.get("cnpj")))
    log(f"[EXPORTADO] arquivo={OUTPUT_XLSX} linhas={len(rows)}")
    log(f"[RESUMO_FINAL] processados={len(targets)} cnpj_encontrados={cnpj_found} casa_ok={casa_ok} telefone_google={phone_google} sem_cnpj_restantes={remaining}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
