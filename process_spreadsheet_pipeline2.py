from __future__ import annotations

import json
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, unquote, urlparse

import requests

from config.config_loader import get_config_value
from services.casa_dos_dados_client import CasaDosDadosClient, CasaDosDadosError


INPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals-25681240-9.xlsx")
OUTPUT_XLSX = Path(r"C:\Users\Asus\Downloads\deals_enriquecido.xlsx")
CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\casa_dos_dados_cache.json")
MAX_MASTERS = 860
CACHE_DELAY_SEC = 0.6
LOOKUP_DELAY_SEC = 1.2
GOOGLE_TIMEOUT_SEC = 12
OPENROUTER_TIMEOUT_SEC = 20
OPENROUTER_MODEL = "openrouter/auto"
TARGET_LOOKUP_DOMAINS = ("cnpj.biz", "consultacnpj.com")

GENERIC_NAME_TOKENS = {
    "supermercado",
    "supermercados",
    "mercado",
    "mercados",
    "loja",
    "lojas",
    "comercio",
    "comercioe",
    "empresa",
    "empresas",
    "negocio",
    "negocios",
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
    return digits


def phone_is_valid(value: str) -> bool:
    return 10 <= len(value) <= 11 and len(set(value)) > 1


def phone_is_mobile(value: str) -> bool:
    return len(value) == 11 and value[2] == "9"


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"^lead mand digital\s*-\s*", "", text)
    text = re.sub(r"^lead mand digital\s*", "", text)
    text = re.sub(r"\bnegocio\b|\bnegócio\b", " ", text)
    text = re.sub(r"[^a-z0-9à-ÿ\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalized_key_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_name(value))


def is_generic_name(value: str) -> bool:
    tokens = [token for token in normalize_name(value).split() if token]
    if not tokens:
        return True
    return len(tokens) <= 2 and all(token in GENERIC_NAME_TOKENS for token in tokens)


def score_record(record: Dict[str, Any]) -> int:
    score = 0
    if bool(record.get("telefone_valido")):
        score += 2
    if bool(record.get("cnpj")):
        score += 2
    if bool(record.get("email")):
        score += 1
    if not bool(record.get("nome_generico")):
        score += 1
    return score


def parse_xlsx(path: Path) -> List[Dict[str, str]]:
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
        target = "xl/" + relmap[first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]].lstrip("/")
        worksheet = ET.fromstring(zf.read(target))
        sheet_data = worksheet.find("a:sheetData", ns)
        header_by_col: Dict[str, str] = {}
        rows: List[Dict[str, str]] = []
        for row_idx, row in enumerate(sheet_data, start=1):
            values_by_col: Dict[str, str] = {}
            for cell in row.findall("a:c", ns):
                ref = cell.attrib.get("r", "")
                col = re.sub(r"\d+", "", ref)
                value_node = cell.find("a:v", ns)
                value = "" if value_node is None else (value_node.text or "")
                if cell.attrib.get("t") == "s" and value.isdigit():
                    value = shared_strings[int(value)]
                values_by_col[col] = value
            if row_idx == 1:
                header_by_col = values_by_col
                continue
            record: Dict[str, str] = {}
            for col, header in header_by_col.items():
                record[str(header or "").strip()] = str(values_by_col.get(col, "") or "").strip()
            rows.append(record)
        return rows


def first_present(row: Dict[str, str], *names: str) -> str:
    for name in names:
        value = str(row.get(name, "") or "").strip()
        if value:
            return value
    return ""


def stage_is_archived(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return bool(text) and ("arquiv" in text or "archive" in text)


def build_records(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for row in rows:
        deal_id = int(re.sub(r"\D+", "", first_present(row, "Negócio - ID", "NegÃ³cio - ID")) or "0")
        org_id = int(re.sub(r"\D+", "", first_present(row, "Organização - ID", "OrganizaÃ§Ã£o - ID")) or "0")
        if not deal_id or not org_id:
            continue
        company = first_present(
            row,
            "Organização - Nome",
            "OrganizaÃ§Ã£o - Nome",
            "Negócio - Organização",
            "NegÃ³cio - OrganizaÃ§Ã£o",
        )
        phones: List[str] = []
        for field in (
            "Pessoa - Telefone - Celular",
            "Pessoa - Telefone - Trabalho",
            "Pessoa - Telefone - Residencial",
            "Pessoa - Telefone - Outros",
        ):
            phone = normalize_phone(row.get(field, ""))
            if phone and phone not in phones:
                phones.append(phone)
        cnpj = normalize_cnpj(
            first_present(
                row,
                "Organização - CNPJ",
                "OrganizaÃ§Ã£o - CNPJ",
                "Pessoa - CNPJ",
            )
        )
        stage = ""
        for key, value in row.items():
            lowered = str(key or "").strip().lower()
            if "stage" in lowered or "etapa" in lowered:
                stage = str(value or "").strip()
                break
        record = {
            "deal_id": deal_id,
            "org_id": org_id,
            "nome_empresa": company,
            "nome_final": normalize_name(company),
            "nome_normalizado": normalized_key_name(company),
            "nome_generico": is_generic_name(company),
            "telefone": phones[0] if phones else "",
            "telefones": phones,
            "telefone_valido": bool(phones and phone_is_valid(phones[0])),
            "email": "",
            "stage": stage,
            "cnpj": cnpj,
            "score": 0,
            "acao": "",
            "master_deal_id": 0,
            "site": "",
            "socios": "",
            "cnpj_busca": "",
            "cnpj_lookup_status": "",
            "variacoes_nome": [],
        }
        if cnpj:
            log(f"[CNPJ_ENCONTRADO] deal={deal_id} cnpj={cnpj}")
        if record["telefone_valido"]:
            log(f"[TELEFONE_VALIDO] deal={deal_id} telefone={record['telefone']}")
        records.append(record)
    return records


def define_masters(records: List[Dict[str, Any]]) -> None:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        key = f"cnpj:{record['cnpj']}" if record.get("cnpj") else f"name:{record['nome_normalizado']}"
        if key in {"cnpj:", "name:"}:
            key = f"deal:{record['deal_id']}"
        groups[key].append(record)

    for key, group in groups.items():
        ordered = sorted(group, key=lambda item: int(item.get("deal_id") or 0))
        master = ordered[0]
        master["acao"] = "RESGATAR" if stage_is_archived(master.get("stage")) else "UPDATE"
        master["master_deal_id"] = master["deal_id"]
        master["is_master"] = True
        log(f"[MASTER_DEFINIDO] deal={master['deal_id']} chave={key}")
        if master["acao"] == "RESGATAR":
            log(f"[RESGATADO] deal={master['deal_id']} stage={master.get('stage')}")
        for duplicate in ordered[1:]:
            duplicate["acao"] = "ARCHIVE"
            duplicate["master_deal_id"] = master["deal_id"]
            duplicate["is_master"] = False
            log(f"[DUPLICADO] deal={duplicate['deal_id']} master={master['deal_id']}")


def choose_best_phone(phones: List[str], repeated_phones: Counter) -> str:
    candidates = [phone for phone in phones if phone_is_valid(phone)]
    candidates = [phone for phone in candidates if repeated_phones.get(phone, 0) <= 3]
    if not candidates:
        return ""
    mobile = [phone for phone in candidates if phone_is_mobile(phone)]
    if mobile:
        mobile.sort(key=lambda phone: (int(repeated_phones.get(phone, 0)), phone))
        return mobile[0]
    candidates.sort(key=lambda phone: (int(repeated_phones.get(phone, 0)), -len(phone), phone))
    return candidates[0]


def prepare_records(records: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    repeated_phones = Counter()
    for record in records:
        for phone in record.get("telefones") or []:
            if phone:
                repeated_phones[phone] += 1

    masters: List[Dict[str, Any]] = []
    duplicates: List[Dict[str, Any]] = []
    for record in records:
        best_phone = choose_best_phone(list(record.get("telefones") or []), repeated_phones)
        record["telefone"] = best_phone
        record["telefone_valido"] = phone_is_valid(best_phone)
        record["score"] = score_record(record)
        if record["acao"] in {"UPDATE", "RESGATAR"}:
            masters.append(record)
        else:
            duplicates.append(record)

    masters.sort(key=lambda item: (-int(item.get("score") or 0), int(item.get("deal_id") or 0)))
    return masters[:MAX_MASTERS], duplicates


def load_cache(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_cache(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def cache_bucket(cache: Dict[str, Any], key: str) -> Dict[str, Any]:
    current = cache.get(key)
    if isinstance(current, dict):
        return current
    current = {}
    cache[key] = current
    return current


def extract_cnpj_from_text(text: str) -> str:
    match = re.search(r"\b(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})\b", str(text or ""))
    return normalize_cnpj(match.group(1)) if match else ""


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


def extract_google_candidate_urls(html: str) -> List[str]:
    urls: List[str] = []
    seen = set()
    for raw in re.findall(r'href="([^"]+)"', str(html or "")):
        candidate = ""
        if raw.startswith("/url?"):
            parsed = urlparse(raw)
            query = parse_qs(parsed.query)
            candidate = query.get("q", [""])[0]
        elif raw.startswith("http://") or raw.startswith("https://"):
            candidate = raw
        candidate = unquote(candidate)
        if not candidate:
            continue
        host = urlparse(candidate).netloc.lower()
        if any(domain in host for domain in TARGET_LOOKUP_DOMAINS):
            if candidate not in seen:
                seen.add(candidate)
                urls.append(candidate)
    return urls


def fetch_first_cnpj_from_google(query: str) -> str:
    html = google_search_html(query)
    for url in extract_google_candidate_urls(html)[:5]:
        try:
            response = requests.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
                    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                },
                timeout=GOOGLE_TIMEOUT_SEC,
            )
            response.raise_for_status()
        except Exception:
            continue
        cnpj = extract_cnpj_from_text(response.text)
        if cnpj:
            return cnpj
    return ""


def generate_name_variations(name: str) -> List[str]:
    base = normalize_name(name)
    variations: List[str] = []
    if base:
        variations.append(base)
    tokens = [token for token in base.split() if token and token not in GENERIC_NAME_TOKENS]
    if tokens:
        variations.append(" ".join(tokens))
        if len(tokens) > 1:
            variations.append(" ".join(tokens[:2]))
    compact = re.sub(r"\b(ltda|me|epp|eireli|sa|s a|mei)\b", " ", base)
    compact = re.sub(r"\s+", " ", compact).strip()
    if compact:
        variations.append(compact)
    output: List[str] = []
    seen = set()
    for item in variations:
        if item and item not in seen:
            seen.add(item)
            output.append(item)
    return output


def suggest_name_variations_openrouter(name: str) -> List[str]:
    api_key = str(get_config_value("openrouter_api_key", "") or "").strip()
    if not api_key:
        return []
    prompt = (
        "Retorne JSON puro com as chaves razao_social, nome_completo e variacoes. "
        "O objetivo e descobrir o CNPJ de uma empresa brasileira. "
        f"Nome informado: {name!r}. "
        "variacoes deve ser uma lista curta, maximo 4 itens, contendo nomes para busca."
    )
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": OPENROUTER_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 180,
        },
        timeout=OPENROUTER_TIMEOUT_SEC,
    )
    response.raise_for_status()
    payload = response.json()
    content = ""
    choices = payload.get("choices") or []
    if choices:
        message = choices[0].get("message") or {}
        content = str(message.get("content") or "").strip()
    if not content:
        return []
    match = re.search(r"\{.*\}", content, re.S)
    raw_json = match.group(0) if match else content
    try:
        parsed = json.loads(raw_json)
    except Exception:
        return []
    output: List[str] = []
    for key in ("razao_social", "nome_completo"):
        value = normalize_name(parsed.get(key))
        if value:
            output.append(value)
    for item in parsed.get("variacoes") or []:
        value = normalize_name(item)
        if value:
            output.append(value)
    deduped: List[str] = []
    seen = set()
    for item in output:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped[:6]


def discover_missing_cnpjs(records: List[Dict[str, Any]]) -> None:
    cache = load_cache(CACHE_JSON)
    cnpj_lookup_cache = cache_bucket(cache, "cnpj_lookup")
    openrouter_cache = cache_bucket(cache, "openrouter_lookup")
    if not any(str(record.get("stage") or "").strip() for record in records):
        log("[ARQUIVADOS_REVISAO] sem_coluna_stage_na_planilha")
    for record in records:
        if normalize_cnpj(record.get("cnpj")):
            continue
        base_name = normalize_name(record.get("nome_final") or record.get("nome_empresa") or "")
        variations = generate_name_variations(base_name)
        record["variacoes_nome"] = list(variations)
        found_cnpj = ""
        queries: List[str] = []

        initial_queries = [
            f"{base_name} CNPJ site:cnpj.biz OR site:consultacnpj.com".strip(),
            f"{base_name} razão social".strip(),
        ]
        for query in initial_queries:
            if query and query not in queries:
                queries.append(query)
        for variation in variations:
            for suffix in (
                "CNPJ site:cnpj.biz OR site:consultacnpj.com",
                "razão social",
            ):
                query = f"{variation} {suffix}".strip()
                if query and query not in queries:
                    queries.append(query)

        for query in queries:
            cached = normalize_cnpj(cnpj_lookup_cache.get(query))
            if cached:
                found_cnpj = cached
                break
            try:
                cnpj = fetch_first_cnpj_from_google(query)
            except Exception:
                cnpj = ""
            cnpj_lookup_cache[query] = cnpj
            save_cache(CACHE_JSON, cache)
            if cnpj:
                found_cnpj = cnpj
                break
            time.sleep(LOOKUP_DELAY_SEC)

        if not found_cnpj:
            cached_variations = openrouter_cache.get(base_name)
            if isinstance(cached_variations, list):
                ai_variations = [normalize_name(item) for item in cached_variations if normalize_name(item)]
            else:
                try:
                    ai_variations = suggest_name_variations_openrouter(base_name)
                except Exception:
                    ai_variations = []
                openrouter_cache[base_name] = ai_variations
                save_cache(CACHE_JSON, cache)
            for variation in ai_variations:
                query = f"{variation} CNPJ site:cnpj.biz OR site:consultacnpj.com".strip()
                if not query or query in queries:
                    continue
                queries.append(query)
                cached = normalize_cnpj(cnpj_lookup_cache.get(query))
                if cached:
                    found_cnpj = cached
                    break
                try:
                    cnpj = fetch_first_cnpj_from_google(query)
                except Exception:
                    cnpj = ""
                cnpj_lookup_cache[query] = cnpj
                save_cache(CACHE_JSON, cache)
                if cnpj:
                    found_cnpj = cnpj
                    break
                time.sleep(LOOKUP_DELAY_SEC)

        record["cnpj_busca"] = queries[0] if queries else ""
        if found_cnpj:
            record["cnpj"] = found_cnpj
            record["cnpj_lookup_status"] = "GOOGLE"
            log(f"[CNPJ_ENCONTRADO_GOOGLE] deal={record['deal_id']} cnpj={found_cnpj}")
        else:
            record["cnpj_lookup_status"] = "NAO_ENCONTRADO"
            log(f"[CNPJ_NAO_ENCONTRADO] deal={record['deal_id']} empresa={record.get('nome_final') or record.get('nome_empresa')}")


def enrich_with_casa(records: List[Dict[str, Any]]) -> None:
    cache = load_cache(CACHE_JSON)
    casa_cache = cache_bucket(cache, "casa")
    casa = CasaDosDadosClient(timeout=12.0)
    for record in records:
        cnpj = normalize_cnpj(record.get("cnpj"))
        if not cnpj:
            continue
        cached = casa_cache.get(cnpj)
        payload: Dict[str, Any] = {}
        if isinstance(cached, dict):
            payload = dict(cached)
        else:
            for attempt in range(2):
                try:
                    result = casa.fetch_by_cnpj(cnpj)
                    payload = {
                        "trade_name": str(result.trade_name or "").strip(),
                        "company_name": str(result.company_name or "").strip(),
                        "phones": [normalize_phone(item) for item in list(result.phones or []) if phone_is_valid(normalize_phone(item))],
                        "emails": [normalize_email(item) for item in list(result.emails or []) if normalize_email(item)],
                        "websites": [str(item or "").strip() for item in list(result.websites or []) if str(item or "").strip()],
                        "decision_makers": [str(item or "").strip() for item in list(result.decision_makers or []) if str(item or "").strip()],
                    }
                    casa_cache[cnpj] = payload
                    save_cache(CACHE_JSON, cache)
                    log(f"[CASA_DADOS_OK] deal={record['deal_id']} cnpj={cnpj}")
                    time.sleep(CACHE_DELAY_SEC)
                    break
                except CasaDosDadosError:
                    if attempt == 1:
                        payload = {}
                    else:
                        time.sleep(1.0)
                except Exception:
                    if attempt == 1:
                        payload = {}
                    else:
                        time.sleep(1.0)
        phones = [phone for phone in list(payload.get("phones") or []) if phone_is_valid(phone)]
        repeated = Counter(phones)
        best_enriched_phone = choose_best_phone(phones, repeated)
        if (not record.get("telefone_valido")) and phone_is_valid(best_enriched_phone):
            record["telefone"] = best_enriched_phone
            record["telefone_valido"] = True
            log(f"[TELEFONE_VALIDO] deal={record['deal_id']} telefone={best_enriched_phone}")
        if not normalize_email(record.get("email")):
            emails = [email for email in list(payload.get("emails") or []) if email]
            if emails:
                record["email"] = emails[0]
        trade_name = str(payload.get("trade_name") or "").strip()
        company_name = str(payload.get("company_name") or "").strip()
        if trade_name:
            record["nome_final"] = normalize_name(trade_name)
        elif company_name and not record.get("nome_final"):
            record["nome_final"] = normalize_name(company_name)
        websites = [str(item or "").strip() for item in list(payload.get("websites") or []) if str(item or "").strip()]
        if websites:
            record["site"] = websites[0]
        decision_makers = [str(item or "").strip() for item in list(payload.get("decision_makers") or []) if str(item or "").strip()]
        if decision_makers:
            record["socios"] = " | ".join(decision_makers[:5])
        record["nome_generico"] = is_generic_name(record.get("nome_final") or record.get("nome_empresa") or "")
        record["score"] = score_record(record)


def excel_column_name(index: int) -> str:
    output = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        output = chr(65 + remainder) + output
    return output


def xml_escape(value: Any) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_xlsx(rows: List[Dict[str, Any]], path: Path) -> None:
    headers = ["deal_id", "org_id", "nome_final", "cnpj", "telefone", "email", "score", "acao"]
    data = [headers]
    for row in rows:
        data.append(
            [
                int(row.get("deal_id") or 0),
                int(row.get("org_id") or 0),
                str(row.get("nome_final") or "").strip(),
                str(row.get("cnpj") or "").strip(),
                str(row.get("telefone") or "").strip(),
                str(row.get("email") or "").strip(),
                int(row.get("score") or 0),
                str(row.get("acao") or "").strip(),
            ]
        )

    sheet_rows: List[str] = []
    for row_idx, row in enumerate(data, start=1):
        cells: List[str] = []
        for col_idx, value in enumerate(row, start=1):
            cell_ref = f"{excel_column_name(col_idx)}{row_idx}"
            if isinstance(value, int):
                cells.append(f'<c r="{cell_ref}"><v>{value}</v></c>')
            else:
                cells.append(f'<c r="{cell_ref}" t="inlineStr"><is><t>{xml_escape(value)}</t></is></c>')
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
  <sheets>
    <sheet name="deals_enriquecido" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"""
    worksheet = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>{''.join(sheet_rows)}</sheetData>
</worksheet>"""
    core = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>
</cp:coreProperties>"""
    app = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Codex</Application>
</Properties>"""

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
    records = build_records(rows)
    discover_missing_cnpjs(records)
    define_masters(records)
    selected_masters, duplicates = prepare_records(records)
    enrich_with_casa(selected_masters)
    selected_masters.sort(key=lambda item: (-int(item.get("score") or 0), int(item.get("deal_id") or 0)))
    selected_masters = selected_masters[:MAX_MASTERS]
    final_rows = list(selected_masters) + sorted(
        duplicates,
        key=lambda item: (int(item.get("master_deal_id") or 0), int(item.get("deal_id") or 0)),
    )
    write_xlsx(final_rows, OUTPUT_XLSX)
    resgatados = sum(1 for item in final_rows if str(item.get("acao") or "") == "RESGATAR")
    sem_cnpj = sum(1 for item in selected_masters if not normalize_cnpj(item.get("cnpj")))
    log(f"[EXPORTADO] arquivo={OUTPUT_XLSX} linhas={len(final_rows)} masters={len(selected_masters)} duplicados={len(duplicates)}")
    log(f"[RESUMO_FINAL] deals_finais={len(selected_masters)} duplicados={len(duplicates)} resgatados={resgatados} masters_sem_cnpj={sem_cnpj}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
