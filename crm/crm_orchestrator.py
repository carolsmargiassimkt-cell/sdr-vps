from __future__ import annotations

import logging
import re
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html import unescape
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Tuple
from urllib.parse import quote_plus, unquote, urljoin, urlparse

from config.config_loader import get_config_value
from crm.activity_registry import ActivityRegistry
from crm.metrics_today import MetricsToday
from crm.notifications_center import NotificationsCenter
from core.db_manager import get_db_manager
from runtime.process_lock import ProcessAlreadyRunningError, ProcessLock
from services.casa_dos_dados_client import CasaDosDadosClient, CasaDosDadosError
from services.whatsapp_service import WhatsAppService

try:
    from logic.priority_queue_manager import PriorityQueueManager
except Exception:
    class PriorityQueueManager:  # type: ignore[override]
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def sync_from_events(self, _events):
            return []

        def list_items(self, limit: int = 100):
            return []

        def obter_proximo_da_fila(self, remove: bool = False):
            return {}


STAGE_FALLBACK_ORDER = [
    "Lead novo",
    "Contato iniciado",
    "Conversando",
    "Reunião marcada",
    "Proposta",
    "Ganho",
    "Perdido",
]

MODO_ENRIQUECIMENTO_TELEFONE = True
PHONE_TIMEOUT_SEC = 3
TURBO_MODE = True
PHONE_CACHE_KEY = "phone_enrichment_cache"
PHONE_BLACKLIST_KEY = "phone_blacklist"
PHONE_RESULTS_KEY = "phone_results_by_day"
PHONE_SOURCE_SCORES = {
    "google": 0,
    "google_maps": 2,
    "site_oficial": 2,
    "instagram": 1,
    "casa_dos_dados": 1,
}
PHONE_MIN_CONSISTENT_SOURCES = 2
BOT_KEYWORDS = (
    "digite",
    "menu",
    "opção",
    "opcao",
    "atendimento automático",
    "atendimento automatico",
    "central",
    "ura",
)
PHONE_REGEX = re.compile(
    r"(?:\+?55[\s().-]*)?(?:\(?\d{2}\)?[\s().-]*)?(?:9\d{4}|\d{4})[\s.-]*\d{4}"
)


def _build_phone_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) bot-sdr-ai/1.0",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }


def _fetch_text_light(session: requests.Session, url: str, timeout: int = PHONE_TIMEOUT_SEC) -> str:
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
        if int(response.status_code or 0) >= 400:
            return ""
        return response.text or ""
    except Exception:
        return ""


def _normalizar_telefone_br(numero: Any, whatsapp_service: WhatsAppService | None = None) -> str:
    service = whatsapp_service or WhatsAppService()
    digits = service.normalize_phone(numero)
    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) not in {10, 11}:
        return ""
    if len(set(digits)) == 1:
        return ""
    return digits


def _is_sequential_block(text: str) -> bool:
    value = str(text or "").strip()
    if len(value) < 6:
        return False
    if len(set(value)) == 1:
        return True
    ascending = "01234567890"
    descending = ascending[::-1]
    return value in ascending or value in descending


def _is_real_phone_candidate(numero: Any, known_cnpj: str = "", whatsapp_service: WhatsAppService | None = None) -> bool:
    digits = _normalizar_telefone_br(numero, whatsapp_service)
    if len(digits) < 10:
        return False
    if digits.startswith("00"):
        return False

    local_number = digits[-8:]
    if _is_sequential_block(local_number):
        return False
    if _is_sequential_block(digits[-6:]):
        return False

    clean_cnpj = re.sub(r"\D+", "", str(known_cnpj or ""))
    if len(clean_cnpj) == 14:
        if digits in clean_cnpj or local_number in clean_cnpj or clean_cnpj.endswith(local_number):
            return False
    return True


def normalizar_telefones_br(numeros: List[Any], whatsapp_service: WhatsAppService | None = None) -> List[str]:
    service = whatsapp_service or WhatsAppService()
    normalized: List[str] = []
    seen = set()
    for item in numeros or []:
        phone = _normalizar_telefone_br(item, service)
        if not phone or phone in seen:
            continue
        seen.add(phone)
        normalized.append(phone)
    return normalized


def detectar_bot(resposta: Any) -> bool:
    normalized = str(resposta or "").strip().lower()
    if not normalized:
        return False
    return any(token in normalized for token in BOT_KEYWORDS)


def validar_whatsapp(numero: Any, whatsapp_service: WhatsAppService | None = None) -> bool:
    service = whatsapp_service or WhatsAppService()
    if not service.is_valid_phone(numero):
        return False
    return bool(service.validate_whatsapp(numero))


def _is_mobile_with_nine(numero: Any, whatsapp_service: WhatsAppService | None = None) -> bool:
    normalized = _normalizar_telefone_br(numero, whatsapp_service)
    return len(normalized) == 11 and normalized[2] == "9"


def _source_log_name(source: str) -> str:
    mapping = {
        "google": "GOOGLE",
        "google_maps": "MAPS",
        "site_oficial": "SITE",
        "instagram": "INSTAGRAM",
        "casa_dos_dados": "CASA",
    }
    return mapping.get(str(source or "").strip().lower(), str(source or "").strip().upper())


def _extrair_urls_google(html: str) -> List[str]:
    urls: List[str] = []
    seen = set()
    for match in re.finditer(r'href="/url\?q=([^"&]+)', str(html or "")):
        raw_url = unquote(match.group(1))
        parsed = urlparse(raw_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        netloc = parsed.netloc.lower()
        if "google." in netloc:
            continue
        clean = raw_url.split("&sa=")[0].strip()
        if clean and clean not in seen:
            seen.add(clean)
            urls.append(clean)
    return urls


def _extrair_telefones_de_texto(texto: str) -> List[Tuple[str, str]]:
    output: List[Tuple[str, str]] = []
    seen = set()
    base = unescape(str(texto or ""))
    for match in PHONE_REGEX.finditer(base):
        raw = match.group(0)
        start = max(0, match.start() - 80)
        end = min(len(base), match.end() + 80)
        context = re.sub(r"\s+", " ", base[start:end]).strip()
        key = (raw, context)
        if key in seen:
            continue
        seen.add(key)
        output.append(key)
    return output


def _is_official_site(url: str) -> bool:
    netloc = urlparse(str(url or "")).netloc.lower()
    if not netloc:
        return False
    blocked = (
        "google.",
        "instagram.com",
        "facebook.com",
        "linkedin.com",
        "youtube.com",
        "casadosdados.com.br",
    )
    return not any(token in netloc for token in blocked)


def _buscar_link_instagram(session: requests.Session, nome_empresa: str) -> str:
    query = f"{str(nome_empresa or '').strip()} instagram".strip()
    html = _fetch_text_light(
        session,
        f"https://www.google.com/search?q={quote_plus(query)}&hl=pt-BR&num=5",
    )
    for url in _extrair_urls_google(html):
        if "instagram.com" in urlparse(url).netloc.lower():
            return url
    return ""


def _descobrir_link_contato(html: str, base_url: str) -> str:
    for match in re.finditer(r'href=["\']([^"\']+)["\']', str(html or ""), flags=re.IGNORECASE):
        href = str(match.group(1) or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        lowered = href.lower()
        if "contato" not in lowered and "fale-conosco" not in lowered and "contact" not in lowered:
            continue
        return urljoin(base_url, href)
    return ""


def _coletar_telefones_multifonte_detalhado(
    nome_empresa: Any,
    *,
    casa_client: CasaDosDadosClient | None = None,
    known_cnpj: str = "",
) -> List[Dict[str, Any]]:
    company = str(nome_empresa or "").strip()
    if not company:
        return []

    session = requests.Session()
    session.headers.update(_build_phone_headers())
    whatsapp_service = WhatsAppService()
    candidates: Dict[str, Dict[str, Any]] = {}

    def add_candidate(raw_phone: Any, source: str, context: str = "", url: str = "") -> None:
        normalized = _normalizar_telefone_br(raw_phone, whatsapp_service)
        if not normalized:
            return
        entry = candidates.setdefault(
            normalized,
            {
                "phone": normalized,
                "sources": set(),
                "contexts": [],
                "urls": [],
            },
        )
        entry["sources"].add(source)
        if context:
            entry["contexts"].append(context[:240])
        if url:
            entry["urls"].append(url)

    google_html = _fetch_text_light(
        session,
        f"https://www.google.com/search?q={quote_plus(f'{company} telefone')}&hl=pt-BR&num=5",
    )
    for raw_phone, context in _extrair_telefones_de_texto(google_html):
        add_candidate(raw_phone, "google", context=context)
    if candidates:
        detailed: List[Dict[str, Any]] = []
        for phone, payload in candidates.items():
            detailed.append(
                {
                    "phone": phone,
                    "sources": sorted(payload["sources"]),
                    "contexts": list(payload["contexts"])[:5],
                    "urls": list(dict.fromkeys(payload["urls"])),
                }
            )
        return detailed

    google_urls = _extrair_urls_google(google_html)
    official_url = next((url for url in google_urls if _is_official_site(url)), "")
    instagram_url = next((url for url in google_urls if "instagram.com" in urlparse(url).netloc.lower()), "")

    maps_html = _fetch_text_light(
        session,
        f"https://www.google.com/search?q={quote_plus(company)}&tbm=lcl&hl=pt-BR&num=5",
    )
    for raw_phone, context in _extrair_telefones_de_texto(maps_html):
        add_candidate(raw_phone, "google_maps", context=context)
    if candidates:
        detailed: List[Dict[str, Any]] = []
        for phone, payload in candidates.items():
            detailed.append(
                {
                    "phone": phone,
                    "sources": sorted(payload["sources"]),
                    "contexts": list(payload["contexts"])[:5],
                    "urls": list(dict.fromkeys(payload["urls"])),
                }
            )
        return detailed

    clean_cnpj = re.sub(r"\D+", "", str(known_cnpj or ""))
    if casa_client is not None:
        try:
            cnpj = clean_cnpj or casa_client.discover_cnpj_by_google(company) or casa_client.discover_cnpj_by_casa_search(company)
            if cnpj:
                enrichment = casa_client.enrich_company(cnpj)
                for raw_phone in list(getattr(enrichment, "phones", []) or []):
                    add_candidate(raw_phone, "casa_dos_dados")
        except Exception:
            pass

    detailed: List[Dict[str, Any]] = []
    for phone, payload in candidates.items():
        detailed.append(
            {
                "phone": phone,
                "sources": sorted(payload["sources"]),
                "contexts": list(payload["contexts"])[:5],
                "urls": list(dict.fromkeys(payload["urls"])),
            }
        )
    return detailed


def _coletar_telefones_high_quality_detalhado(
    nome_empresa: Any,
    *,
    casa_client: CasaDosDadosClient | None = None,
    known_cnpj: str = "",
) -> List[Dict[str, Any]]:
    company = str(nome_empresa or "").strip()
    if not company:
        return []

    session = requests.Session()
    session.headers.update(_build_phone_headers())
    whatsapp_service = WhatsAppService()
    candidates: Dict[str, Dict[str, Any]] = {}
    official_url = ""
    display_name = company
    clean_cnpj = re.sub(r"\D+", "", str(known_cnpj or ""))

    def add_candidate(raw_phone: Any, source: str, context: str = "", url: str = "") -> None:
        normalized = _normalizar_telefone_br(raw_phone, whatsapp_service)
        if not normalized or not _is_real_phone_candidate(
            normalized,
            known_cnpj=clean_cnpj,
            whatsapp_service=whatsapp_service,
        ):
            return
        entry = candidates.setdefault(
            normalized,
            {
                "phone": normalized,
                "sources": set(),
                "contexts": [],
                "urls": [],
            },
        )
        entry["sources"].add(source)
        if context:
            entry["contexts"].append(context[:240])
        if url:
            entry["urls"].append(url)

    def _build_output() -> List[Dict[str, Any]]:
        return [
            {
                "phone": phone,
                "sources": sorted(payload["sources"]),
                "contexts": list(payload["contexts"])[:5],
                "urls": list(dict.fromkeys(payload["urls"])),
            }
            for phone, payload in candidates.items()
        ]

    if clean_cnpj and casa_client is not None:
        try:
            enrichment = casa_client.enrich_company(clean_cnpj)
            display_name = extrair_nome_fantasia(company, enrichment, company) or company
            for raw_phone in list(getattr(enrichment, "phones", []) or []):
                add_candidate(raw_phone, "casa_dos_dados", context=f"CNPJ {clean_cnpj}")
            websites = list(getattr(enrichment, "websites", []) or [])
            official_url = next((url for url in websites if _is_official_site(url)), "")
        except Exception:
            pass
    if candidates:
        return _build_output()

    maps_query = f"{display_name} google maps".strip()
    maps_html = _fetch_text_light(
        session,
        f"https://www.google.com/search?q={quote_plus(maps_query)}&hl=pt-BR&num=5",
    )
    for raw_phone, context in _extrair_telefones_de_texto(maps_html):
        add_candidate(raw_phone, "google_maps", context=context)
    maps_urls = _extrair_urls_google(maps_html)
    if not official_url:
        official_url = next((url for url in maps_urls if _is_official_site(url)), "")
    if candidates:
        return _build_output()

    if not official_url:
        site_html = _fetch_text_light(
            session,
            f"https://www.google.com/search?q={quote_plus(display_name)}&hl=pt-BR&num=5",
        )
        official_url = next((url for url in _extrair_urls_google(site_html) if _is_official_site(url)), "")
    if not official_url:
        return []

    home_html = _fetch_text_light(session, official_url)
    if home_html:
        for raw_phone, context in _extrair_telefones_de_texto(home_html):
            add_candidate(raw_phone, "site_oficial", context=context, url=official_url)
    contact_url = _descobrir_link_contato(home_html, official_url) if home_html else ""
    if contact_url:
        contact_html = _fetch_text_light(session, contact_url)
        for raw_phone, context in _extrair_telefones_de_texto(contact_html):
            add_candidate(raw_phone, "site_oficial", context=context, url=contact_url)

    return _build_output()


def coletar_telefones_multifonte(nome_empresa, known_cnpj: str = ""):
    casa_client = None
    try:
        casa_client = CasaDosDadosClient(timeout=8.0)
    except Exception:
        casa_client = None
    detailed = _coletar_telefones_multifonte_detalhado(nome_empresa, casa_client=casa_client, known_cnpj=known_cnpj)
    return [item["phone"] for item in detailed]


def _resolve_openrouter_api_key() -> str:
    configured = str(get_config_value("openrouter_api_key", "") or "").strip()
    if configured:
        return configured
    try:
        from logic.whatsapp_pitch_engine import WhatsAppPitchEngine

        return str(getattr(WhatsAppPitchEngine, "OPENROUTER_TOKEN", "") or "").strip()
    except Exception:
        return ""


def _normalize_company_name_for_cnpj(nome: Any) -> str:
    raw = str(nome or "").strip().lower()
    raw = re.sub(
        r"\b(ltda|ltda\.|me|eireli|epp|s\/a|sa|mei|comercio|comércio|comÃ©rcio|servicos|serviços|serviÃ§os|industria|indústria|indÃºstria|holding|grupo)\b",
        " ",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(r"[^0-9a-zà-ÿÀ-Ÿ&.\- ]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" .-")
    return raw


def _clean_company_display_name(nome: Any) -> str:
    raw = str(nome or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"\b(ltda|ltda\.|me|eireli|epp|s\/a|sa|mei)\b\.?", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(
        r"\b(comercio|comÃ©rcio|comÃƒÂ©rcio|servicos|serviÃ§os|serviÃƒÂ§os|industria|indÃºstria|indÃƒÂºstria|holding|grupo)\b",
        " ",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(r"\s+", " ", raw).strip(" -|,:;.")
    return raw


def _extract_brand_from_website(url: Any) -> str:
    raw_url = str(url or "").strip()
    if not raw_url:
        return ""
    try:
        parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url}")
    except Exception:
        return ""
    host = str(parsed.netloc or "").lower()
    host = re.sub(r"^www\d*\.", "", host)
    labels = [
        part
        for part in host.split(".")
        if part and part not in {"com", "combr", "br", "net", "org", "co", "empresarial"}
    ]
    if not labels:
        return ""
    candidate = labels[0]
    if candidate in {"site", "app", "web", "portal"} and len(labels) > 1:
        candidate = labels[1]
    candidate = re.sub(r"[^a-z0-9]+", " ", candidate, flags=re.IGNORECASE).strip()
    return _clean_company_display_name(candidate.title())


def extrair_nome_fantasia(org: Any, casa_dados: Any = None, google: Any = "") -> str:
    original = _clean_company_display_name(org)
    candidates: List[str] = []
    if casa_dados is not None:
        candidates.extend(
            [
                str(getattr(casa_dados, "trade_name", "") or "").strip(),
                str(getattr(casa_dados, "company_name", "") or "").strip(),
            ]
        )
        for website in list(getattr(casa_dados, "websites", []) or [])[:3]:
            brand = _extract_brand_from_website(website)
            if brand:
                candidates.append(brand)
    google_hint = _clean_company_display_name(google)
    if google_hint:
        candidates.append(google_hint)
    if original:
        candidates.append(original)

    generic_terms = {
        "empresa",
        "negocio",
        "negÃ³cio",
        "cliente",
        "contato",
        "supermercado",
        "supermercados",
        "mercado",
        "mercados",
        "comercio",
        "comÃ©rcio",
        "servicos",
        "serviÃ§os",
    }
    seen = set()
    for candidate in candidates:
        cleaned = _clean_company_display_name(candidate)
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        if key in generic_terms:
            continue
        if len(cleaned) <= 2:
            continue
        return cleaned
    return original or str(org or "").strip()


def enrich_name_with_llm(nome):
    raw_name = str(nome or "").strip()
    normalized = _normalize_company_name_for_cnpj(raw_name)
    fallback_name = normalized or raw_name
    fallback_queries = [
        f"{fallback_name} CNPJ".strip(),
        f"{fallback_name} razao social".strip(),
    ]

    api_key = _resolve_openrouter_api_key()
    if not raw_name or not api_key:
        return {
            "normalized": fallback_name,
            "trade_name": _clean_company_display_name(fallback_name),
            "company_name": raw_name,
            "search_queries": fallback_queries,
            "used": False,
            "error": "missing_api_key" if raw_name else "empty_name",
        }

    payload = {
        "model": str(get_config_value("openrouter_model_economy", "openai/gpt-4o-mini") or "openai/gpt-4o-mini"),
        "temperature": 0.2,
        "max_tokens": 180,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Normalize nome empresarial brasileiro para busca de CNPJ. "
                    "Remova sufixos societários e responda somente JSON válido com as chaves "
                    "normalized, trade_name, company_name e search_queries. "
                    "trade_name deve ser o nome fantasia provÃ¡vel, company_name a razÃ£o social provÃ¡vel "
                    "e search_queries deve ter exatamente 2 itens curtos."
                ),
            },
            {
                "role": "user",
                "content": raw_name,
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=3,
    )
    response.raise_for_status()
    body = response.json()
    choices = body.get("choices") or []
    content = (((choices[0] or {}).get("message") or {}).get("content") or "").strip() if choices else ""
    if not content:
        raise ValueError("empty_openrouter_response")

    import json

    start = content.find("{")
    end = content.rfind("}")
    parsed = json.loads(content[start:end + 1] if start >= 0 and end > start else content)
    normalized_result = str(parsed.get("normalized") or fallback_name).strip() or fallback_name
    trade_name = _clean_company_display_name(parsed.get("trade_name") or normalized_result)
    company_name = str(parsed.get("company_name") or raw_name).strip()
    raw_queries = parsed.get("search_queries") or []
    search_queries = []
    if isinstance(raw_queries, list):
        for item in raw_queries:
            query = str(item or "").strip()
            if query and query not in search_queries:
                search_queries.append(query)
    if not search_queries:
        search_queries = fallback_queries
    while len(search_queries) < 2:
        search_queries.append(fallback_queries[len(search_queries)])
    return {
        "normalized": normalized_result,
        "trade_name": trade_name,
        "company_name": company_name,
        "search_queries": search_queries[:2],
        "used": True,
        "error": "",
    }


class CRMOrchestrator:
    def __init__(self, pipedrive_client, logger, base_dir: Path) -> None:
        self.crm = pipedrive_client
        self.logger = logger
        logs_dir = base_dir / "logs"
        self.metrics = MetricsToday(logs_dir / "metrics_today.json", logger)
        self.notifications = NotificationsCenter(
            queue_file=logs_dir / "notifications_queue.json",
            call_tasks_file=logs_dir / "call_tasks_queue.json",
            logger=logger,
        )
        self.priority_queue = PriorityQueueManager(logs_dir / "priority_queue.json", logger)
        self.activities = ActivityRegistry(
            timeline_file=logs_dir / "lead_timeline.json",
            pipedrive_client=pipedrive_client,
            logger=logger,
        )

    def registrar_atividade(
        self,
        lead_id: str,
        descricao: str,
        person_id: int = 0,
        deal_id: int = 0,
    ) -> Dict[str, Any]:
        return self.activities.registrar_atividade(
            lead_id=lead_id,
            descricao=descricao,
            person_id=person_id,
            deal_id=deal_id,
        )

    def push_event(self, event_type: str, lead: Dict[str, Any], descricao: str) -> Dict[str, Any]:
        return self.notifications.enqueue_event(event_type=event_type, lead=lead, descricao=descricao)

    def get_notifications(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.notifications.list_events(limit=limit)

    def sync_priority_queue(self) -> List[Dict[str, Any]]:
        events = self.notifications.list_events(limit=0)
        return self.priority_queue.sync_from_events(events)

    def get_priority_queue(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.priority_queue.list_items(limit=limit)

    def obter_proximo_da_fila(self, remove: bool = False) -> Dict[str, Any]:
        self.sync_priority_queue()
        return self.priority_queue.obter_proximo_da_fila(remove=remove)

    def processar_prioridade_antes_do_fluxo_normal(self, processar_item_cb, fluxo_normal_cb=None) -> Any:
        prioridade = self.obter_proximo_da_fila(remove=True)
        if prioridade:
            return processar_item_cb(prioridade)
        if callable(fluxo_normal_cb):
            return fluxo_normal_cb()
        return None

    def get_metrics_today(self) -> Dict[str, int]:
        return self.metrics.get_metrics()

    def increment_metric(self, key: str, amount: int = 1) -> Dict[str, int]:
        return self.metrics.increment(key, amount)

    def get_timeline(self, lead_id: str) -> List[Dict[str, Any]]:
        return self.activities.get_timeline(lead_id)

    def fetch_pipeline(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        stages = self.crm.get_stages()
        deals = self.crm.get_deals(status="all_not_deleted")
        normalized_stages = self._normalize_stages(stages)
        normalized_deals = [self._normalize_deal(deal) for deal in deals]
        return normalized_stages, normalized_deals

    def move_deal(self, deal_id: int, stage_id: int) -> bool:
        return False

    @staticmethod
    def _normalize_stages(stages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not stages:
            return [{"id": idx + 1, "name": name} for idx, name in enumerate(STAGE_FALLBACK_ORDER)]
        normalized = []
        for stage in stages:
            normalized.append(
                {
                    "id": int(stage.get("id", 0) or 0),
                    "name": str(stage.get("name", "")).strip() or f"Stage {stage.get('id', '')}",
                }
            )
        return normalized

    @staticmethod
    def _normalize_deal(deal: Dict[str, Any]) -> Dict[str, Any]:
        person = deal.get("person_id") or {}
        org = deal.get("org_id") or {}
        company = ""
        if isinstance(org, dict):
            company = org.get("name", "")
        elif isinstance(org, str):
            company = org
        contact = ""
        phone = ""
        if isinstance(person, dict):
            contact = person.get("name", "")
            phones = person.get("phone") or []
            if phones and isinstance(phones, list):
                value = phones[0]
                if isinstance(value, dict):
                    phone = value.get("value", "")
                else:
                    phone = str(value)
        elif isinstance(person, str):
            contact = person
        return {
            "id": int(deal.get("id", 0) or 0),
            "title": str(deal.get("title", "")),
            "stage_id": int(deal.get("stage_id", 0) or 0),
            "empresa": company or str(deal.get("title", "")),
            "contato": contact or "Sem contato",
            "telefone": phone or "Sem telefone",
            "score": int(deal.get("probability", 0) or 0),
            "ultimo_contato": str(deal.get("update_time", "") or ""),
            "person_id": int((person.get("value", 0) if isinstance(person, dict) else 0) or 0),
        }


class CRMEnrichmentLoop:
    PIPELINE_ID = 2
    FETCH_LIMIT = 150
    MAX_EMPRESAS = 150
    MAX_TENTATIVAS = 3
    TURBO_WORKERS = 4
    PIPEDRIVE_DELAY_SEC = 0.5 if TURBO_MODE else 3.0
    EXTERNAL_DELAY_SEC = 0.5 if TURBO_MODE else 4.0
    LOOP_SLEEP_SEC = 5.0 if TURBO_MODE else 15.0
    STATE_KEY = "crm_enrichment_loop"
    CNPJ_CACHE_KEY = "cnpj_cache"
    LLM_CACHE_KEY = "llm_name_cache"
    PHONE_CACHE_KEY = PHONE_CACHE_KEY
    PHONE_BLACKLIST_KEY = PHONE_BLACKLIST_KEY
    PHONE_RESULTS_KEY = PHONE_RESULTS_KEY

    def __init__(self, pipedrive_client, logger, base_dir: Path) -> None:
        self.crm = pipedrive_client
        self.logger = logger or logging.getLogger("crm_enrichment_loop")
        self.base_dir = Path(base_dir)
        self.db = get_db_manager()
        try:
            self.casa = CasaDosDadosClient(timeout=3.0 if TURBO_MODE else 8.0)
        except Exception as exc:
            self.logger.warning(f"[CASA_DESABILITADA] {exc}")
            self.casa = None
        self.whatsapp = WhatsAppService()
        self._last_pipedrive_call = 0.0
        self._last_external_call = 0.0
        self._cycle_org_cache: Dict[int, Dict[str, Any]] = {}
        self._cycle_processed_orgs: set[int] = set()
        self._cycle_phone_blacklist: set[str] = set()
        self._consecutive_429 = 0
        self._pipedrive_lock = Lock()
        self._cycle_lock = Lock()
        self._state_lock = Lock()
        self._run_processed_orgs: set[int] = set()
        self._cycle_duplicate_keys: set[str] = set()
        self._archived_stage_id: int | None = None

    def run_forever(self) -> None:
        lock = ProcessLock("crm_enrichment_loop")
        try:
            lock.acquire()
        except ProcessAlreadyRunningError:
            self.logger.warning("[SKIP_DUPLICADO] loop de enriquecimento já em execução")
            return
        try:
            counters = self.run_cycle()
            print(f"[FINALIZADO_TOTAL] empresas_processadas={int(counters.get('processed') or 0)}")
            print(
                f"[ENRICHMENT_DONE] processadas={int(counters.get('processed') or 0)} "
                f"cnpj_encontrado={int(counters.get('cnpj_encontrado') or 0)}"
            )
        finally:
            lock.release()

    def run_cycle(self) -> Dict[str, int]:
        self._cycle_org_cache = {}
        self._cycle_processed_orgs = set()
        self._run_processed_orgs = set()
        self._cycle_duplicate_keys = set()
        self._cycle_phone_blacklist = set(self._load_phone_blacklist())
        self._consecutive_429 = 0
        if TURBO_MODE:
            print(f"[TURBO_MODE] workers={self.TURBO_WORKERS} fetch_limit={self.FETCH_LIMIT}")
        self._testar_conexao_pipedrive()
        if MODO_ENRIQUECIMENTO_TELEFONE:
            return self._run_phone_enrichment_cycle()
        counters = {
            "processed": 0,
            "enriched": 0,
            "ignored": 0,
            "people_created": 0,
            "cnpj_encontrado": 0,
        }
        deals = self._prepare_master_deals(self._fetch_target_deals())

        for deal in deals:
            if self._consecutive_429 >= 2:
                break

            if not self._deal_in_target_pipeline(deal):
                counters["ignored"] += 1
                continue

            deal_id = int(deal.get("id") or 0)
            org = deal.get("org_id") or {}
            org_id = self._extract_id(org)
            org_name = str((org or {}).get("name") if isinstance(org, dict) else deal.get("org_name") or "").strip()
            if not org_id or not org_name:
                counters["ignored"] += 1
                continue

            print(f"[DEAL_ANALISADO] deal={deal_id} org={org_id} nome={org_name}")
            print(f"[PROCESSANDO_MASTER] deal={deal_id} org={org_id}")
            counters["processed"] += 1

            if org_id in self._cycle_processed_orgs or self._already_enriched_today(org_id):
                print(f"[ORG_JA_ENRIQUECIDA] org={org_id}")
                counters["ignored"] += 1
                continue

            organization = self._get_org_cached(org_id)
            if not organization:
                counters["ignored"] += 1
                continue

            existing_cnpj = self.crm.extract_cnpj(organization)
            org_people = self._safe_pipedrive_call(lambda: self.crm.get_organization_persons(org_id, limit=100))
            if org_people is None:
                counters["ignored"] += 1
                continue

            enrichment = None
            if existing_cnpj:
                print(f"[ORG_JA_ENRIQUECIDA] org={org_id}")
                self._mark_enriched_today(org_id, existing_cnpj)
                self._cycle_processed_orgs.add(org_id)
                counters["ignored"] += 1
                continue
            else:
                discovered_cnpj = self._discover_cnpj(org_name)
                if not discovered_cnpj:
                    print(f"[CNPJ_NAO_ENCONTRADO] org={org_id} nome={org_name}")
                    self._mark_enriched_today(org_id, "")
                    self._cycle_processed_orgs.add(org_id)
                    counters["ignored"] += 1
                    continue
                print(f"[CNPJ_ENCONTRADO] org={org_id} cnpj={discovered_cnpj}")
                enrichment = self._enrich_by_cnpj(discovered_cnpj)
                if not enrichment:
                    print(f"[CNPJ_NAO_ENCONTRADO] org={org_id} nome={org_name}")
                    counters["ignored"] += 1
                    continue

                updated = self._update_organization_if_needed(organization, enrichment)
                if updated:
                    print(f"[ORG_ATUALIZADA] org={org_id}")
                    counters["enriched"] += 1
                self._mark_enriched_today(org_id, enrichment.cnpj)

            person_action = self._ensure_person_for_deal(deal, org_id, org_people or [], enrichment)
            if person_action == "created":
                counters["people_created"] += 1
                print(f"[PESSOA_CRIADA] org={org_id} deal={deal_id}")
            elif person_action == "exists":
                print(f"[PESSOA_JA_EXISTE] org={org_id} deal={deal_id}")

            self._cycle_processed_orgs.add(org_id)

        print(
            f"[ENRICHMENT_SUMMARY] processed={counters['processed']} "
            f"enriched={counters['enriched']} ignored={counters['ignored']} "
            f"people_created={counters['people_created']}"
        )
        return counters

    def _run_phone_enrichment_cycle(self) -> Dict[str, int]:
        counters = {
            "processed": 0,
            "enriched": 0,
            "ignored": 0,
            "people_created": 0,
            "cnpj_encontrado": 0,
        }
        deals = self._prepare_master_deals(self._fetch_target_deals())
        if TURBO_MODE:
            with ThreadPoolExecutor(max_workers=self.TURBO_WORKERS) as executor:
                futures = [executor.submit(self._process_phone_deal, deal) for deal in deals]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception as exc:
                        self.logger.warning(f"[PIPEDRIVE_SKIP_CICLO] erro_worker={exc}")
                        counters["ignored"] += 1
                        continue
                    for key in counters:
                        counters[key] += int(result.get(key) or 0)
        else:
            for deal in deals:
                result = self._process_phone_deal(deal)
                for key in counters:
                    counters[key] += int(result.get(key) or 0)

        print(
            f"[ENRICHMENT_SUMMARY] processed={counters['processed']} "
            f"enriched={counters['enriched']} ignored={counters['ignored']} "
            f"people_created={counters['people_created']}"
        )
        return counters

    def _process_phone_deal(self, deal: Dict[str, Any]) -> Dict[str, int]:
        started_at = time.perf_counter()
        counters = {"processed": 0, "enriched": 0, "ignored": 0, "people_created": 0}
        deal_id = int((deal or {}).get("id") or 0)
        org = (deal or {}).get("org_id") or {}
        org_id = self._extract_id(org)
        org_name = str((org or {}).get("name") if isinstance(org, dict) else (deal or {}).get("org_name") or "").strip()
        try:
            if self._consecutive_429 >= 2:
                counters["ignored"] += 1
                return counters
            if str((deal or {}).get("status") or "open").strip().lower() != "open":
                print(f"[SKIP_ARQUIVADO] deal={deal_id}")
                counters["ignored"] += 1
                return counters
            if self._is_archived_deal(deal):
                print(f"[SKIP_ARQUIVADO] deal={deal_id} stage=arquivados")
                counters["ignored"] += 1
                return counters
            if not self._deal_in_target_pipeline(deal):
                counters["ignored"] += 1
                return counters
            if not org_id or not org_name:
                counters["ignored"] += 1
                return counters
            if not self._reserve_cycle_org(org_id):
                counters["ignored"] += 1
                return counters

            print(f"[DEAL_ANALISADO] deal={deal_id} org={org_id} nome={org_name}")
            print(f"[PROCESSANDO_MASTER] deal={deal_id} org={org_id}")
            counters["processed"] += 1

            if self._already_phone_enriched_today(org_id):
                print(f"[ORG_JA_ENRIQUECIDA] org={org_id}")
                counters["ignored"] += 1
                return counters

            organization = self._get_org_cached(org_id)
            if not organization:
                counters["ignored"] += 1
                return counters

            org_people = self._safe_pipedrive_call(lambda: self.crm.get_organization_persons(org_id, limit=100))
            if org_people is None:
                counters["ignored"] += 1
                return counters

            enrichment = None
            current_cnpj = self.crm.extract_cnpj(organization)
            if current_cnpj:
                print(f"[ORG_JA_ENRIQUECIDA] org={org_id}")
                counters["ignored"] += 1
                return counters
            if not current_cnpj:
                print(f"[FOCO_SEM_CNPJ] org={org_id} deal={deal_id}")
                discovered_cnpj = self._discover_cnpj(org_name)
                if discovered_cnpj:
                    current_cnpj = discovered_cnpj
                    counters["cnpj_encontrado"] += 1
                    print(f"[CNPJ_ENCONTRADO] org={org_id} cnpj={discovered_cnpj}")
                    enrichment = self._enrich_by_cnpj(discovered_cnpj)
                    if enrichment:
                        updated = self._update_organization_if_needed(organization, enrichment)
                        if updated:
                            print(f"[ORG_ATUALIZADA] org={org_id}")
                            counters["enriched"] += 1
                    else:
                        print(f"[CNPJ_NAO_ENCONTRADO] org={org_id} nome={org_name}")

            ranked_phones = self._get_ranked_company_phones(org_name, known_cnpj=current_cnpj)
            if not ranked_phones:
                print(f"[TELEFONE_INVALIDO] org={org_id} motivo=nenhum_telefone_qualificado")
                self._mark_phone_enriched_today(org_id, [])
                counters["ignored"] += 1
                return counters

            chosen_item: Dict[str, Any] = {}
            candidate_phones = list(ranked_phones[:3])
            for option in candidate_phones:
                phone = _normalizar_telefone_br(option.get("phone"), self.whatsapp)
                if not phone:
                    continue
                if phone in self._cycle_phone_blacklist or not self.whatsapp.can_send(phone):
                    continue
                chosen_item = option
                break
            if not chosen_item:
                print(f"[TELEFONE_INVALIDO] org={org_id} motivo=top3_indisponivel")
                self._mark_phone_enriched_today(org_id, [])
                counters["ignored"] += 1
                return counters

            print(
                f"[TELEFONE_ESCOLHIDO] org={org_id} telefone={str(chosen_item.get('phone') or '').strip()} "
                f"score={int(chosen_item.get('score') or 0)}"
            )
            person_action = self._ensure_person_for_deal(
                deal,
                org_id,
                org_people or [],
                enrichment=enrichment,
                ranked_phones=[chosen_item] + [item for item in candidate_phones if item is not chosen_item],
                candidate_name=str((org_people[0] or {}).get("name") or "").strip() if org_people else org_name,
            )
            if person_action == "created":
                counters["people_created"] += 1
                counters["enriched"] += 1
                print(f"[PESSOA_CRIADA] org={org_id} deal={deal_id}")
                print(f"[PESSOA_ORGANIZADA] org={org_id} deal={deal_id} acao=created")
            elif person_action in {"updated", "exists"}:
                counters["enriched"] += 1
                print(f"[PESSOA_JA_EXISTE] org={org_id} deal={deal_id}")
                print(f"[PESSOA_ORGANIZADA] org={org_id} deal={deal_id} acao={person_action}")

            self._store_phone_result(org_id, org_name, ranked_phones)
            self._mark_phone_enriched_today(org_id, ranked_phones)
            return counters
        finally:
            print(f"[TIME_EMPRESA] org={org_id} deal={deal_id} ms={int((time.perf_counter() - started_at) * 1000)}")

    def _fetch_target_deals(self) -> List[Dict[str, Any]]:
        response = self._safe_pipedrive_call(
            lambda: self.crm.get_deals(
                status="open",
                limit=max(self.FETCH_LIMIT * 4, 600),
                pipeline_id=self.PIPELINE_ID,
            )
        )
        if response is None:
            print("[PIPEDRIVE_SKIP_CICLO] motivo=sem_resposta_get_deals")
            return []
        priority_super: List[Dict[str, Any]] = []
        filtered_high: List[Dict[str, Any]] = []
        seen_orgs: set[int] = set()
        for deal in list(response or []):
            if len(priority_super) + len(filtered_high) >= self.MAX_EMPRESAS:
                break
            deal_status = str((deal or {}).get("status") or "open").strip().lower()
            if deal_status != "open":
                print(f"[SKIP_ARQUIVADO] deal={int((deal or {}).get('id') or 0)} status={deal_status}")
                continue
            if not self._deal_in_target_pipeline(deal):
                print(
                    f"[PIPELINE_IGNORADO] deal={int((deal or {}).get('id') or 0)} "
                    f"pipeline={int((deal or {}).get('pipeline_id') or 0)}"
                )
                continue
            if self._is_archived_deal(deal):
                print(f"[SKIP_ARQUIVADO] deal={int((deal or {}).get('id') or 0)} stage=arquivados")
                continue
            org = deal.get("org_id") or {}
            org_id = self._extract_id(org)
            org_name = str((org or {}).get("name") if isinstance(org, dict) else deal.get("org_name") or "").strip()
            if not org_id or not org_name:
                continue
            if org_id in seen_orgs:
                continue
            if org_id in self._cycle_processed_orgs or self._already_phone_enriched_today(org_id) or self._already_enriched_today(org_id):
                print(f"[FETCH_SKIP_ENRIQUECIDA] org={org_id} deal={int((deal or {}).get('id') or 0)}")
                continue
            organization = self._get_org_cached(org_id)
            existing_cnpj = self.crm.extract_cnpj(organization)
            if not existing_cnpj:
                print(f"[FOCO_SEM_CNPJ] org={org_id} deal={int((deal or {}).get('id') or 0)}")
                seen_orgs.add(org_id)
                if self._is_super_minas(deal):
                    print(f"[PRIORIDADE_SUPER_MINAS] org={org_id} deal={int((deal or {}).get('id') or 0)}")
                    priority_super.append(deal)
                else:
                    filtered_high.append(deal)
            if len(priority_super) + len(filtered_high) >= min(self.FETCH_LIMIT, self.MAX_EMPRESAS):
                break
        ordered = priority_super + filtered_high
        return ordered[: min(self.FETCH_LIMIT, self.MAX_EMPRESAS)]

    def _safe_pipedrive_call(self, fn):
        with self._pipedrive_lock:
            self._respect_pipedrive_delay()
            try:
                result = fn()
            except Exception as exc:
                self._last_pipedrive_call = time.time()
                self.logger.warning(f"[PIPEDRIVE_SKIP_CICLO] erro={exc}")
                return None
            self._last_pipedrive_call = time.time()
            status = int(getattr(self.crm, "last_http_status", 0) or 0)
            if status == 429:
                self._consecutive_429 += 1
                time.sleep(10)
                return None
            if result is None:
                self.logger.warning(
                    f"[PIPEDRIVE_SKIP_CICLO] status={status} endpoint={getattr(self.crm, 'last_http_endpoint', '')}"
                )
                return None
            self._consecutive_429 = 0
            return result

    def _testar_conexao_pipedrive(self) -> bool:
        for attempt in range(1, 3):
            connected = bool(self._safe_pipedrive_call(lambda: self.crm.test_connection()))
            if connected:
                return True
            self.logger.warning(f"[PIPEDRIVE_OFFLINE] tentativa={attempt}/2")
            if attempt < 2:
                time.sleep(0.5)
        return False

    def _respect_pipedrive_delay(self) -> None:
        elapsed = time.time() - self._last_pipedrive_call
        if elapsed < self.PIPEDRIVE_DELAY_SEC:
            time.sleep(self.PIPEDRIVE_DELAY_SEC - elapsed)

    def _respect_external_delay(self) -> None:
        elapsed = time.time() - self._last_external_call
        if elapsed < self.EXTERNAL_DELAY_SEC:
            time.sleep(self.EXTERNAL_DELAY_SEC - elapsed)

    def _build_duplicate_groups(self, deals: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for deal in deals:
            key = self._duplicate_group_key(deal)
            if not key:
                continue
            groups.setdefault(key, []).append(deal)
        return groups

    def _duplicate_group_key(self, deal: Dict[str, Any]) -> str:
        org = deal.get("org_id") or {}
        org_id = self._extract_id(org)
        organization = self._get_org_cached(org_id) if org_id else {}
        cnpj = re.sub(r"\D+", "", str(self.crm.extract_cnpj(organization) or ""))
        if cnpj:
            return f"cnpj:{cnpj}"
        org_name = str((org or {}).get("name") if isinstance(org, dict) else deal.get("org_name") or "").strip()
        normalized = self._normalize_org_name(_clean_company_display_name(org_name) or org_name)
        if not normalized:
            return ""
        return f"name:{normalized}"

    def _prepare_master_deals(self, deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups = self._build_duplicate_groups(deals)
        masters: List[Dict[str, Any]] = []
        for _, grouped_deals in groups.items():
            if not grouped_deals:
                continue
            master = self._select_master_deal(grouped_deals)
            if not master:
                continue
            duplicate_group_key = self._duplicate_group_key(master)
            if duplicate_group_key:
                self._cycle_duplicate_keys.add(duplicate_group_key)
            duplicate_deals = [deal for deal in grouped_deals if int((deal or {}).get("id") or 0) != int((master or {}).get("id") or 0)]
            if duplicate_deals:
                print(
                    f"[DEAL_MASTER_DEFINIDO] master={int((master or {}).get('id') or 0)} "
                    f"duplicados={','.join(str(int((deal or {}).get('id') or 0)) for deal in duplicate_deals)}"
                )
                self._consolidate_duplicates_into_master(master, duplicate_deals)
            masters.append(master)
        return masters

    def _select_master_deal(self, deals: List[Dict[str, Any]]) -> Dict[str, Any]:
        ordered = sorted(deals, key=self._deal_master_sort_key)
        return dict(ordered[0] or {}) if ordered else {}

    def _deal_master_sort_key(self, deal: Dict[str, Any]) -> Tuple[str, int]:
        add_time = str((deal or {}).get("add_time") or "").strip()
        return (add_time or "9999-12-31 23:59:59", int((deal or {}).get("id") or 0))

    def _resolve_archived_stage_id(self) -> int:
        if isinstance(self._archived_stage_id, int) and self._archived_stage_id > 0:
            return self._archived_stage_id
        stages = self._safe_pipedrive_call(lambda: self.crm.get_stages()) or []
        for stage in list(stages or []):
            if not isinstance(stage, dict):
                continue
            try:
                pipeline_id = int(stage.get("pipeline_id") or 0)
            except Exception:
                pipeline_id = 0
            stage_name = str(stage.get("name") or "").strip().lower()
            if pipeline_id == int(self.PIPELINE_ID) and "arquiv" in stage_name:
                self._archived_stage_id = int(stage.get("id") or 0)
                break
        return int(self._archived_stage_id or 0)

    def _is_archived_deal(self, deal: Dict[str, Any]) -> bool:
        archived_stage_id = self._resolve_archived_stage_id()
        return bool(archived_stage_id and int((deal or {}).get("stage_id") or 0) == archived_stage_id)

    def _consolidate_duplicates_into_master(self, master: Dict[str, Any], duplicates: List[Dict[str, Any]]) -> None:
        master_deal_id = int((master or {}).get("id") or 0)
        master_org = (master or {}).get("org_id") or {}
        master_org_id = self._extract_id(master_org)
        if not master_deal_id or not master_org_id:
            return
        master_org_payload: Dict[str, Any] = {}
        master_org_data = self._get_org_cached(master_org_id)
        master_cnpj = re.sub(r"\D+", "", str(self.crm.extract_cnpj(master_org_data) or ""))
        master_name = str((master_org_data or {}).get("name") or "").strip()
        migrated_items: List[str] = []

        for duplicate in duplicates:
            duplicate_id = int((duplicate or {}).get("id") or 0)
            duplicate_org = (duplicate or {}).get("org_id") or {}
            duplicate_org_id = self._extract_id(duplicate_org)
            duplicate_org_data = self._get_org_cached(duplicate_org_id) if duplicate_org_id else {}
            duplicate_cnpj = re.sub(r"\D+", "", str(self.crm.extract_cnpj(duplicate_org_data) or ""))
            duplicate_name = str((duplicate_org_data or {}).get("name") or "").strip()

            if not master_cnpj and duplicate_cnpj:
                master_org_payload.update(self.crm.build_org_fields(duplicate_cnpj, str((duplicate_org_data or {}).get(self.crm.CNAE_FIELD_KEY) or "").strip()))
                master_cnpj = duplicate_cnpj
                migrated_items.append(f"cnpj:{duplicate_id}")

            preferred_name = extrair_nome_fantasia(duplicate_name, None, duplicate_name)
            if preferred_name and (
                self._is_generic_org_name(master_name)
                or len(_clean_company_display_name(preferred_name).split()) < len(_clean_company_display_name(master_name).split() or [master_name])
            ):
                master_org_payload["name"] = preferred_name
                master_name = preferred_name
                migrated_items.append(f"nome:{duplicate_id}")

            duplicate_people = self._safe_pipedrive_call(lambda org_id=duplicate_org_id: self.crm.get_organization_persons(org_id, limit=100)) or []
            for person in list(duplicate_people or []):
                self._ensure_participant(master, person)
            if duplicate_people:
                migrated_items.append(f"pessoas:{duplicate_id}")

            note_lines = [
                f"Consolidado do deal duplicado #{duplicate_id}.",
                f"Empresa: {duplicate_name or str((duplicate_org or {}).get('name') or '').strip()}",
            ]
            if duplicate_cnpj:
                note_lines.append(f"CNPJ: {duplicate_cnpj}")
            if duplicate_people:
                people_summary = []
                for person in list(duplicate_people or [])[:5]:
                    person_name = str((person or {}).get('name') or '').strip()
                    phones = []
                    for item in (person or {}).get("phone") or []:
                        value = item.get("value") if isinstance(item, dict) else item
                        phone = _normalizar_telefone_br(value, self.whatsapp)
                        if phone:
                            phones.append(phone)
                    emails = []
                    for item in (person or {}).get("email") or []:
                        value = item.get("value") if isinstance(item, dict) else item
                        email = str(value or "").strip().lower()
                        if email:
                            emails.append(email)
                    summary = person_name or "Sem nome"
                    if phones:
                        summary += f" | tel: {', '.join(phones[:3])}"
                    if emails:
                        summary += f" | email: {', '.join(emails[:3])}"
                    people_summary.append(summary)
                if people_summary:
                    note_lines.append("Contatos migrados:")
                    note_lines.extend(people_summary)
            self._safe_pipedrive_call(lambda lines=note_lines: self.crm.add_note(deal_id=master_deal_id, content="\n".join(lines)))

            archived_stage_id = self._resolve_archived_stage_id()
            if archived_stage_id:
                moved = self._safe_pipedrive_call(
                    lambda deal_id=duplicate_id, stage_id=archived_stage_id: self.crm.update_stage(deal_id=deal_id, stage_id=stage_id)
                )
                if moved:
                    print(f"[DEAL_DUPLICADO_ARQUIVADO] deal={duplicate_id} master={master_deal_id}")

        sanitized_payload = {key: value for key, value in master_org_payload.items() if value not in (None, "", [], {})}
        if sanitized_payload:
            updated = self._safe_pipedrive_call(
                lambda org_id=master_org_id, payload=sanitized_payload: self.crm.update_organization(org_id, payload)
            )
            if updated:
                print(f"[DADOS_MIGRADOS_MASTER] master={master_deal_id} itens={','.join(migrated_items) or 'org'}")
                return
        if migrated_items:
            print(f"[DADOS_MIGRADOS_MASTER] master={master_deal_id} itens={','.join(migrated_items)}")

    def _get_org_cached(self, org_id: int) -> Dict[str, Any]:
        if org_id in self._cycle_org_cache:
            return self._cycle_org_cache[org_id]
        organization = self._safe_pipedrive_call(lambda: self.crm.get_organization(int(org_id)))
        if isinstance(organization, dict):
            self._cycle_org_cache[org_id] = organization
        return dict(organization or {})

    def _deal_in_target_pipeline(self, deal: Dict[str, Any]) -> bool:
        try:
            return int((deal or {}).get("pipeline_id") or 0) == int(self.PIPELINE_ID)
        except Exception:
            return False

    def _is_super_minas(self, deal: Dict[str, Any]) -> bool:
        tokens = {
            str(token or "").strip().upper()
            for token in self.crm.resolve_label_tokens((deal or {}).get("label"), self.crm.get_deal_labels())
        }
        return "SUPER_MINAS" in tokens or "SUPER MINAS" in tokens

    def _has_valid_phone_signal(self, org_name: str, org_people: List[Dict[str, Any]]) -> bool:
        cached = self._get_cached_phone_result(org_name)
        if any(bool(item.get("valid_whatsapp")) for item in cached):
            return True
        for person in list(org_people or []):
            for item in person.get("phone") or []:
                value = item.get("value") if isinstance(item, dict) else item
                normalized = _normalizar_telefone_br(value, self.whatsapp)
                if normalized and self.whatsapp.can_send(normalized) and _is_mobile_with_nine(normalized, self.whatsapp):
                    return True
        return False

    def _reserve_cycle_org(self, org_id: int) -> bool:
        with self._cycle_lock:
            if org_id in self._cycle_processed_orgs or org_id in self._run_processed_orgs:
                return False
            if len(self._run_processed_orgs) >= self.MAX_EMPRESAS:
                return False
            self._cycle_processed_orgs.add(org_id)
            self._run_processed_orgs.add(org_id)
            return True

    def _extract_phone_token(self, raw_value: Any) -> str:
        token = str(raw_value or "").strip()
        if "_" in token:
            token = token.split("_")[-1]
        return _normalizar_telefone_br(token, self.whatsapp)

    def _load_tested_phones_today(self) -> set[str]:
        today = datetime.now().strftime("%Y-%m-%d")
        payload: Dict[str, Any] = {}
        try:
            with open(self.base_dir / "sent.json", "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
                if isinstance(loaded, dict):
                    payload = loaded
        except Exception:
            payload = {}

        tested: set[str] = set()
        for item in list(payload.get(today) or []):
            normalized = self._extract_phone_token(item)
            if normalized:
                tested.update(self.whatsapp.phone_variants(normalized))
        pending = payload.get("PENDING") or {}
        if isinstance(pending, dict):
            for item in pending.keys():
                normalized = self._extract_phone_token(item)
                if normalized:
                    tested.update(self.whatsapp.phone_variants(normalized))
        return tested

    def _is_blacklist_active(self, phone: str, entry: Dict[str, Any]) -> bool:
        reason = str((entry or {}).get("reason") or "").strip()
        updated_at = str((entry or {}).get("updated_at") or "").strip()
        if reason != "tested_today_no_response":
            return True
        if not updated_at:
            return False
        return updated_at[:10] == datetime.now().strftime("%Y-%m-%d")

    def _get_ranked_company_phones(self, org_name: str, known_cnpj: str = "") -> List[Dict[str, Any]]:
        cached = self._get_cached_phone_result(org_name, known_cnpj=known_cnpj)
        if cached:
            return cached

        self._respect_external_delay()
        try:
            detailed = _coletar_telefones_multifonte_detalhado(org_name, casa_client=self.casa, known_cnpj=known_cnpj)
        finally:
            self._last_external_call = time.time()

        ranked: List[Dict[str, Any]] = []
        tested_today = self._load_tested_phones_today()
        for item in detailed:
            phone = _normalizar_telefone_br(item.get("phone"), self.whatsapp)
            if not phone:
                continue
            print(f"[TELEFONE_COLETADO] empresa={org_name} telefone={phone} fontes={','.join(item.get('sources') or [])}")

            contexts = list(item.get("contexts") or [])
            source_names = [_source_log_name(source) for source in list(item.get("sources") or [])]
            print(f"[TELEFONE_SOURCE: {'|'.join(source_names) if source_names else 'GOOGLE'}] telefone={phone}")

            if phone in self._cycle_phone_blacklist:
                print(f"[BLACKLIST_APLICADA] empresa={org_name} telefone={phone} motivo=blacklist_runtime")
                continue
            if phone in tested_today:
                print(f"[BLACKLIST_APLICADA] empresa={org_name} telefone={phone} motivo=testado_hoje")
                self._blacklist_phone(phone, "tested_today_no_response", sync_whatsapp=False)
                continue
            if not self.whatsapp.can_send(phone):
                print(f"[BLACKLIST_APLICADA] empresa={org_name} telefone={phone} motivo=blacklist_whatsapp")
                continue

            is_bot = any(detectar_bot(context) for context in contexts)
            is_whatsapp = validar_whatsapp(phone, self.whatsapp)
            is_mobile = _is_mobile_with_nine(phone, self.whatsapp)
            sources = list(dict.fromkeys(item.get("sources") or []))
            source_count = len(sources)
            cnpj_linked = bool(re.sub(r"\D+", "", str(known_cnpj or "")) and "casa_dos_dados" in sources)
            score = 0
            if is_whatsapp:
                score += 5
            for source in sources:
                score += int(PHONE_SOURCE_SCORES.get(source, 0) or 0)
            if not is_mobile:
                score -= 3
            if is_bot:
                score -= 5
            print(
                f"[TELEFONE_SCORE] empresa={org_name} telefone={phone} score={score} "
                f"whatsapp_valido={int(is_whatsapp)} fixo={int(not is_mobile)} bot={int(is_bot)}"
            )

            if is_bot:
                print(f"[TELEFONE_BOT] empresa={org_name} telefone={phone}")
                self._blacklist_phone(phone, "bot_detected_phone_enrichment")
                continue

            if not is_whatsapp:
                print(f"[TELEFONE_INVALIDO] empresa={org_name} telefone={phone} motivo=sem_whatsapp_validado")
                print(f"[TELEFONE_DESCARTADO] empresa={org_name} telefone={phone} motivo=sem_whatsapp_validado")
                continue
            if not cnpj_linked and source_count < PHONE_MIN_CONSISTENT_SOURCES:
                print(
                    f"[TELEFONE_DESCARTADO] empresa={org_name} telefone={phone} "
                    f"motivo=fonte_insuficiente fontes={source_count}"
                )
                continue
            if not is_mobile:
                print(f"[TELEFONE_FIXO] empresa={org_name} telefone={phone} score={score}")
                print(f"[TELEFONE_DESCARTADO] empresa={org_name} telefone={phone} motivo=telefone_fixo")
                continue
            print(
                f"[TELEFONE_VALIDO] empresa={org_name} telefone={phone} "
                f"cnpj_vinculado={int(cnpj_linked)} fontes={source_count}"
            )

            ranked.append(
                {
                    "phone": phone,
                    "score": score,
                    "valid_whatsapp": bool(is_whatsapp),
                    "is_mobile": bool(is_mobile),
                    "sources": sources,
                    "contexts": contexts,
                    "urls": list(item.get("urls") or []),
                }
            )

        ranked.sort(
            key=lambda item: (
                -int(bool(item.get("valid_whatsapp"))),
                -int(item.get("score") or 0),
                -int(bool(item.get("is_mobile"))),
                -len(item.get("sources") or []),
                str(item.get("phone") or ""),
            )
        )
        self._set_cached_phone_result(org_name, ranked, known_cnpj=known_cnpj)
        return ranked

    def _get_high_quality_company_phones(self, org_name: str, known_cnpj: str = "") -> List[Dict[str, Any]]:
        self._respect_external_delay()
        try:
            detailed = _coletar_telefones_high_quality_detalhado(org_name, casa_client=self.casa, known_cnpj=known_cnpj)
        finally:
            self._last_external_call = time.time()

        ranked: List[Dict[str, Any]] = []
        tested_today = self._load_tested_phones_today()
        clean_cnpj = re.sub(r"\D+", "", str(known_cnpj or ""))
        for item in detailed:
            phone = _normalizar_telefone_br(item.get("phone"), self.whatsapp)
            if not phone or not _is_real_phone_candidate(phone, known_cnpj=clean_cnpj, whatsapp_service=self.whatsapp):
                print(f"[TELEFONE_INVALIDO] empresa={org_name} telefone={phone} motivo=validacao_real")
                continue
            print(f"[TELEFONE_COLETADO] empresa={org_name} telefone={phone} fontes={','.join(item.get('sources') or [])}")

            if phone in self._cycle_phone_blacklist:
                print(f"[BLACKLIST_APLICADA] empresa={org_name} telefone={phone} motivo=blacklist_runtime")
                continue
            if phone in tested_today:
                print(f"[BLACKLIST_APLICADA] empresa={org_name} telefone={phone} motivo=testado_hoje")
                self._blacklist_phone(phone, "tested_today_no_response", sync_whatsapp=False)
                continue
            if not self.whatsapp.can_send(phone):
                print(f"[BLACKLIST_APLICADA] empresa={org_name} telefone={phone} motivo=blacklist_whatsapp")
                continue

            contexts = list(item.get("contexts") or [])
            sources = list(dict.fromkeys(item.get("sources") or []))
            is_whatsapp = validar_whatsapp(phone, self.whatsapp)
            is_mobile = _is_mobile_with_nine(phone, self.whatsapp)
            cnpj_linked = bool(clean_cnpj and "casa_dos_dados" in sources)

            if not cnpj_linked and not is_mobile:
                print(f"[TELEFONE_DESCARTADO] empresa={org_name} telefone={phone} motivo=fixo_sem_cnpj")
                continue

            score = 10
            if is_whatsapp:
                score += 5
            if is_mobile:
                score += 2
            for source in sources:
                score += int(PHONE_SOURCE_SCORES.get(source, 0) or 0)

            if not is_whatsapp:
                score -= 6
            if not cnpj_linked and len(sources) < 1:
                score -= 4

            print(
                f"[TELEFONE_VALIDO_REAL] empresa={org_name} telefone={phone} "
                f"fontes={','.join(sources)} whatsapp_valido={int(is_whatsapp)}"
            )
            ranked.append(
                {
                    "phone": phone,
                    "score": score,
                    "valid_whatsapp": bool(is_whatsapp),
                    "is_mobile": bool(is_mobile),
                    "sources": sources,
                    "contexts": contexts,
                    "urls": list(item.get("urls") or []),
                }
            )

        ranked.sort(
            key=lambda item: (
                -int(bool(item.get("valid_whatsapp"))),
                -int(bool(item.get("is_mobile"))),
                -int(item.get("score") or 0),
                str(item.get("phone") or ""),
            )
        )
        return ranked

    def _load_phone_blacklist(self) -> set[str]:
        state = self._get_runtime_state()
        payload = state.setdefault(self.PHONE_BLACKLIST_KEY, {})
        phones = set()
        for phone, entry in payload.items():
            normalized = _normalizar_telefone_br(phone, self.whatsapp)
            if normalized and self._is_blacklist_active(normalized, entry if isinstance(entry, dict) else {}):
                phones.add(normalized)
        return phones

    def _blacklist_phone(self, phone: str, reason: str, *, sync_whatsapp: bool = True) -> None:
        normalized = _normalizar_telefone_br(phone, self.whatsapp)
        if not normalized:
            return
        print(f"[BLACKLIST_APLICADA] telefone={normalized} motivo={str(reason or '').strip()}")
        self._cycle_phone_blacklist.add(normalized)
        if sync_whatsapp:
            self.whatsapp.mark_invalid(normalized, reason)
        state = self._get_runtime_state()
        payload = state.setdefault(self.PHONE_BLACKLIST_KEY, {})
        payload[normalized] = {
            "reason": str(reason or "").strip(),
            "updated_at": datetime.now().isoformat(),
        }
        self._set_runtime_state(state)

    def _phone_cache_key(self, org_name: str, known_cnpj: str = "") -> str:
        clean_cnpj = re.sub(r"\D+", "", str(known_cnpj or ""))
        if len(clean_cnpj) == 14:
            return f"cnpj:{clean_cnpj}"
        return f"name:{self._name_cache_key(org_name)}"

    def _get_cached_phone_result(self, org_name: str, known_cnpj: str = "") -> List[Dict[str, Any]]:
        state = self._get_runtime_state()
        cache = state.setdefault(self.PHONE_CACHE_KEY, {})
        item = cache.get(self._phone_cache_key(org_name, known_cnpj=known_cnpj)) or {}
        phones = item.get("phones") or []
        if not isinstance(phones, list):
            return []
        output: List[Dict[str, Any]] = []
        for phone_item in phones:
            if not isinstance(phone_item, dict):
                continue
            phone = _normalizar_telefone_br(phone_item.get("phone"), self.whatsapp)
            if not phone or phone in self._cycle_phone_blacklist:
                continue
            output.append(
                {
                    "phone": phone,
                    "score": int(phone_item.get("score") or 0),
                    "valid_whatsapp": bool(phone_item.get("valid_whatsapp")),
                    "is_mobile": bool(phone_item.get("is_mobile")) or _is_mobile_with_nine(phone, self.whatsapp),
                    "sources": list(phone_item.get("sources") or []),
                    "contexts": list(phone_item.get("contexts") or []),
                    "urls": list(phone_item.get("urls") or []),
                }
            )
        return output

    def _set_cached_phone_result(self, org_name: str, ranked_phones: List[Dict[str, Any]], known_cnpj: str = "") -> None:
        state = self._get_runtime_state()
        cache = state.setdefault(self.PHONE_CACHE_KEY, {})
        cache[self._phone_cache_key(org_name, known_cnpj=known_cnpj)] = {
            "phones": [
                {
                    "phone": str(item.get("phone") or "").strip(),
                    "score": int(item.get("score") or 0),
                    "valid_whatsapp": bool(item.get("valid_whatsapp")),
                    "is_mobile": bool(item.get("is_mobile")),
                    "sources": list(item.get("sources") or []),
                    "contexts": list(item.get("contexts") or []),
                    "urls": list(item.get("urls") or []),
                }
                for item in list(ranked_phones or [])[:3]
            ],
            "updated_at": datetime.now().isoformat(),
        }
        self._set_runtime_state(state)

    def _already_phone_enriched_today(self, org_id: int) -> bool:
        with self._state_lock:
            state = self.db.get_runtime_state(self.STATE_KEY)
            today = datetime.now().strftime("%Y-%m-%d")
            processed = (state.get(self.PHONE_RESULTS_KEY) or {}).get(today, {})
            return str(org_id) in processed

    def _mark_phone_enriched_today(self, org_id: int, ranked_phones: List[Dict[str, Any]]) -> None:
        with self._state_lock:
            state = self.db.get_runtime_state(self.STATE_KEY)
            today = datetime.now().strftime("%Y-%m-%d")
            processed = state.setdefault(self.PHONE_RESULTS_KEY, {})
            processed.setdefault(today, {})[str(org_id)] = {
                "phones": [str(item.get("phone") or "").strip() for item in list(ranked_phones or [])[:3]],
                "updated_at": datetime.now().isoformat(),
            }
            state["updated_at"] = datetime.now().isoformat()
            self.db.set_runtime_state(self.STATE_KEY, state)

    def _store_phone_result(self, org_id: int, org_name: str, ranked_phones: List[Dict[str, Any]]) -> None:
        state = self._get_runtime_state()
        results = state.setdefault("phone_results", {})
        results[str(org_id)] = {
            "empresa": str(org_name or "").strip(),
            "telefones": [
                {
                    "phone": str(item.get("phone") or "").strip(),
                    "score": int(item.get("score") or 0),
                    "sources": list(item.get("sources") or []),
                }
                for item in list(ranked_phones or [])[:3]
            ],
            "updated_at": datetime.now().isoformat(),
        }
        self._set_runtime_state(state)

    def _tag_deal_real_phone(self, deal_id: int) -> None:
        if int(deal_id or 0) <= 0:
            return
        try:
            tagged = self._safe_pipedrive_call(lambda: self.crm.add_tag("deal", int(deal_id), "TELEFONE_VALIDO_REAL"))
        except Exception:
            tagged = False
        if tagged:
            print(f"[TAG_APLICADA] deal={int(deal_id)} tag=TELEFONE_VALIDO_REAL")

    def process_light_high_quality_deal(self, deal: Dict[str, Any]) -> Dict[str, int]:
        started_at = time.perf_counter()
        counters = {"processed": 0, "enriched": 0, "ignored": 0, "people_created": 0, "cnpj_encontrado": 0}
        deal_id = int((deal or {}).get("id") or 0)
        org = (deal or {}).get("org_id") or {}
        org_id = self._extract_id(org)
        org_name = str((org or {}).get("name") if isinstance(org, dict) else (deal or {}).get("org_name") or "").strip()
        try:
            if str((deal or {}).get("status") or "open").strip().lower() != "open":
                counters["ignored"] += 1
                return counters
            if self._is_archived_deal(deal) or not self._deal_in_target_pipeline(deal):
                counters["ignored"] += 1
                return counters
            if not org_id or not org_name:
                counters["ignored"] += 1
                return counters
            if not self._reserve_cycle_org(org_id):
                counters["ignored"] += 1
                return counters

            counters["processed"] += 1
            if self._already_phone_enriched_today(org_id):
                print(f"[ORG_JA_ENRIQUECIDA] org={org_id}")
                counters["ignored"] += 1
                return counters
            organization = self._get_org_cached(org_id)
            if not organization:
                counters["ignored"] += 1
                return counters

            org_people = self._safe_pipedrive_call(lambda: self.crm.get_organization_persons(org_id, limit=100))
            if org_people is None:
                counters["ignored"] += 1
                return counters

            enrichment = None
            current_cnpj = self.crm.extract_cnpj(organization)
            if not current_cnpj:
                print(f"[FOCO_LIGHT_SEM_CNPJ] org={org_id} deal={deal_id}")
                discovered_cnpj = self._discover_cnpj(org_name)
                if discovered_cnpj:
                    current_cnpj = discovered_cnpj
                    counters["cnpj_encontrado"] += 1
                    print(f"[CNPJ_ENCONTRADO_LIGHT] org={org_id} cnpj={discovered_cnpj}")
                    enrichment = self._enrich_by_cnpj(discovered_cnpj)
                    if enrichment:
                        updated = self._update_organization_if_needed(organization, enrichment)
                        if updated:
                            print(f"[ORG_ATUALIZADA_LIGHT] org={org_id}")
                            counters["enriched"] += 1
                else:
                    print(f"[CNPJ_NAO_ENCONTRADO_LIGHT] org={org_id} nome={org_name}")

            ranked_phones = self._get_high_quality_company_phones(org_name, known_cnpj=current_cnpj)
            if not ranked_phones:
                print(f"[TELEFONE_INVALIDO] org={org_id} motivo=nenhum_telefone_real")
                self._mark_phone_enriched_today(org_id, [])
                if counters["cnpj_encontrado"] > 0 or counters["enriched"] > 0:
                    print(f"[ENRICHMENT_LIGHT_SEM_TELEFONE_MAS_COM_CNPJ] org={org_id} deal={deal_id}")
                    return counters
                counters["ignored"] += 1
                return counters

            top_ranked = list(ranked_phones[:3])
            chosen_item = top_ranked[0]
            clean_cnpj = re.sub(r"\D+", "", str(current_cnpj or ""))
            if enrichment is None and clean_cnpj and self.casa is not None:
                try:
                    enrichment = self._enrich_by_cnpj(clean_cnpj)
                except Exception:
                    enrichment = None
            person_action = self._ensure_person_for_deal(
                deal,
                org_id,
                org_people or [],
                enrichment=enrichment,
                ranked_phones=top_ranked,
                candidate_name=str((org_people[0] or {}).get("name") or "").strip() if org_people else org_name,
            )
            if person_action == "created":
                counters["people_created"] += 1
            if person_action in {"created", "updated", "exists"}:
                counters["enriched"] += 1

            self._set_cached_phone_result(org_name, ranked_phones, known_cnpj=current_cnpj)
            self._store_phone_result(org_id, org_name, ranked_phones)
            self._mark_phone_enriched_today(org_id, ranked_phones)
            self._tag_deal_real_phone(deal_id)
            self._safe_pipedrive_call(
                lambda: self.crm.add_note(
                    deal_id=deal_id,
                    content=(
                        f"Telefone real validado automaticamente: {chosen_item.get('phone')}. "
                        f"Fontes: {', '.join(chosen_item.get('sources') or []) or 'desconhecida'}."
                    ),
                )
            )
            return counters
        finally:
            print(f"[TIME_EMPRESA_LIGHT] org={org_id} deal={deal_id} ms={int((time.perf_counter() - started_at) * 1000)}")

    def _discover_cnpj(self, org_name: str) -> str:
        normalized_name = str(org_name or "").strip()
        if not normalized_name:
            return ""
        normalized_query = _normalize_company_name_for_cnpj(normalized_name) or normalized_name
        cached = self._get_cached_cnpj(normalized_name)
        if cached:
            return cached
        if self._is_cached_cnpj_miss(normalized_name):
            print(f"[CNPJ_FAIL] nome={normalized_name} motivo=cache_miss")
            return ""

        attempts = 0
        self._respect_external_delay()
        try:
            attempts += 1
            cnpj = self._run_cnpj_discovery("casa", normalized_query)
            self._consecutive_429 = 0
        except CasaDosDadosError as exc:
            if "HTTP 429" in str(exc):
                self._consecutive_429 += 1
            cnpj = ""
        finally:
            self._last_external_call = time.time()
        if cnpj:
            self._set_cached_cnpj(normalized_name, cnpj)
            return cnpj

        google_queries = [
            f"{normalized_query} CNPJ".strip(),
            f'"{normalized_name}" CNPJ'.strip(),
        ]
        if attempts < self.MAX_TENTATIVAS and google_queries:
            self._respect_external_delay()
            try:
                attempts += 1
                cnpj = self._run_cnpj_discovery("google_query", google_queries[0])
                self._consecutive_429 = 0
            except CasaDosDadosError as exc:
                if "HTTP 429" in str(exc):
                    self._consecutive_429 += 1
                cnpj = ""
            finally:
                self._last_external_call = time.time()
            if cnpj:
                print(f"[CNPJ_GOOGLE] nome={normalized_name} query={google_queries[0]}")
                self._set_cached_cnpj(normalized_name, cnpj)
                return cnpj

        if attempts >= self.MAX_TENTATIVAS:
            self._set_cached_cnpj(normalized_name, "")
            print(f"[CNPJ_FAIL] nome={normalized_name}")
            return ""
        llm_payload = self._get_llm_name_payload(normalized_name)
        if not llm_payload:
            print(f"[LLM_SKIPPED] nome={normalized_name}")
            self._set_cached_cnpj(normalized_name, "")
            print(f"[CNPJ_FAIL] nome={normalized_name}")
            return ""

        queries = list(llm_payload.get("search_queries") or [])
        if llm_payload.get("used"):
            print(f"[CNPJ_LLM] nome={normalized_name}")
            print(f"[LLM_USED] nome={normalized_name}")
        else:
            print(f"[LLM_SKIPPED] nome={normalized_name}")

        llm_queries = []
        llm_trade_name = str(llm_payload.get("trade_name") or "").strip()
        llm_company_name = str(llm_payload.get("company_name") or "").strip()
        for query in (llm_trade_name, llm_company_name):
            clean_name = _normalize_company_name_for_cnpj(query)
            if clean_name and clean_name not in llm_queries:
                llm_queries.append(f"{clean_name} CNPJ")
        for query in queries[:2]:
            clean = str(query or "").strip()
            if clean and clean not in llm_queries:
                llm_queries.append(clean)
            quoted = f'"{clean}" CNPJ'.strip()
            if clean and quoted not in llm_queries:
                llm_queries.append(quoted)
        for query in llm_queries[:2]:
            if attempts >= self.MAX_TENTATIVAS:
                break
            self._respect_external_delay()
            try:
                attempts += 1
                cnpj = self._run_cnpj_discovery("google_query", query)
                self._consecutive_429 = 0
            except CasaDosDadosError as exc:
                if "HTTP 429" in str(exc):
                    self._consecutive_429 += 1
                cnpj = ""
            finally:
                self._last_external_call = time.time()
            if cnpj:
                print(f"[CNPJ_GOOGLE] nome={normalized_name} query={query}")
                self._set_cached_cnpj(normalized_name, cnpj)
                return cnpj
        self._set_cached_cnpj(normalized_name, "")
        print(f"[CNPJ_FAIL] nome={normalized_name}")
        return ""

    def _run_cnpj_discovery(self, source: str, query: str) -> str:
        if self.casa is None:
            return ""
        if source == "casa":
            return self.casa.discover_cnpj_by_casa_search(query)
        if source == "google":
            return self.casa.discover_cnpj_by_google(query)
        if source == "google_query":
            encoded_query = requests.utils.quote(str(query or "").strip(), safe="")
            payload = self.casa._fetch_text(
                f"https://www.google.com/search?q={encoded_query}&hl=pt-BR&num=5"
            )
            return self.casa._extract_first_cnpj(payload)
        return ""

    def _get_runtime_state(self) -> Dict[str, Any]:
        with self._state_lock:
            state = self.db.get_runtime_state(self.STATE_KEY)
            if not isinstance(state, dict):
                state = {}
            state.setdefault(self.CNPJ_CACHE_KEY, {})
            state.setdefault(self.LLM_CACHE_KEY, {})
            state.setdefault(self.PHONE_CACHE_KEY, {})
            state.setdefault(self.PHONE_BLACKLIST_KEY, {})
            state.setdefault(self.PHONE_RESULTS_KEY, {})
            return state

    def _set_runtime_state(self, state: Dict[str, Any]) -> None:
        with self._state_lock:
            state["updated_at"] = datetime.now().isoformat()
            self.db.set_runtime_state(self.STATE_KEY, state)

    def _name_cache_key(self, name: str) -> str:
        return self._normalize_org_name(name)

    def _get_cached_cnpj_status(self, org_name: str) -> Dict[str, Any]:
        state = self._get_runtime_state()
        cache = state.get(self.CNPJ_CACHE_KEY) or {}
        item = cache.get(self._name_cache_key(org_name)) or {}
        return dict(item) if isinstance(item, dict) else {}

    def _get_cached_cnpj(self, org_name: str) -> str:
        item = self._get_cached_cnpj_status(org_name)
        return str(item.get("cnpj") or "").strip()

    def _is_cached_cnpj_miss(self, org_name: str) -> bool:
        item = self._get_cached_cnpj_status(org_name)
        if str(item.get("status") or "").strip().lower() != "miss":
            return False
        return str(item.get("updated_at") or "")[:10] == datetime.now().strftime("%Y-%m-%d")

    def _set_cached_cnpj(self, org_name: str, cnpj: str) -> None:
        clean_cnpj = re.sub(r"\D+", "", str(cnpj or ""))
        state = self._get_runtime_state()
        cache = state.setdefault(self.CNPJ_CACHE_KEY, {})
        cache[self._name_cache_key(org_name)] = {
            "cnpj": clean_cnpj,
            "status": "hit" if clean_cnpj else "miss",
            "updated_at": datetime.now().isoformat(),
        }
        self._set_runtime_state(state)

    def _get_llm_name_payload(self, org_name: str) -> Dict[str, Any]:
        cache_key = self._name_cache_key(org_name)
        state = self._get_runtime_state()
        cache = state.setdefault(self.LLM_CACHE_KEY, {})
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            return dict(cached)
        try:
            payload = enrich_name_with_llm(org_name)
        except Exception as exc:
            print(f"[LLM_FAIL] nome={org_name} erro={exc}")
            payload = {
                "normalized": str(org_name or "").strip(),
                "trade_name": "",
                "company_name": "",
                "search_queries": [],
                "used": False,
                "error": str(exc),
            }
        cache[cache_key] = {
            "normalized": str(payload.get("normalized") or org_name).strip(),
            "trade_name": str(payload.get("trade_name") or "").strip(),
            "company_name": str(payload.get("company_name") or "").strip(),
            "search_queries": list(payload.get("search_queries") or []),
            "used": bool(payload.get("used")),
            "error": str(payload.get("error") or ""),
            "updated_at": datetime.now().isoformat(),
        }
        self._set_runtime_state(state)
        if payload.get("error") and payload.get("used") is not True:
            print(f"[LLM_FAIL] nome={org_name} erro={payload.get('error')}")
        return dict(cache.get(cache_key) or {})

    def _enrich_by_cnpj(self, cnpj: str):
        normalized = re.sub(r"\D+", "", str(cnpj or ""))
        if len(normalized) != 14 or self.casa is None:
            return None
        for attempt in range(2):
            self._respect_external_delay()
            try:
                result = self.casa.enrich_company(normalized)
                self._last_external_call = time.time()
                self._consecutive_429 = 0
                return result
            except Exception as exc:
                self._last_external_call = time.time()
                if "HTTP 429" in str(exc):
                    self._consecutive_429 += 1
                if attempt == 1:
                    return None
        return None

    def _update_organization_if_needed(self, organization: Dict[str, Any], enrichment) -> bool:
        if not organization or not enrichment:
            return False
        payload: Dict[str, Any] = {}
        existing_cnpj = self.crm.extract_cnpj(organization)
        if not existing_cnpj and enrichment.cnpj:
            payload.update(self.crm.build_org_fields(enrichment.cnpj, enrichment.primary_cnae))
        elif not str(organization.get(self.crm.CNAE_FIELD_KEY) or "").strip() and enrichment.primary_cnae:
            payload[self.crm.CNAE_FIELD_KEY] = enrichment.primary_cnae

        current_name = str(organization.get("name") or "").strip()
        preferred_name = extrair_nome_fantasia(current_name, enrichment, current_name)
        current_name_clean = _clean_company_display_name(current_name)
        if preferred_name and (
            self._is_generic_org_name(current_name)
            or (
                current_name_clean
                and preferred_name.lower() != current_name_clean.lower()
                and len(preferred_name.split()) <= len(current_name_clean.split())
            )
        ):
            payload["name"] = preferred_name
            print(f"[NOME_FANTASIA_DEFINIDO] org={int(organization.get('id') or 0)} nome={preferred_name}")

        sanitized: Dict[str, Any] = {}
        for key, value in payload.items():
            if value in (None, "", [], {}):
                continue
            if key == self.crm.CNAE_FIELD_KEY:
                value = str(value).strip()[:64]
                if not value:
                    continue
            if key == "name":
                value = str(value).strip()[:200]
                if not value or value == current_name:
                    continue
            sanitized[key] = value

        payload = sanitized
        if not payload:
            return False
        org_id = int(organization.get("id") or 0)
        updated = self._safe_pipedrive_call(lambda: self.crm.update_organization(org_id, payload))
        if updated:
            return True
        if len(payload) == 1:
            self.logger.warning(f"[ORG_UPDATE_FAIL] org={org_id} payload={payload}")
            return False
        for key in ("name", self.crm.CNAE_FIELD_KEY, self.crm.CNPJ_FIELD_KEY):
            value = payload.get(key)
            if value in (None, "", [], {}):
                continue
            single_payload = {key: value}
            updated_single = self._safe_pipedrive_call(lambda p=single_payload: self.crm.update_organization(org_id, p))
            if updated_single:
                self.logger.warning(f"[ORG_UPDATE_FALLBACK_OK] org={org_id} campo={key}")
                return True
        self.logger.warning(f"[ORG_UPDATE_FAIL] org={org_id} payload={payload}")
        return False

    def _ensure_person_for_deal(
        self,
        deal: Dict[str, Any],
        org_id: int,
        org_people: List[Dict[str, Any]],
        enrichment,
        ranked_phones: List[Dict[str, Any]] | None = None,
        candidate_name: str = "",
    ) -> str:
        top_ranked = list(ranked_phones or [])
        candidate_name = str(candidate_name or "").strip()
        if enrichment:
            candidate_name = candidate_name or str(enrichment.main_partner or "").split(" - ")[0].strip()
            if not candidate_name:
                for decision_maker in list(getattr(enrichment, "decision_makers", []) or []):
                    candidate_name = str(decision_maker or "").split(" - ")[0].strip()
                    if candidate_name:
                        break
        if not candidate_name and org_people:
            candidate_name = str((org_people[0] or {}).get("name") or "").strip()
        if not candidate_name:
            return "none"

        candidate_phones: List[str] = []
        candidate_emails: List[str] = []
        for item in top_ranked[:5]:
            phone = _normalizar_telefone_br(item.get("phone"), self.whatsapp)
            if phone and phone not in candidate_phones:
                candidate_phones.append(phone)
        if enrichment:
            for raw_phone in list(getattr(enrichment, "phones", []) or [])[:5]:
                phone = _normalizar_telefone_br(raw_phone, self.whatsapp)
                if phone and phone not in candidate_phones:
                    candidate_phones.append(phone)
            for raw_email in list(getattr(enrichment, "emails", []) or [])[:5]:
                email = str(raw_email or "").strip().lower()
                if email and email not in candidate_emails:
                    candidate_emails.append(email)

        existing_person = self._find_existing_person(org_people, candidate_name, candidate_phones)
        if existing_person:
            merged_phones = self._merge_person_phones(existing_person, candidate_phones)
            merged_emails = self._merge_person_emails(existing_person, candidate_emails)
            update_payload: Dict[str, Any] = {}
            if merged_phones and self._should_update_person_phones(existing_person, merged_phones):
                update_payload["phone"] = merged_phones
            if merged_emails and self._should_update_person_emails(existing_person, merged_emails):
                update_payload["email"] = merged_emails
            if update_payload:
                updated = self._safe_pipedrive_call(
                    lambda: self.crm.update_person(int(existing_person.get("id") or 0), update_payload)
                )
                if updated:
                    self._ensure_participant(deal, existing_person)
                    return "updated"
            self._ensure_participant(deal, existing_person)
            return "exists"

        payload = {"name": candidate_name, "org_id": int(org_id)}
        if candidate_phones:
            payload["phone"] = candidate_phones[:5]
        if candidate_emails:
            payload["email"] = candidate_emails[:5]
        person = self._safe_pipedrive_call(lambda: self.crm.create_person(payload))
        if not person:
            return "none"
        self._ensure_participant(deal, person)
        return "created"

    def _merge_person_phones(self, person: Dict[str, Any], candidate_phones: List[str]) -> List[str]:
        merged: List[str] = []
        seen = set()
        for phone in list(candidate_phones or []):
            normalized = _normalizar_telefone_br(phone, self.whatsapp)
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
        for item in person.get("phone") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = _normalizar_telefone_br(value, self.whatsapp)
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
        return merged[:5]

    def _should_update_person_phones(self, person: Dict[str, Any], merged_phones: List[str]) -> bool:
        current = []
        for item in person.get("phone") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = _normalizar_telefone_br(value, self.whatsapp)
            if normalized:
                current.append(normalized)
        return current[:5] != list(merged_phones or [])[:5]

    def _merge_person_emails(self, person: Dict[str, Any], candidate_emails: List[str]) -> List[str]:
        merged: List[str] = []
        seen = set()
        for email in list(candidate_emails or []):
            normalized = str(email or "").strip().lower()
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
        for item in person.get("email") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = str(value or "").strip().lower()
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
        return merged[:5]

    def _should_update_person_emails(self, person: Dict[str, Any], merged_emails: List[str]) -> bool:
        current = []
        for item in person.get("email") or []:
            value = item.get("value") if isinstance(item, dict) else item
            normalized = str(value or "").strip().lower()
            if normalized:
                current.append(normalized)
        return current[:5] != list(merged_emails or [])[:5]

    def _ensure_participant(self, deal: Dict[str, Any], person: Dict[str, Any]) -> None:
        deal_id = int(deal.get("id") or 0)
        person_id = self._extract_id(person.get("id") or person)
        if not deal_id or not person_id:
            return
        participants = self._safe_pipedrive_call(lambda: self.crm.get_deal_participants(deal_id, limit=50))
        if participants is None:
            return
        for participant in participants:
            if self._extract_id(participant.get("person_id")) == person_id:
                return
        self._safe_pipedrive_call(lambda: self.crm.add_deal_participant(deal_id, person_id))

    def _find_existing_person(self, persons: List[Dict[str, Any]], candidate_name: str, candidate_phones: List[str]) -> Dict[str, Any]:
        normalized_name = self._normalize_person_name(candidate_name)
        normalized_phones = {
            _normalizar_telefone_br(phone, self.whatsapp)
            for phone in list(candidate_phones or [])
            if _normalizar_telefone_br(phone, self.whatsapp)
        }
        for person in persons:
            name = self._normalize_person_name(person.get("name"))
            phones = []
            for item in person.get("phone") or []:
                value = item.get("value") if isinstance(item, dict) else item
                phone = _normalizar_telefone_br(value, self.whatsapp)
                if phone:
                    phones.append(phone)
            if normalized_phones and any(phone in normalized_phones for phone in phones):
                return person
            if normalized_name and normalized_name == name:
                return person
        return {}

    def _already_enriched_today(self, org_id: int) -> bool:
        with self._state_lock:
            state = self.db.get_runtime_state(self.STATE_KEY)
            today = datetime.now().strftime("%Y-%m-%d")
            processed = (state.get("processed_by_day") or {}).get(today, {})
            return str(org_id) in processed

    def _mark_enriched_today(self, org_id: int, cnpj: str) -> None:
        with self._state_lock:
            state = self.db.get_runtime_state(self.STATE_KEY)
            today = datetime.now().strftime("%Y-%m-%d")
            processed = state.setdefault("processed_by_day", {})
            processed.setdefault(today, {})[str(org_id)] = {
                "cnpj": str(cnpj or "").strip(),
                "updated_at": datetime.now().isoformat(),
            }
            state["updated_at"] = datetime.now().isoformat()
            self.db.set_runtime_state(self.STATE_KEY, state)

    @staticmethod
    def _normalize_org_name(name: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

    @staticmethod
    def _normalize_person_name(name: Any) -> str:
        return re.sub(r"\s+", " ", str(name or "").strip().lower())

    @staticmethod
    def _extract_id(value: Any) -> int:
        if isinstance(value, dict):
            try:
                return int(value.get("value") or value.get("id") or 0)
            except Exception:
                return 0
        try:
            return int(value or 0)
        except Exception:
            return 0

    @staticmethod
    def _is_generic_org_name(name: str) -> bool:
        clean = str(name or "").strip().lower()
        if not clean:
            return True
        generic_terms = ("empresa", "lead", "negócio", "negocio", "cliente", "contato")
        return any(term == clean or clean.startswith(f"{term} ") for term in generic_terms)


if __name__ == "__main__":
    from crm.pipedrive_client import PipedriveClient

    logger = logging.getLogger("crm_enrichment_loop")
    logging.basicConfig(level=logging.INFO)
    loop = CRMEnrichmentLoop(PipedriveClient(), logger, Path(__file__).resolve().parents[1])
    loop.run_forever()
