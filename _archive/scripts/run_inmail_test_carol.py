from __future__ import annotations

import csv
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from playwright.sync_api import Browser, Error, Page, sync_playwright
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dependencia opcional
    def load_dotenv(*args, **kwargs):
        return False

BASE_DIR = Path(__file__).resolve().parent.parent
MAX_INMAIL_TESTE = 5
MAX_TESTE = 5
INMAIL_SENT_FILE = BASE_DIR / "inmail_enviados.txt"
FALLBACK_TARGETS = BASE_DIR / "config" / "linkedin_targets.json"
SAVED_LIST_URL = "https://www.linkedin.com/sales/lists/people/7434225554490433536"
FEED_URL = "https://www.linkedin.com/feed/"

PITCH_SPINTAX = (
    "{Oi|Fala|OlÃ¡} {primeiro_nome}, tudo bem? "
    "{Vi|Notei} sua atuaÃ§Ã£o e queria te perguntar direto: "
    "vocÃªs hoje conseguem capturar dados reais de quem consome na ponta "
    "ou ainda fica meio no escuro? {Tenho ajudado|Estamos ajudando} marcas "
    "com isso. Faz sentido olhar?"
)

_API_CLIENT = None
_API_ENABLED = True
_API_INIT_DONE = False
_FAST_ROUTE_CONFIGURED = False

load_dotenv()
load_dotenv(BASE_DIR / ".env")
load_dotenv(Path("C:/Users/Asus/.env"))


def log(msg: str) -> None:
    print(f"[TESTE REAL LINKEDIN] {msg}")


def log_result(nome: str, empresa: str, acao: str, resultado: str) -> None:
    print("[TESTE REAL LINKEDIN]")
    print(f"Nome: {nome}")
    print(f"Empresa: {empresa}")
    print(f"Ação: {acao}")
    print(f"Resultado: {resultado}")


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _safe_fast_reset(page: Page) -> None:
    try:
        page.goto(FEED_URL, wait_until="domcontentloaded", timeout=8000)
        page.wait_for_timeout(random.randint(1000, 1500))
    except Exception:
        pass


def _configure_fast_mode(context) -> None:
    global _FAST_ROUTE_CONFIGURED
    if _FAST_ROUTE_CONFIGURED:
        return

    def _route_handler(route):
        req = route.request
        if req.resource_type in {"image", "media", "font"}:
            route.abort()
            return
        route.continue_()

    context.route("**/*", _route_handler)
    _FAST_ROUTE_CONFIGURED = True


def _open_linkedin_fast(page: Page, reused_tab: bool) -> None:
    t0 = _now_ms()
    try:
        page.bring_to_front()
    except Exception:
        pass
    page.goto(FEED_URL, wait_until="domcontentloaded", timeout=12000)
    page.wait_for_timeout(random.randint(1000, 1500))
    dt = int(_now_ms() - t0)
    log(f"[LINKEDIN_FAST] reutilizou aba={str(reused_tab).lower()} | tempo_abertura_ms={dt}")


def _try_import_linkedin_client():
    try:
        from linkedin_api import Linkedin  # type: ignore

        return Linkedin
    except Exception:
        local_repo = Path("C:/Users/Asus/linkedin-api")
        if local_repo.exists():
            import sys

            sys.path.append(str(local_repo))
            from linkedin_api import Linkedin  # type: ignore

            return Linkedin
        raise


def expand_spintax(template: str, values: Dict[str, str]) -> str:
    text = str(template or "")
    for key, value in values.items():
        text = text.replace("{" + key + "}", str(value or "").strip())
    while True:
        match = re.search(r"\{([^{}]+)\}", text)
        if not match:
            break
        choices = [part.strip() for part in match.group(1).split("|") if part.strip()]
        selected = random.choice(choices) if choices else ""
        text = text[: match.start()] + selected + text[match.end() :]
    return " ".join(text.split()).strip()


def load_leads_carol(include_fallback: bool = True) -> List[Dict[str, str]]:
    candidates = [
        BASE_DIR / "carol.txt",
        BASE_DIR / "carol.json",
        BASE_DIR / "carol.csv",
        BASE_DIR / "config" / "carol.txt",
        BASE_DIR / "config" / "carol.json",
        BASE_DIR / "config" / "carol.csv",
    ]
    leads: List[Dict[str, str]] = []

    for file_path in candidates:
        if not file_path.exists():
            continue
        suffix = file_path.suffix.lower()
        try:
            if suffix == ".txt":
                for line in file_path.read_text(encoding="utf-8").splitlines():
                    raw = line.strip()
                    if not raw:
                        continue
                    if raw.startswith("http"):
                        leads.append({"nome": "", "profile_url": raw, "public_id": ""})
                    else:
                        parts = [item.strip() for item in raw.split(";")]
                        if len(parts) >= 2:
                            leads.append({"nome": parts[0], "profile_url": parts[1], "public_id": ""})
            elif suffix == ".json":
                payload = json.loads(file_path.read_text(encoding="utf-8"))
                if isinstance(payload, list):
                    for item in payload:
                        if not isinstance(item, dict):
                            continue
                        leads.append(
                            {
                                "nome": str(item.get("nome") or item.get("name") or "").strip(),
                                "profile_url": str(item.get("profile_url") or item.get("url") or "").strip(),
                                "public_id": str(item.get("public_id") or item.get("id") or "").strip(),
                            }
                        )
            elif suffix == ".csv":
                with file_path.open("r", encoding="utf-8", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        leads.append(
                            {
                                "nome": str(row.get("nome") or row.get("name") or "").strip(),
                                "profile_url": str(row.get("profile_url") or row.get("url") or "").strip(),
                                "public_id": str(row.get("public_id") or row.get("id") or "").strip(),
                            }
                        )
        except Exception as exc:
            log(f"erro lendo lista LEADS CAROL ({file_path.name}): {exc}")
        if leads:
            log(f"lista LEADS CAROL carregada de {file_path}")
            break

    if include_fallback and not leads and FALLBACK_TARGETS.exists():
        try:
            payload = json.loads(FALLBACK_TARGETS.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    leads.append(
                        {
                            "nome": str(item.get("nome") or "").strip(),
                            "profile_url": str(item.get("profile_url") or "").strip(),
                            "public_id": str(item.get("public_id") or "").strip(),
                        }
                    )
                if leads:
                    log(f"lista fallback carregada: {FALLBACK_TARGETS}")
        except Exception as exc:
            log(f"erro carregando fallback de leads: {exc}")
    return leads


def normalize_profile_url(profile_url: str, public_id: str) -> str:
    url = str(profile_url or "").strip()
    if url:
        return url
    pid = str(public_id or "").strip().strip("/")
    if pid:
        return f"https://www.linkedin.com/in/{pid}/"
    return ""


def load_sent_registry() -> set[str]:
    if not INMAIL_SENT_FILE.exists():
        INMAIL_SENT_FILE.write_text("", encoding="utf-8")
        return set()
    return {line.strip().lower() for line in INMAIL_SENT_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}


def mark_sent(identifier: str) -> None:
    key = str(identifier or "").strip().lower()
    if not key:
        return
    with INMAIL_SENT_FILE.open("a", encoding="utf-8") as file_obj:
        file_obj.write(key + "\n")


def _candidate_cdp_urls() -> List[str]:
    from_env = str(os.getenv("LINKEDIN_CDP_URL", "")).strip()
    if from_env:
        return [from_env]
    # Porta fixa para evitar alternância automática de sessão.
    return ["http://127.0.0.1:9223"]


def ensure_li_at_from_browser(page: Page) -> None:
    current = str(os.getenv("LINKEDIN_LI_AT", "")).strip()
    try:
        cookies = page.context.cookies(["https://www.linkedin.com"])
    except Exception as exc:
        log(f"[ERRO] [LINKEDIN] [API] falha ao ler cookies do browser: {exc}")
        return
    for cookie in cookies:
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "").strip()
        if not value:
            continue
        if name == "li_at" and not current:
            os.environ["LINKEDIN_LI_AT"] = value
            current = value
            log("[LINKEDIN] [API] LINKEDIN_LI_AT carregado da sessão existente")
        if name == "JSESSIONID" and not str(os.getenv("LINKEDIN_JSESSIONID", "")).strip():
            os.environ["LINKEDIN_JSESSIONID"] = value
    if current and str(os.getenv("LINKEDIN_JSESSIONID", "")).strip():
        log("[LINKEDIN] [API] JSESSIONID carregado da sessão existente")


def _connect_existing_browser(playwright) -> Optional[Browser]:
    for cdp_url in _candidate_cdp_urls():
        try:
            browser = playwright.chromium.connect_over_cdp(cdp_url)
            log(f"CDP conectado: {cdp_url}")
            return browser
        except Exception as exc:
            log(f"erro ao conectar CDP existente ({cdp_url}): {exc}")
            continue
    return None


def get_existing_linkedin_page(playwright) -> Optional[Page]:
    browser = _connect_existing_browser(playwright)
    if browser is None:
        return None
    if not browser.contexts:
        log("nenhum contexto existente encontrado")
        return None
    context = browser.contexts[0]
    _configure_fast_mode(context)
    for page in context.pages:
        current_url = str(page.url or "")
        if "linkedin.com" in current_url:
            try:
                page.bring_to_front()
            except Exception:
                pass
            return page
    return context.new_page()


def collect_leads_from_saved_list(page: Page, list_name: str = "CAROL", limit: int = 30) -> List[Dict[str, str]]:
    leads: List[Dict[str, str]] = []
    try:
        page.goto(SAVED_LIST_URL, wait_until="domcontentloaded", timeout=30000)
    except Exception as exc:
        log(f"erro ao abrir Sales Navigator lists: {exc}")
        return leads

    current = str(page.url or "").lower()
    if "login" in current or "authwall" in current:
        log("sessÃ£o sem autenticaÃ§Ã£o para Sales Navigator")
        return leads

    if "/sales/lists/people/" in current:
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass
        try:
            for _ in range(8):
                page.mouse.wheel(0, 1800)
                page.wait_for_timeout(250)
            extracted = page.evaluate(
                """(maxItems) => {
                    const out = [];
                    const seen = new Set();
                    const anchors = Array.from(document.querySelectorAll("a[href]"));
                    for (const a of anchors) {
                        const hrefRaw = (a.getAttribute("href") || "").trim();
                        if (!hrefRaw) continue;
                        if (!hrefRaw.includes("/sales/lead/") && !hrefRaw.includes("linkedin.com/in/")) continue;
                        let href = hrefRaw.startsWith("/") ? `https://www.linkedin.com${hrefRaw}` : hrefRaw;
                        href = href.split("?")[0];
                        if (seen.has(href)) continue;
                        seen.add(href);
                        const nome = (a.textContent || "").replace(/\\s+/g, " ").trim();
                        out.push({ nome, profile_url: href, public_id: "" });
                        if (out.length >= Number(maxItems || 30)) break;
                    }
                    return out;
                }""",
                int(limit),
            )
            if isinstance(extracted, list):
                for item in extracted:
                    if not isinstance(item, dict):
                        continue
                    href = str(item.get("profile_url") or "").strip()
                    if not href:
                        continue
                    leads.append(
                        {
                            "nome": str(item.get("nome") or "").strip(),
                            "profile_url": href,
                            "public_id": str(item.get("public_id") or "").strip(),
                        }
                    )
        except Exception as exc:
            log(f"erro ao extrair leads da lista salva: {exc}")
        if not leads:
            log("lista LEADS CAROL abriu, mas nenhum lead foi extraido do DOM")
        return leads

    list_locator_candidates = [
        lambda: page.get_by_role("link", name=re.compile(rf"\b{re.escape(list_name)}\b", re.IGNORECASE)).first,
        lambda: page.locator(f"a:has-text('{list_name}')").first,
        lambda: page.get_by_role("link", name=re.compile(rf"\bleads?\s+{re.escape(list_name)}\b", re.IGNORECASE)).first,
    ]
    clicked = False
    for fn in list_locator_candidates:
        try:
            item = fn()
            item.wait_for(state="visible", timeout=6000)
            item.scroll_into_view_if_needed()
            item.click()
            clicked = True
            break
        except Exception:
            continue

    if not clicked:
        log(f"lista salva '{list_name}' nÃ£o encontrada na sessÃ£o")
        return leads

    try:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass

    try:
        cards = page.locator("a[href*='linkedin.com/in/'], a[href*='/sales/lead/']")
        total = min(cards.count(), int(limit))
        for idx in range(total):
            card = cards.nth(idx)
            href = str(card.get_attribute("href") or "").strip()
            nome = " ".join((card.inner_text() or "").split()).strip()
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.linkedin.com" + href
            leads.append({"nome": nome, "profile_url": href, "public_id": ""})
    except Exception as exc:
        log(f"erro ao extrair leads da lista salva: {exc}")
    return leads


def find_inmail_button(page: Page):
    strategies = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*inmail\s*$", re.IGNORECASE)).first,
        lambda: page.get_by_role("button", name=re.compile(r"^\s*enviar inmail\s*$", re.IGNORECASE)).first,
    ]
    for idx, fn in enumerate(strategies, start=1):
        try:
            locator = fn()
            locator.wait_for(state="visible", timeout=3000)
            if locator.is_visible() and locator.is_enabled():
                log(f"seletor botÃ£o OK #{idx}")
                return locator
        except Exception:
            continue
    return None


def find_message_button(page: Page):
    strategies = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*message\s*$", re.IGNORECASE)).first,
        lambda: page.get_by_role("button", name=re.compile(r"^\s*mensagem\s*$", re.IGNORECASE)).first,
    ]
    for fn in strategies:
        try:
            locator = fn()
            locator.wait_for(state="visible", timeout=2000)
            if locator.is_visible() and locator.is_enabled():
                return locator
        except Exception:
            continue
    return None


def try_open_inmail_via_more(page: Page) -> bool:
    more_buttons = [
        lambda: page.get_by_role("button", name=re.compile(r"more actions|mais ações|mais acoes", re.IGNORECASE)).first,
        lambda: page.get_by_role("button", name=re.compile(r"more|mais", re.IGNORECASE)).first,
    ]
    more_clicked = False
    for fn in more_buttons:
        try:
            btn = fn()
            btn.wait_for(state="visible", timeout=2000)
            if btn.is_visible() and btn.is_enabled():
                btn.scroll_into_view_if_needed()
                btn.click()
                more_clicked = True
                log("fallback: menu Mais/More aberto")
                break
        except Exception:
            continue
    if not more_clicked:
        return False

    inmail_actions = [
        lambda: page.get_by_role("menuitem", name=re.compile(r"send inmail|enviar inmail|inmail", re.IGNORECASE)).first,
        lambda: page.get_by_role("button", name=re.compile(r"send inmail|enviar inmail|inmail", re.IGNORECASE)).first,
        lambda: page.get_by_text(re.compile(r"send inmail|enviar inmail", re.IGNORECASE)).first,
    ]
    for fn in inmail_actions:
        try:
            item = fn()
            item.wait_for(state="visible", timeout=2500)
            if item.is_visible() and item.is_enabled():
                item.scroll_into_view_if_needed()
                item.click()
                log("fallback: ação InMail encontrada no menu")
                return True
        except Exception:
            continue
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    return False


def has_inmail_composer_marker(page: Page) -> bool:
    checks = [
        lambda: page.get_by_text(re.compile(r"\binmail\b", re.IGNORECASE)).first,
        lambda: page.get_by_role("heading", name=re.compile(r"inmail", re.IGNORECASE)).first,
    ]
    for fn in checks:
        try:
            node = fn()
            node.wait_for(state="visible", timeout=1500)
            if node.is_visible():
                return True
        except Exception:
            continue
    return False


def _is_sales_lead_url(url: str) -> bool:
    return "/sales/lead/" in str(url or "").lower()


def find_subject_box(page: Page):
    options = [
        lambda: page.get_by_label(re.compile(r"assunto|subject", re.IGNORECASE)).first,
        lambda: page.get_by_placeholder(re.compile(r"assunto|subject", re.IGNORECASE)).first,
        lambda: page.get_by_role("textbox", name=re.compile(r"assunto|subject", re.IGNORECASE)).first,
    ]
    for fn in options:
        try:
            locator = fn()
            locator.wait_for(state="visible", timeout=1200)
            if locator.is_visible() and locator.is_enabled():
                return locator
        except Exception:
            continue
    return None


def find_message_box(page: Page):
    options = [
        lambda: page.get_by_role("textbox", name=re.compile(r"mensagem|message|inmail|write", re.IGNORECASE)).first,
        lambda: page.get_by_label(re.compile(r"mensagem|message|inmail|write", re.IGNORECASE)).first,
        lambda: page.get_by_placeholder(re.compile(r"mensagem|message|inmail|write", re.IGNORECASE)).first,
        lambda: page.locator("div[contenteditable='true'][role='textbox']").first,
        lambda: page.locator("form textarea").first,
    ]
    for idx, fn in enumerate(options, start=1):
        try:
            locator = fn()
            locator.wait_for(state="visible", timeout=3000)
            if locator.is_visible() and locator.is_enabled():
                log(f"seletor campo mensagem OK #{idx}")
                return locator
        except Exception:
            continue
    return None


def find_send_button(page: Page):
    options = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*send\s*$", re.IGNORECASE)).first,
        lambda: page.get_by_role("button", name=re.compile(r"^\s*enviar\s*$", re.IGNORECASE)).first,
        lambda: page.get_by_role("button", name=re.compile(r"send inmail|enviar inmail", re.IGNORECASE)).first,
        lambda: page.locator("button[type='submit']").first,
    ]
    for fn in options:
        try:
            locator = fn()
            locator.wait_for(state="visible", timeout=2500)
            if locator.is_visible() and locator.is_enabled():
                return locator
        except Exception:
            continue
    return None


def compose_message(nome: str) -> str:
    primeiro_nome = (str(nome or "").strip().split() or ["contato"])[0]
    return expand_spintax(PITCH_SPINTAX, {"primeiro_nome": primeiro_nome})


def _get_api_client():
    global _API_CLIENT, _API_ENABLED, _API_INIT_DONE
    if _API_INIT_DONE:
        return _API_CLIENT if _API_ENABLED else None

    li_at = str(os.getenv("LINKEDIN_LI_AT", "")).strip()
    if not li_at:
        log("[ERRO] LINKEDIN_LI_AT não definido")
        log("[ERRO] [LINKEDIN] [API] LINKEDIN_LI_AT não definido")
        _API_ENABLED = False
        _API_INIT_DONE = True
        return None
    try:
        Linkedin = _try_import_linkedin_client()
        api = None
        try:
            api = Linkedin("", "", cookies={"li_at": li_at})
        except TypeError:
            # Compatibilidade com versão local do linkedin-api sem suporte a cookies no construtor.
            import logging

            from requests.cookies import RequestsCookieJar
            from linkedin_api.client import Client  # type: ignore
            from linkedin_api import linkedin as linkedin_module  # type: ignore

            client = Client(debug=False)
            cookiejar = RequestsCookieJar()
            cookiejar.set("li_at", li_at, domain=".linkedin.com", path="/")
            jsession = str(os.getenv("LINKEDIN_JSESSIONID", "")).strip()
            if jsession:
                cookiejar.set("JSESSIONID", jsession, domain=".linkedin.com", path="/")
            client._set_session_cookies(cookiejar)  # type: ignore[attr-defined]
            api = Linkedin.__new__(Linkedin)
            api.client = client
            api.logger = getattr(linkedin_module, "logger", logging.getLogger("linkedin_api"))
        try:
            # Sanity check de autenticação solicitado
            api.get_profile("me")
        except Exception as exc:
            # Algumas versões da lib não suportam "me" e retornam erro mesmo com cookie válido.
            try:
                api.get_conversations()
            except Exception:
                log(f"[ERRO] Falha na autenticação LinkedIn API: {exc}")
                log("[ERRO] Cookie inválido ou expirado")
                _API_ENABLED = False
                _API_INIT_DONE = True
                return None
        _API_CLIENT = api
        _API_ENABLED = True
        _API_INIT_DONE = True
        log("[API OK] LinkedIn autenticado")
        return _API_CLIENT
    except Exception as exc:
        log(f"[ERRO] Falha ao inicializar linkedin-api: {exc}")
        _API_ENABLED = False
        _API_INIT_DONE = True
        return None


def _extract_public_id_from_profile_url(profile_url: str) -> str:
    url = str(profile_url or "").strip()
    match = re.search(r"linkedin\.com/in/([^/?#]+)", url, flags=re.IGNORECASE)
    return str(match.group(1)).strip() if match else ""


def _extract_urn_id_from_sales_url(profile_url: str) -> str:
    url = str(profile_url or "").strip()
    match = re.search(r"/sales/lead/([^,/?#]+)", url, flags=re.IGNORECASE)
    return str(match.group(1)).strip() if match else ""


def _parse_sales_auth(profile_url: str) -> tuple[str, str, str]:
    url = str(profile_url or "").strip()
    m = re.search(r"/sales/lead/([^,]+),([^,]+),([^/?#]+)", url, flags=re.IGNORECASE)
    if not m:
        return "", "", ""
    return str(m.group(1)).strip(), str(m.group(2)).strip(), str(m.group(3)).strip()


def _session_cookies_for_requests(page: Page) -> Dict[str, str]:
    try:
        cookies = page.context.cookies(["https://www.linkedin.com"])
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for item in cookies:
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "").strip()
        if name and value:
            out[name] = value
    return out


def _fetch_sales_profile(page: Page, profile_url: str) -> Dict[str, Any]:
    pid, auth_type, auth_token = _parse_sales_auth(profile_url)
    if not pid:
        return {}
    cookies = _session_cookies_for_requests(page)
    if not cookies:
        return {}
    csrf = str(cookies.get("JSESSIONID", "")).strip().strip('"')
    headers = {
        "accept": "application/json",
        "csrf-token": csrf,
        "x-restli-protocol-version": "2.0.0",
        "user-agent": "Mozilla/5.0",
    }
    url = f"https://www.linkedin.com/sales-api/salesApiProfiles/(profileId:{pid},authType:{auth_type},authToken:{auth_token})"
    params = {
        "decoration": "(entityUrn,objectUrn,fullName,firstName,lastName,degree,pendingInvitation,flagshipProfileUrl)"
    }
    try:
        res = requests.get(url, params=params, cookies=cookies, headers=headers, timeout=20)
        payload = res.json()
        if isinstance(payload, dict) and "status" in payload and int(payload.get("status") or 0) >= 400:
            return {}
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _clean_lead_name(raw_name: str) -> str:
    text = str(raw_name or "").strip()
    text = re.sub(r"\s+A última atividade.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+está disponível.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_public_id_from_page(page: Page) -> str:
    try:
        result = page.evaluate(
            """() => {
                const anchors = Array.from(document.querySelectorAll('a[href]'));
                for (const a of anchors) {
                    const href = (a.getAttribute('href') || '').trim();
                    const m = href.match(/linkedin\\.com\\/in\\/([^/?#]+)/i) || href.match(/\\/in\\/([^/?#]+)/i);
                    if (m && m[1]) return String(m[1]).trim();
                }
                return '';
            }"""
        )
        return str(result or "").strip()
    except Exception:
        return ""


def _resolve_public_id(page: Page, lead: Dict[str, str]) -> str:
    public_id = str(lead.get("public_id") or "").strip()
    if public_id:
        return public_id
    from_url = _extract_public_id_from_profile_url(str(lead.get("profile_url") or ""))
    if from_url:
        return from_url
    return _extract_public_id_from_page(page)


def _resolve_public_id_by_name(api, nome: str) -> str:
    cleaned = _clean_lead_name(nome)
    if not cleaned:
        return ""
    query = " ".join(cleaned.split()[:2]).strip()
    if not query:
        return ""
    try:
        results = api.search_people(keywords=query, limit=10) or []
    except Exception:
        return ""
    target = re.sub(r"[^a-z0-9 ]", "", cleaned.lower())
    for item in results:
        if not isinstance(item, dict):
            continue
        candidate_name = str(item.get("name") or item.get("title") or "").strip()
        candidate_norm = re.sub(r"[^a-z0-9 ]", "", candidate_name.lower())
        if target and candidate_norm and target.startswith(candidate_norm[: max(4, len(candidate_norm) - 2)]):
            pid = str(item.get("public_id") or item.get("publicIdentifier") or "").strip()
            if pid:
                return pid
    return ""


def _is_connected_from_profile(profile: Dict[str, Any]) -> bool:
    connection = profile.get("connectionOfViewer")
    if isinstance(connection, dict):
        if bool(connection.get("connected")):
            return True
        value = str(connection.get("value") or "").lower()
        if value in {"connected", "first_degree_connection", "first_degree"}:
            return True
    if isinstance(connection, str) and connection.strip().lower() in {"connected", "first_degree_connection"}:
        return True
    if bool(profile.get("is_connected")):
        return True
    return False


def _resolve_profile_urn(profile: Dict[str, Any]) -> str:
    candidates = [
        profile.get("entityUrn"),
        profile.get("objectUrn"),
        profile.get("dashEntityUrn"),
        (profile.get("miniProfile") or {}).get("entityUrn") if isinstance(profile.get("miniProfile"), dict) else "",
    ]
    for urn in candidates:
        value = str(urn or "").strip()
        if value.startswith("urn:li:"):
            return value
    return ""


def _api_add_connection(api, public_id: str, message: str) -> bool:
    if hasattr(api, "add_connection"):
        api.add_connection(profile_public_id=public_id, message=message[:300])
        return True
    payload = {
        "invitee": {
            "com.linkedin.voyager.growth.invitation.InviteeProfile": {
                "profileId": public_id,
            }
        },
        "customMessage": message[:300],
    }
    res = api._post("/growth/normInvitations", params={"action": "verifyQuotaAndCreate"}, data=json.dumps(payload))
    return int(getattr(res, "status_code", 0)) in {200, 201}


def try_api_fallback(page: Page, lead: Dict[str, str]) -> bool:
    action_t0 = _now_ms()
    api = _get_api_client()
    nome = str(lead.get("nome") or "").strip() or "contato"
    profile_url = normalize_profile_url(str(lead.get("profile_url") or ""), str(lead.get("public_id") or ""))
    empresa = str(lead.get("empresa") or "N/D").strip() or "N/D"
    if api is None:
        log("[FALLBACK BLOQUEADO] API indisponível")
        log_result(nome, empresa, "fallback_api", "bloqueado")
        return False
    public_id = _resolve_public_id(page, lead)
    urn_id = _extract_urn_id_from_sales_url(profile_url)
    mensagem = compose_message(nome)
    try:
        log(f"[FALLBACK] avaliando lead | Nome: {nome} | Perfil: {profile_url or public_id or urn_id}")
        sales_profile = _fetch_sales_profile(page, profile_url)
        if sales_profile:
            object_urn = str(sales_profile.get("objectUrn") or "").strip()
            degree = str(sales_profile.get("degree") or "").strip()
            flagship = str(sales_profile.get("flagshipProfileUrl") or "").strip()
            pending = bool(sales_profile.get("pendingInvitation", False))
            if not public_id:
                public_id = _extract_public_id_from_profile_url(flagship)
            if object_urn and degree == "1":
                send_error = api.send_message(message_body=mensagem, recipients=[object_urn])
                if not send_error:
                    log(f"[LINKEDIN_FAST] ação executada=api_send_message | tempo_total_ms={int(_now_ms() - action_t0)}")
                    log("[MSG] enviada via API")
                    log_result(nome, empresa, "send_message", "enviada_via_api")
                    return True
                log(f"[ERRO] [FALLBACK] send_message retornou erro para {nome}")
                return False
            if pending:
                log(f"[SKIP] {nome} | convite já pendente")
                return False
            if degree and degree != "1":
                if public_id and _api_add_connection(api, public_id=public_id, message=mensagem):
                    log(f"[LINKEDIN_FAST] ação executada=api_add_connection | tempo_total_ms={int(_now_ms() - action_t0)}")
                    log("[CONNECTION] enviada via API")
                    log_result(nome, empresa, "add_connection", "enviada_via_api")
                    return True
                log(f"[ERRO] [FALLBACK] convite não confirmado | {nome}")
                return False

        if not public_id:
            public_id = _resolve_public_id_by_name(api, nome)
        if public_id:
            profile = api.get_profile(public_id=public_id) or {}
        elif urn_id:
            profile = api.get_profile(urn_id=urn_id) or {}
            if not public_id:
                public_id = (
                    str(profile.get("public_id") or "").strip()
                    or str(profile.get("publicIdentifier") or "").strip()
                    or str(profile.get("vanityName") or "").strip()
                )
        else:
            log(f"[SKIP] {nome} | sem public_id/urn_id para fallback")
            return False
        if _is_connected_from_profile(profile):
            urn = _resolve_profile_urn(profile)
            if not urn:
                log(f"[ERRO] [FALLBACK] conectado sem URN para send_message | {nome}")
                return False
            send_error = api.send_message(message_body=mensagem, recipients=[urn])
            if send_error:
                log(f"[ERRO] [FALLBACK] send_message retornou erro para {nome}")
                return False
            log(f"[LINKEDIN_FAST] ação executada=api_send_message | tempo_total_ms={int(_now_ms() - action_t0)}")
            log("[MSG] enviada via API")
            log_result(nome, empresa, "send_message", "enviada_via_api")
            return True

        if not public_id:
            log(f"[SKIP] {nome} | sem public_id para add_connection")
            return False
        if not _api_add_connection(api, public_id=public_id, message=mensagem):
            log(f"[ERRO] [FALLBACK] convite não confirmado | {nome}")
            return False
        log(f"[LINKEDIN_FAST] ação executada=api_add_connection | tempo_total_ms={int(_now_ms() - action_t0)}")
        log("[CONNECTION] enviada via API")
        log_result(nome, empresa, "add_connection", "enviada_via_api")
        return True
    except Exception as exc:
        log(f"[ERRO] [FALLBACK] {nome} | fallback indisponível: {exc}")
        return False


def try_send_inmail(page: Page, lead: Dict[str, str]) -> bool:
    nome = str(lead.get("nome") or "").strip() or "contato"
    empresa = str(lead.get("empresa") or "N/D").strip() or "N/D"
    url = normalize_profile_url(str(lead.get("profile_url") or ""), str(lead.get("public_id") or ""))
    if not url:
        log(f"Nome: {nome} | Status: erro | motivo=sem profile_url/public_id")
        return False

    for attempt in range(1, 3):
        try:
            action_t0 = _now_ms()
            page.goto(url, wait_until="domcontentloaded", timeout=25000)
            page.wait_for_timeout(random.randint(1000, 1500))
            current_url = str(page.url or "").lower()
            if "linkedin.com/login" in current_url or "linkedin.com/authwall" in current_url:
                log(f"Nome: {nome} | Perfil: {url} | Status: erro | sessÃ£o nÃ£o autenticada")
                return False
            opened_composer = False
            button = find_inmail_button(page)
            if button is not None:
                label = " ".join((button.inner_text() or "").split()).strip().lower()
                is_explicit_inmail = "inmail" in label
                if is_explicit_inmail:
                    button.scroll_into_view_if_needed()
                    button.click()
                    opened_composer = True
            if not opened_composer and try_open_inmail_via_more(page):
                opened_composer = True
            if not opened_composer and _is_sales_lead_url(url):
                message_button = find_message_button(page)
                if message_button is not None:
                    message_button.scroll_into_view_if_needed()
                    message_button.click()
                    if has_inmail_composer_marker(page):
                        opened_composer = True
                    else:
                        log(f"Nome: {nome} | Perfil: {url} | Status: [SKIP] composer sem marcador InMail")
                        return False
            if not opened_composer:
                log(f"Nome: {nome} | Perfil: {url} | Status: [SKIP] sem InMail disponÃ­vel")
                return False

            page.wait_for_timeout(random.randint(1000, 1500))

            subject_box = find_subject_box(page)
            if subject_box is None and not has_inmail_composer_marker(page):
                log(f"Nome: {nome} | Perfil: {url} | Status: [SKIP] sem confirmação de composer InMail")
                return False
            mensagem = compose_message(nome)
            if subject_box is not None:
                subject_box.click()
                subject_box.press_sequentially(f"Contato {nome.split()[0]}", delay=80)

            message_box = find_message_box(page)
            if message_box is None:
                raise RuntimeError("campo de mensagem nÃ£o encontrado")
            message_box.scroll_into_view_if_needed()
            message_box.click()
            message_box.press_sequentially(mensagem, delay=80)

            send_button = find_send_button(page)
            if send_button is not None and send_button.is_visible() and send_button.is_enabled():
                send_button.scroll_into_view_if_needed()
                send_button.click()
            else:
                page.keyboard.press("Enter")

            page.wait_for_timeout(random.randint(1000, 1500))
            total_ms = int(_now_ms() - action_t0)
            log(f"[LINKEDIN_FAST] ação executada=inmail | tempo_total_ms={total_ms}")
            log(f"Nome: {nome} | Perfil: {url} | Mensagem: {mensagem}")
            log(f"[INMAIL] enviado | Nome: {nome} | Perfil: {url}")
            log_result(nome, empresa, "inmail", "enviado")
            return True
        except Error as exc:
            log(f"Nome: {nome} | Perfil: {url} | tentativa={attempt} | erro playwright={exc}")
            _safe_fast_reset(page)
        except Exception as exc:
            log(f"Nome: {nome} | Perfil: {url} | tentativa={attempt} | erro={exc}")
            _safe_fast_reset(page)
    log(f"Nome: {nome} | Perfil: {url} | Status: erro")
    return False


def main() -> int:
    sent_registry = load_sent_registry()
    sent_count = 0
    processed_count = 0

    with sync_playwright() as playwright:
        page = get_existing_linkedin_page(playwright)
        if page is None:
            log("sessÃ£o Playwright existente nÃ£o encontrada; abortando sem login")
            return 1
        if "/sales/login" in str(page.url or "").lower():
            log("[ERRO] sessão ativa caiu em /sales/login; abortando sem tocar na conta")
            return 1
        reused_tab = "linkedin.com" in str(page.url or "").lower()
        _open_linkedin_fast(page, reused_tab=reused_tab)
        ensure_li_at_from_browser(page)

        leads = load_leads_carol(include_fallback=False)
        if not leads:
            leads = collect_leads_from_saved_list(page, list_name="LEADS CAROL", limit=60)
        if not leads:
            log("nenhum lead disponÃ­vel na lista LEADS CAROL")
            return 0

        for lead in leads:
            if sent_count >= MAX_TESTE:
                break
            if processed_count >= MAX_TESTE:
                break
            nome = str(lead.get("nome") or "").strip()
            profile_url = normalize_profile_url(str(lead.get("profile_url") or ""), str(lead.get("public_id") or ""))
            dedup_key = (profile_url or nome).strip().lower()
            if not dedup_key:
                continue
            if dedup_key in sent_registry:
                log(f"Nome: {nome or 'contato'} | Perfil: {profile_url} | Status: [SKIP] duplicado")
                continue
            processed_count += 1

            ok = try_send_inmail(page, lead)
            if not ok:
                ok = try_api_fallback(page, lead)
                if not ok:
                    log(f"[SKIP] Nome: {nome or 'contato'} | Perfil: {profile_url} | nenhuma opção possível")
                    continue
            mark_sent(dedup_key)
            sent_registry.add(dedup_key)
            sent_count += 1
            delay = random.randint(30, 90)
            log(f"delay humano pÃ³s-envio: {delay}s")
            time.sleep(delay)

    log(f"finalizado | enviados={sent_count} | limite={MAX_TESTE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

