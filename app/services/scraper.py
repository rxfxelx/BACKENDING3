# app/services/scraper.py
import random
import urllib.parse
import base64
import unicodedata
from contextlib import suppress
from typing import AsyncGenerator, List, Set

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

# Google Local (SERP local)
SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={query}&start={start}{uule}"

# Áreas onde costumam estar os cards/resultados
RESULT_CONTAINERS = [
    ".rlfl__tls", ".VkpGBb", ".rllt__details", ".rllt__wrapped",
    "div[role='article']", "#search", "div[role='main']",
]

# Botões comuns de consentimento (GDPR)
CONSENT_BUTTONS = [
    "button#L2AGLb",
    "button:has-text('Aceitar tudo')",
    "button:has-text('Concordo')",
    "button:has-text('Aceitar')",
    "button:has-text('I agree')",
    "button:has-text('Accept all')",
]

# Alguns UAs reais
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]


def _norm_ascii(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))


def _uule_for_city(city: str) -> str:
    """UULE fixa a geolocalização da consulta."""
    c = (city or "").strip()
    if not c:
        return ""
    if "," not in c:
        c = f"{c},Brazil"
    b64 = base64.b64encode(c.encode("utf-8")).decode("ascii")
    return "&uule=" + urllib.parse.quote("w+CAIQICI" + b64, safe="")


async def _try_accept_consent(page) -> None:
    try:
        for sel in CONSENT_BUTTONS:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.click()
                await page.wait_for_timeout(250)
                break
    except Exception:
        pass


async def _humanize(page) -> None:
    """Movimentos e scroll leves para reduzir padrão robótico."""
    try:
        await page.mouse.move(
            random.randint(50, 400),
            random.randint(60, 300),
            steps=random.randint(5, 12),
        )
        await page.evaluate("() => { window.scrollBy(0, Math.floor(160 + Math.random()*260)); }")
        await page.wait_for_timeout(random.randint(200, 480))
    except Exception:
        pass


async def _extract_phones_from_page(page) -> List[str]:
    phones: Set[str] = set()
    try:
        # tel: em href
        hrefs = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.getAttribute('href'))",
        )
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""))
            if n:
                phones.add(n)

        # texto do link tel:
        texts = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.innerText || e.textContent || '')",
        )
        for t in texts or []:
            n = normalize_br(t)
            if n:
                phones.add(n)

        # blocos de texto onde o número pode aparecer
        for sel in RESULT_CONTAINERS:
            try:
                blocks = await page.eval_on_selector_all(
                    sel, "els => els.map(e => e.innerText || e.textContent || '')"
                )
                for block in blocks or []:
                    for n in extract_phones_from_text(block):
                        phones.add(n)
            except Exception:
                continue
    except Exception:
        pass
    return list(phones)


def _city_variants(city: str) -> List[str]:
    c = (city or "").strip()
    base = [c, f"{c} MG", f"{c}, MG"]
    no_acc = list({_norm_ascii(x) for x in base})
    variants = base + [f"em {x}" for x in base] + no_acc + [f"em {x}" for x in no_acc]
    return list(dict.fromkeys(variants))


async def _is_captcha_or_sorry(page) -> bool:
    """Heurísticas simples para recaptcha/‘unusual traffic’."""
    try:
        txt = (await page.content())[:100000].lower()
        if "/sorry/" in txt or "unusual traffic" in txt or "recaptcha" in txt or "g-recaptcha" in txt:
            return True
        sel_hit = await page.locator("form[action*='/sorry'], iframe[src*='recaptcha'], #recaptcha").count()
        return sel_hit > 0
    except Exception:
        return False


def _cooldown_secs(hit: int) -> int:
    """Backoff progressivo, mas curto para não travar o fluxo."""
    base = 20  # 20s
    mx = 120   # até 2 min
    wait = min(mx, int(base * (1.6 ** max(0, hit - 1))) + random.randint(0, 10))
    return wait


async def search_numbers(
    nicho: str,
    locais: List[str],
    target: int,
    *,
    max_pages: int | None = None,
) -> AsyncGenerator[str, None]:
    """
    Pagina Google Local até esgotar ou atingir 'target'.
    Anti-bloqueio:
      - bloqueia imagens/fontes/CSS
      - aceita consent
      - humanize leve
      - detecta captcha/sorry e aplica cooldown + leve variação do termo
    """
    seen: Set[str] = set()
    q_base = (nicho or "").strip()
    empty_limit = int(getattr(settings, "MAX_EMPTY_PAGES", 8))
    captcha_hits_global = 0

    async with async_playwright() as p:
        browser = await getattr(p, settings.BROWSER).launch(headless=settings.HEADLESS)
        ua = settings.USER_AGENT or random.choice(UA_POOL)
        context = await browser.new_context(
            user_agent=ua,
            locale="pt-BR",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"},
            viewport={"width": 1280, "height": 900},
        )

        # Bloqueia recursos pesados (acelera e reduz detecção)
        async def _route_handler(route):
            rtype = route.request.resource_type
            if rtype in {"image", "media", "font", "stylesheet"}:
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", _route_handler)

        page = await context.new_page()
        page.set_default_timeout(15000)

        try:
            total_yield = 0
            for local in locais:
                city = (local or "").strip()
                if not city:
                    continue
                uule = _uule_for_city(city)

                # termos com pequenas variações
                terms: List[str] = []
                for v in _city_variants(city):
                    t = f"{q_base} {v}".strip()
                    if t not in terms:
                        terms.append(t)

                for term in terms:
                    empty_pages = 0
                    idx = 0
                    captcha_hits_term = 0

                    while True:
                        if target and total_yield >= target:
                            return

                        start = idx * 20  # 0,20,40...
                        q = term
                        # pequena aleatoriedade no termo sob bloqueio
                        if captcha_hits_term > 0:
                            decorations = ["", " ", "  ", " ★", " ✔", " ✓"]
                            q = (term + random.choice(decorations)).strip()

                        url = SEARCH_FMT.format(
                            query=urllib.parse.quote_plus(q),
                            start=start,
                            uule=uule,
                        )

                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                        except Exception:
                            idx += 1
                            continue

                        await _try_accept_consent(page)
                        await _humanize(page)

                        # CAPTCHA/sorry handling
                        if await _is_captcha_or_sorry(page):
                            captcha_hits_term += 1
                            captcha_hits_global += 1
                            wait_s = _cooldown_secs(captcha_hits_global)
                            await page.wait_for_timeout(wait_s * 1000)
                            if captcha_hits_term >= 2:
                                break  # pula este termo
                            idx += 1
                            continue

                        # aguarda elementos ou segue
                        try:
                            await page.wait_for_selector(
                                "a[href^='tel:']," + ",".join(RESULT_CONTAINERS),
                                timeout=6000,
                            )
                        except PWTimeoutError:
                            pass

                        phones = await _extract_phones_from_page(page)

                        new = 0
                        for ph in phones:
                            if ph not in seen:
                                seen.add(ph)
                                new += 1
                                total_yield += 1
                                yield ph
                                if target and total_yield >= target:
                                    return

                        empty_pages = empty_pages + 1 if new == 0 else 0
                        if empty_pages >= empty_limit:
                            break  # esgotou este termo

                        # jitter crescente quanto mais fundo
                        await page.wait_for_timeout(
                            180 + min(1200, int(idx * 35 + random.randint(80, 180)))
                        )
                        idx += 1
        finally:
            # Fechamento em ordem para evitar TargetClosedError de rotas pendentes
            with suppress(Exception):
                await context.unroute("**/*")
            with suppress(Exception):
                await page.close()
            with suppress(Exception):
                await context.close()
            with suppress(Exception):
                await browser.close()


# Mantido para compatibilidade com import opcional no main
async def shutdown_playwright():
    """Compat: nada a fazer; recursos são fechados no finally do search."""
    return
