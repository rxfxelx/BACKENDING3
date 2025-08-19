# app/services/scraper.py
import random
import urllib.parse
import base64
import unicodedata
import asyncio
from contextlib import suppress
from asyncio import CancelledError
from typing import AsyncGenerator, List, Set

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={query}&start={start}{uule}"

RESULT_CONTAINERS = [
    ".rlfl__tls", ".VkpGBb", ".rllt__details", ".rllt__wrapped",
    "div[role='article']", "#search", "div[role='main']",
]

CONSENT_BUTTONS = [
    "button#L2AGLb",
    "button:has-text('Aceitar tudo')",
    "button:has-text('Concordo')",
    "button:has-text('Aceitar')",
    "button:has-text('I agree')",
    "button:has-text('Accept all')",
]

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

def _norm_ascii(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

def _uule_for_city(city: str) -> str:
    c = (city or "").strip()
    if not c: return ""
    if "," not in c: c = f"{c},Brazil"
    b64 = base64.b64encode(c.encode("utf-8")).decode("ascii")
    return "&uule=" + urllib.parse.quote("w+CAIQICI" + b64, safe="")

async def _try_accept_consent(page) -> None:
    with suppress(Exception):
        for sel in CONSENT_BUTTONS:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.click()
                await page.wait_for_timeout(250)
                break

async def _humanize(page) -> None:
    with suppress(Exception):
        await page.mouse.move(random.randint(50, 400), random.randint(60, 300), steps=random.randint(5, 12))
        await page.evaluate("() => { window.scrollBy(0, Math.floor(160 + Math.random()*260)); }")
        await page.wait_for_timeout(random.randint(200, 480))

async def _extract_phones_from_page(page) -> List[str]:
    phones: Set[str] = set()
    with suppress(Exception):
        hrefs = await page.eval_on_selector_all("a[href^='tel:']", "els => els.map(e => e.getAttribute('href'))")
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""));  if n: phones.add(n)
        texts = await page.eval_on_selector_all("a[href^='tel:']", "els => els.map(e => e.innerText || e.textContent || '')")
        for t in texts or []:
            n = normalize_br(t);  if n: phones.add(n)
        for sel in RESULT_CONTAINERS:
            with suppress(Exception):
                blocks = await page.eval_on_selector_all(sel, "els => els.map(e => e.innerText || e.textContent || '')")
                for block in blocks or []:
                    for n in extract_phones_from_text(block):
                        phones.add(n)
    return list(phones)

def _city_variants(city: str) -> List[str]:
    c = (city or "").strip()
    base = [c, f"{c} MG", f"{c}, MG"]
    no_acc = list({_norm_ascii(x) for x in base})
    variants = base + [f"em {x}" for x in base] + no_acc + [f"em {x}" for x in no_acc]
    return list(dict.fromkeys(variants))

async def _is_captcha_or_sorry(page) -> bool:
    with suppress(Exception):
        txt = (await page.content())[:100000].lower()
        if "/sorry/" in txt or "unusual traffic" in txt or "recaptcha" in txt or "g-recaptcha" in txt:
            return True
        if await page.locator("form[action*='/sorry'], iframe[src*='recaptcha'], #recaptcha").count() > 0:
            return True
    return False

def _cooldown_secs(hit: int) -> int:
    base = 20; mx = 120
    return min(mx, int(base * (1.6 ** max(0, hit-1))) + random.randint(0, 10))

def _is_transport_closed_err(e: Exception) -> bool:
    m = str(e).lower()
    return ("handler is closed" in m) or ("target closed" in m) or ("browser has been closed" in m) or ("browser closed" in m) or ("writeunixtransport" in m)

async def _has_next(page) -> bool:
    with suppress(Exception):
        return await page.locator("a[aria-label='Próxima'], a#pnnext, a[aria-label='Next']").count() > 0
    return False

async def _safe_sleep(ms: int):
    with suppress(Exception):
        await asyncio.sleep(ms/1000)

async def search_numbers(
    nicho: str,
    locais: List[str],
    target: int,                 # ignorado para parada; varremos até acabar
    *,
    max_pages: int | None = None,
) -> AsyncGenerator[str, None]:
    seen: Set[str] = set()
    q_base = (nicho or "").strip()
    empty_limit = int(getattr(settings, "MAX_EMPTY_PAGES", 12))
    captcha_hits_global = 0

    try:
        async with async_playwright() as p:
            browser = await getattr(p, settings.BROWSER).launch(headless=settings.HEADLESS)
            ua = settings.USER_AGENT or random.choice(UA_POOL)
            context = await browser.new_context(
                user_agent=ua,
                locale="pt-BR",
                extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"},
                viewport={"width": 1280, "height": 900},
            )

            # Rotas só para domínios Google (evita ruído no fechamento)
            async def _route_handler(route):
                try:
                    r = route.request
                    u = r.url
                    if r.resource_type in {"image", "media", "font", "stylesheet"}:
                        await route.abort()
                        return
                    await route.continue_()
                except Exception as e:
                    if _is_transport_closed_err(e):
                        return
                # outras falhas ignoradas

            for patt in ["https://www.google.com/**", "https://consent.google.com/**", "https://www.gstatic.com/**"]:
                with suppress(Exception):
                    await context.route(patt, _route_handler)

            page = await context.new_page()
            page.set_default_timeout(15000)

            try:
                for local in locais:
                    city = (local or "").strip()
                    if not city:
                        continue
                    uule = _uule_for_city(city)

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
                            if max_pages is not None and idx >= max_pages:
                                break

                            start = idx * 20
                            q = term if captcha_hits_term == 0 else (term + random.choice(["", " ", "  ", " ★", " ✔", " ✓"])).strip()
                            url = SEARCH_FMT.format(query=urllib.parse.quote_plus(q), start=start, uule=uule)

                            try:
                                await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                            except Exception as e:
                                if _is_transport_closed_err(e): return
                                idx += 1; continue

                            with suppress(Exception):
                                await _try_accept_consent(page)
                                await _humanize(page)

                            with suppress(Exception):
                                if await _is_captcha_or_sorry(page):
                                    captcha_hits_term += 1; captcha_hits_global += 1
                                    await _safe_sleep(_cooldown_secs(captcha_hits_global) * 1000)
                                    if captcha_hits_term >= 2: break
                                    idx += 1; continue

                            try:
                                await page.wait_for_selector("a[href^='tel:']," + ",".join(RESULT_CONTAINERS), timeout=6000)
                            except PWTimeoutError:
                                pass
                            except Exception as e:
                                if _is_transport_closed_err(e): return

                            try:
                                phones = await _extract_phones_from_page(page)
                            except Exception as e:
                                if _is_transport_closed_err(e): return
                                phones = []

                            new = 0
                            for ph in phones:
                                if ph not in seen:
                                    seen.add(ph)
                                    new += 1
                                    yield ph

                            try:
                                has_next = await _has_next(page)
                            except Exception as e:
                                if _is_transport_closed_err(e): return
                                has_next = False

                            if new == 0 and not has_next:
                                break

                            empty_pages = empty_pages + 1 if new == 0 else 0
                            if empty_pages >= empty_limit:
                                break

                            await _safe_sleep(180 + min(1200, int(idx * 35 + random.randint(80, 180))))
                            idx += 1

            finally:
                # desmonta rotas antes de fechar
                for patt in ["https://www.google.com/**", "https://consent.google.com/**", "https://www.gstatic.com/**"]:
                    with suppress(Exception):
                        await context.unroute(patt)
                with suppress(Exception): await page.close()
                with suppress(Exception): await context.close()
                with suppress(Exception): await browser.close()
                await _safe_sleep(50)  # dá tempo para tasks internas finalizarem

    except CancelledError:
        return
    except Exception as e:
        if _is_transport_closed_err(e):
            return
        # demais erros: deixe subir se quiser logar
        return
