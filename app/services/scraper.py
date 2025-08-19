# app/services/scraper.py
import random
import urllib.parse
import base64
import unicodedata
from typing import AsyncGenerator, List, Set

from playwright.async_api import (
    async_playwright,
    TimeoutError as PWTimeoutError,
    Error as PWError,
)
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={query}&start={start}{uule}"

RESULT_CONTAINERS = [
    ".rlfl__tls",
    ".VkpGBb",
    ".rllt__details",
    ".rllt__wrapped",
    "div[role='article']",
    "#search",
    "div[role='main']",
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
    """Pequenos movimentos/scroll com variação de tempo para reduzir padrão robótico."""
    try:
        await page.mouse.move(
            random.randint(30, 600),
            random.randint(50, 500),
            steps=random.randint(6, 14),
        )
        # pequenos scrolls, para cima e para baixo
        dy = random.randint(160, 300) * random.choice([1, 1, 1, -1])
        await page.evaluate(f"() => window.scrollBy(0, {dy});")
        await page.wait_for_timeout(random.randint(240, 560))
    except Exception:
        pass


async def _extract_phones_from_page(page) -> List[str]:
    phones: Set[str] = set()
    try:
        # href tel:
        hrefs = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.getAttribute('href'))",
        )
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""))
            if n:
                phones.add(n)

        # innerText dos links tel:
        texts = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.innerText || e.textContent || '')",
        )
        for t in texts or []:
            n = normalize_br(t)
            if n:
                phones.add(n)

        # blocos de resultados
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
    try:
        txt = (await page.content())[:100000].lower()
        if "/sorry/" in txt or "unusual traffic" in txt or "recaptcha" in txt or "g-recaptcha" in txt:
            return True
        sel_hit = await page.locator("form[action*='/sorry'], iframe[src*='recaptcha'], #recaptcha").count()
        return sel_hit > 0
    except Exception:
        return False


def _cooldown_secs(hit: int) -> int:
    base = 20  # 20s
    mx = 120   # 2 min
    return min(mx, int(base * (1.6 ** max(0, hit - 1))) + random.randint(0, 10))


async def search_numbers(
    nicho: str,
    locais: List[str],
    target: int,
    *,
    max_pages: int | None = None,  # mantido por compat
) -> AsyncGenerator[str, None]:
    """
    Percorre o Google Local até atingir 'target' ou esgotar os termos/páginas.
    Anti-bloqueio leve (rota estática, consent, humanize, cooldown sob captcha).
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

        # Bloqueia recursos pesados que não ajudam (reduz detecção e acelera)
        async def _route_handler(route):
            rtype = route.request.resource_type
            if rtype in {"image", "media", "font", "stylesheet"}:
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", _route_handler)

        async def _ensure_page(page):
            if page is None or page.is_closed():
                return await context.new_page()
            return page

        page = await context.new_page()
        page.set_default_timeout(15000)

        try:
            total_yield = 0
            for local in locais:
                city = (local or "").strip()
                if not city:
                    continue
                uule = _uule_for_city(city)

                # termos (leva variações para driblar repetição)
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

                        start = idx * 20  # 0, 20, 40...
                        q = term
                        if captcha_hits_term > 0:
                            decorations = ["", " ", "  ", " ★", " ✔", " ✓"]
                            q = (term + random.choice(decorations)).strip()

                        url = SEARCH_FMT.format(
                            query=urllib.parse.quote_plus(q),
                            start=start,
                            uule=uule,
                        )

                        # navegação com pequena robustez (reabre página se necessário)
                        try:
                            page = await _ensure_page(page)
                            await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                        except (PWTimeoutError, PWError, Exception):
                            idx += 1
                            continue

                        await _try_accept_consent(page)
                        await _humanize(page)

                        # captcha / sorry
                        if await _is_captcha_or_sorry(page):
                            captcha_hits_term += 1
                            captcha_hits_global += 1
                            await page.wait_for_timeout(_cooldown_secs(captcha_hits_global) * 1000)
                            if captcha_hits_term >= 2:
                                break  # pula este termo
                            idx += 1
                            continue

                        # aguarda algum bloco/telefone
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

                        # jitter mais variado conforme aprofunda
                        wait_ms = random.randint(240, 560) + min(
                            1600, int(idx * 42 + random.randint(120, 260))
                        )
                        await page.wait_for_timeout(wait_ms)
                        idx += 1
        finally:
            try:
                await context.close()
            finally:
                await browser.close()


# Compat: o main.py chama isso no shutdown; aqui é no-op porque já fechamos no finally.
async def shutdown_playwright() -> None:
    return
