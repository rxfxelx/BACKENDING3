import unicodedata, random, urllib.parse
from typing import AsyncGenerator, List, Set
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={query}&start={start}"

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
    # pool básico de UAs; usado só se settings.USER_AGENT estiver vazio
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

def _norm_ascii(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

async def _try_accept_consent(page) -> None:
    try:
        for sel in CONSENT_BUTTONS:
            loc = page.locator(sel)
            if await loc.count() > 0 and await loc.first.is_visible():
                await loc.first.click()
                await page.wait_for_timeout(300)
                break
    except Exception:
        pass

async def _humanize(page) -> None:
    try:
        # movimentos leves do mouse
        await page.mouse.move(random.randint(50, 400), random.randint(60, 300), steps=random.randint(5, 15))
        # pequenos scrolls
        await page.evaluate("() => { window.scrollBy(0, Math.floor(200 + Math.random()*300)); }")
        await page.wait_for_timeout(random.randint(250, 600))
    except Exception:
        pass

async def _extract_phones_from_page(page) -> List[str]:
    """
    1) Captura tel: (href e texto)
    2) Fallback: regex SOMENTE dentro dos containers dos cartões locais
    """
    phones: Set[str] = set()
    try:
        hrefs = await page.eval_on_selector_all("a[href^='tel:']", "els => els.map(e => e.getAttribute('href'))")
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""))
            if n: phones.add(n)

        texts = await page.eval_on_selector_all("a[href^='tel:']", "els => els.map(e => e.innerText || e.textContent || '')")
        for t in texts or []:
            n = normalize_br(t)
            if n: phones.add(n)

        for sel in RESULT_CONTAINERS:
            try:
                blocks = await page.eval_on_selector_all(sel, "els => els.map(e => e.innerText || e.textContent || '')")
                for block in blocks or []:
                    for n in extract_phones_from_text(block):
                        phones.add(n)
            except Exception:
                continue
    except Exception:
        pass
    return list(phones)

async def search_numbers(nicho: str, locais: List[str], target: int) -> AsyncGenerator[str, None]:
    """
    Pagina 0,20,40... por local. Emite números únicos. Não corta por meta.
    """
    seen: Set[str] = set()
    q_base = (nicho or "").strip()

    async with async_playwright() as p:
        browser = await getattr(p, settings.BROWSER).launch(headless=settings.HEADLESS)
        ua = settings.USER_AGENT or random.choice(UA_POOL)
        context = await browser.new_context(
            user_agent=ua,
            locale="pt-BR",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"},
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()
        try:
            for local in locais:
                city = (local or "").strip()
                if not city:
                    continue
                empty_pages = 0
                for idx in range(settings.MAX_PAGES_PER_QUERY):
                    start = idx * settings.PAGE_SIZE
                    term = f"{q_base} {city}".strip()
                    # tentativa 1: termo original
                    query = urllib.parse.quote_plus(term)
                    url = SEARCH_FMT.format(query=query, start=start)

                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await _try_accept_consent(page)
                    await _humanize(page)

                    try:
                        await page.wait_for_selector(",".join(["a[href^='tel:']"] + RESULT_CONTAINERS), timeout=6000)
                    except PWTimeoutError:
                        pass

                    await page.wait_for_timeout(random.randint(350, 800))
                    phones = await _extract_phones_from_page(page)

                    # se não achou nada, tenta fallback com termo sem acento
                    if not phones and idx == 0:
                        try:
                            term2 = _norm_ascii(term)
                            if term2 and term2 != term:
                                url2 = SEARCH_FMT.format(query=urllib.parse.quote_plus(term2), start=start)
                                await page.goto(url2, wait_until="domcontentloaded", timeout=45000)
                                await _humanize(page)
                                await page.wait_for_timeout(random.randint(300, 700))
                                phones = await _extract_phones_from_page(page)
                        except Exception:
                            pass

                    new = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new += 1
                            yield ph
                    empty_pages = empty_pages + 1 if new == 0 else 0
                    if empty_pages >= 5:
                        break  # esgotado neste local
                    # jitter leve entre páginas
                    await page.wait_for_timeout(random.randint(300, 700))
        finally:
            await context.close()
            await browser.close()
