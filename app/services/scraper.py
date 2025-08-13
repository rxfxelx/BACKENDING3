import urllib.parse
from typing import AsyncGenerator, List, Set
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"

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

async def _try_accept_consent(page) -> None:
    try:
        for sel in CONSENT_BUTTONS:
            loc = page.locator(sel)
            if await loc.count() > 0:
                if await loc.first.is_visible():
                    await loc.first.click()
                    await page.wait_for_timeout(300)
                    break
    except Exception:
        pass

async def _extract_phones_from_page(page) -> List[str]:
    """
    1) Coleta tel: (href e texto).
    2) Fallback: extrai telefones apenas de containers de cartões locais.
    Nunca usa body inteiro. Nunca gera número.
    """
    phones: Set[str] = set()
    try:
        hrefs = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.getAttribute('href'))"
        )
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""))
            if n:
                phones.add(n)

        texts = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.innerText || e.textContent || '')"
        )
        for t in texts or []:
            n = normalize_br(t)
            if n:
                phones.add(n)

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

async def search_numbers(nicho: str, locais: List[str], target: int) -> AsyncGenerator[str, None]:
    """
    Pagina 0,20,40... por local. Emite números únicos até esgotar.
    NÃO corta por meta. Quem decide parar é a orquestração no main.
    """
    seen: Set[str] = set()

    async with async_playwright() as p:
        browser = await getattr(p, settings.BROWSER).launch(headless=settings.HEADLESS)
        context = await browser.new_context(
            user_agent=settings.USER_AGENT,
            locale="pt-BR",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"},
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()
        try:
            for local in locais:
                empty_pages = 0
                for idx in range(settings.MAX_PAGES_PER_QUERY):
                    start = idx * settings.PAGE_SIZE
                    q = urllib.parse.quote_plus(f"{nicho.strip()} {local.strip()}")
                    url = SEARCH_FMT.format(q=q, start=start)

                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await _try_accept_consent(page)

                    try:
                        await page.wait_for_selector(
                            ",".join(["a[href^='tel:']"] + RESULT_CONTAINERS),
                            timeout=6000
                        )
                    except PWTimeoutError:
                        pass

                    await page.wait_for_timeout(600)

                    phones = await _extract_phones_from_page(page)
                    new = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new += 1
                            yield ph

                    empty_pages = empty_pages + 1 if new == 0 else 0
                    if empty_pages >= 5:
                        break  # esgotado neste local
        finally:
            await context.close()
            await browser.close()
