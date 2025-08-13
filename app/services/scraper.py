import urllib.parse
from typing import AsyncGenerator, List, Set
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"

# Seletores dos cartões do Google Local
CARD_SELECTORS = [
    "a[href^='tel:']",
    "div[role='article']",
    ".VkpGBb",
    ".rllt__details",
    ".rllt__wrapped",
    ".rlfl__tls",
]

async def _extract_phones_from_page(page) -> List[str]:
    """
    1) Coleta links tel:
    2) Fallback: extrai telefones APENAS dos containers de cartões locais.
    Nunca usa o body inteiro. Nunca gera números.
    """
    phones: Set[str] = set()
    try:
        # 1) href tel:
        hrefs = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.getAttribute('href'))"
        )
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""))
            if n:
                phones.add(n)

        # 1b) textos de links tel:
        texts = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.innerText || e.textContent || '')"
        )
        for t in texts or []:
            n = normalize_br(t)
            if n:
                phones.add(n)

        # 2) fallback focado nos cartões
        for sel in ["div[role='article']", ".VkpGBb", ".rllt__details", ".rllt__wrapped", ".rlfl__tls"]:
            try:
                blocks = await page.eval_on_selector_all(
                    sel, "els => els.map(e => e.innerText || e.textContent || '')"
                )
                for block in blocks or []:
                    for n in extract_phones_from_text(block):
                        phones.add(n)
            except Exception:
                pass

    except Exception:
        pass

    return list(phones)

async def search_numbers(nicho: str, locais: List[str], target: int) -> AsyncGenerator[str, None]:
    """
    Pagina 0,20,40... por local. Emite números únicos e para ao atingir `target`.
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

                    # Tenta aceitar consentimento se aparecer
                    try:
                        btn = page.locator("button:has-text('Concordo')")
                        if await btn.count() > 0:
                            await btn.first.click()
                    except Exception:
                        pass

                    # Espera algum container de resultado ou tel:
                    try:
                        await page.wait_for_selector(",".join(CARD_SELECTORS), timeout=5000)
                    except PWTimeoutError:
                        pass

                    await page.wait_for_timeout(700)

                    phones = await _extract_phones_from_page(page)
                    new = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new += 1
                            yield ph
                            if len(seen) >= target:
                                return

                    empty_pages = empty_pages + 1 if new == 0 else 0
                    if empty_pages >= 3:
                        break  # esgotado neste local
        finally:
            await context.close()
            await browser.close()
