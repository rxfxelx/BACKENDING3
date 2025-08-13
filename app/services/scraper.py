import urllib.parse
from typing import AsyncGenerator, List, Set
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"

# Containers típicos dos cartões do Google Local
RESULT_CONTAINERS = [
    ".rlfl__tls",         # lista de locais
    ".VkpGBb",            # cartão
    ".rllt__details",     # detalhes
    ".rllt__wrapped",     # detalhes alternativo
    "div[role='article']",# cartão acessível
    "#search",
    "div[role='main']",
]

CONSENT_BUTTONS = [
    "button:has-text('Aceitar tudo')",
    "button:has-text('Concordo')",
    "button:has-text('Aceitar')",
    "button#L2AGLb",  # id clássico de consent
    "button:has-text('I agree')",
    "button:has-text('Accept all')",
]

async def _try_accept_consent(page) -> None:
    try:
        for sel in CONSENT_BUTTONS:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    if await loc.first.is_visible():
                        await loc.first.click()
                        await page.wait_for_timeout(400)
                        break
            except Exception:
                continue
    except Exception:
        pass

async def _extract_phones_from_page(page) -> List[str]:
    """
    1) Coleta links tel: (href e texto).
    2) Fallback: extrai telefones **apenas** de containers dos cartões locais.
    Nunca usa o body inteiro. Nunca gera número.
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

        # 2) fallback estrito em containers
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
    Pagina 0,20,40... por local. Emite números únicos.
    Permite coletar até ~4x o target para dar margem à verificação WhatsApp.
    """
    seen: Set[str] = set()
    max_raw = max(target * 4, target + 20)

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

                    await page.goto(url, wait_until="networkidle", timeout=45000)
                    await _try_accept_consent(page)

                    # Espera algum container de resultado ou tel:
                    try:
                        await page.wait_for_selector(
                            ",".join(["a[href^='tel:']"] + RESULT_CONTAINERS),
                            timeout=6000
                        )
                    except PWTimeoutError:
                        # segue mesmo assim; algumas páginas rendem rápido
                        pass

                    await page.wait_for_timeout(700)

                    phones = await _extract_phones_from_page(page)
                    new = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new += 1
                            yield ph
                            if len(seen) >= max_raw:
                                return

                    empty_pages = empty_pages + 1 if new == 0 else 0
                    if empty_pages >= 5:
                        # 5 páginas seguidas sem novos números neste local → esgotou
                        break
        finally:
            await context.close()
            await browser.close()
