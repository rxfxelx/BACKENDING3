import urllib.parse
from typing import AsyncGenerator, List, Set
from playwright.async_api import async_playwright
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"

async def _extract_phones_from_page(page) -> List[str]:
    """
    Extrai apenas telefones que aparecem como links tel: na página do Google Local.
    Evita capturar números aleatórios de outras partes do HTML.
    """
    phones: Set[str] = set()
    try:
        # href tel:
        hrefs = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.getAttribute('href'))"
        )
        for h in hrefs or []:
            if not h:
                continue
            # h ex: "tel:+55 31 99999-9999"
            norm = normalize_br(h.replace("tel:", ""))
            if norm:
                phones.add(norm)

        # textos visíveis dos tel:
        texts = await page.eval_on_selector_all(
            "a[href^='tel:']",
            "els => els.map(e => e.innerText || e.textContent || '')"
        )
        for t in texts or []:
            norm = normalize_br(t)
            if norm:
                phones.add(norm)

        # fallback leve: às vezes o tel não vem como link, mas o rótulo vem junto
        # Limitado ao container principal para reduzir lixo
        try:
            container_text = await page.locator("body").inner_text()
            for p in extract_phones_from_text(container_text):
                phones.add(p)
        except Exception:
            pass

    except Exception:
        pass

    return list(phones)

async def search_numbers(nicho: str, locais: List[str], target: int) -> AsyncGenerator[str, None]:
    """
    Stream de números únicos encontrados. Pagina 0,20,40... por local.
    Não gera nada. Só entrega o que existe em tel: do Google Local.
    """
    seen: Set[str] = set()

    async with async_playwright() as p:
        browser = await getattr(p, settings.BROWSER).launch(headless=settings.HEADLESS)
        context = await browser.new_context(
            user_agent=settings.USER_AGENT,
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()
        try:
            for local in locais:
                pages_without_new = 0
                for idx in range(settings.MAX_PAGES_PER_QUERY):
                    start = idx * settings.PAGE_SIZE
                    q = urllib.parse.quote_plus(f"{nicho.strip()} {local.strip()}")
                    url = SEARCH_FMT.format(q=q, start=start)

                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    # pequena espera para render dos cartões
                    await page.wait_for_timeout(700)

                    phones = await _extract_phones_from_page(page)

                    new_found = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new_found += 1
                            yield ph
                            # Não precisamos coletar “muito além” do alvo.
                            # O cap evita excesso e reduz risco de fallback duplicar.
                            if len(seen) >= max(target, 1) * 2:
                                return

                    pages_without_new = pages_without_new + 1 if new_found == 0 else 0
                    if pages_without_new >= 3:
                        # Provável esgotado para esse local
                        break
        finally:
            await context.close()
            await browser.close()
