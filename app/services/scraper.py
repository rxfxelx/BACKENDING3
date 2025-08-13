import asyncio, urllib.parse, re
from typing import AsyncGenerator, Iterable, List, Tuple
from playwright.async_api import async_playwright
from ..config import settings
from ..utils.phone import extract_phones_from_text

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"

async def _extract_phones_from_page(page) -> list[str]:
    # Try to get text from the 'local pack' cards and the full content
    txts = []
    try:
        # Grab visible text
        txts.append(await page.content())
        body = await page.locator("body").inner_text()
        txts.append(body)
    except Exception:
        pass
    text = "\n".join([t for t in txts if t])
    phones = extract_phones_from_text(text)
    return phones

async def search_numbers(nicho: str, locais: list[str], target: int) -> AsyncGenerator[str, None]:
    # Streams unique phones as found
    seen = set()
    async with async_playwright() as p:
        browser = await getattr(p, settings.BROWSER).launch(headless=settings.HEADLESS)
        context = await browser.new_context(user_agent=settings.USER_AGENT, viewport={"width":1280,"height":900})
        page = await context.new_page()
        try:
            for local in locais:
                pages_without_new = 0
                for idx in range(settings.MAX_PAGES_PER_QUERY):
                    start = idx * settings.PAGE_SIZE
                    q = urllib.parse.quote_plus(f"{nicho.strip()} {local.strip()}")
                    url = SEARCH_FMT.format(q=q, start=start)
                    await page.goto(url, wait_until="networkidle", timeout=45000)
                    await page.wait_for_timeout(800)
                    phones = await _extract_phones_from_page(page)
                    new = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new += 1
                            yield ph
                            if len(seen) >= target * 3:  # cap raw scraping to 3x target to control cost
                                return
                    if new == 0:
                        pages_without_new += 1
                    else:
                        pages_without_new = 0
                    if pages_without_new >= 3:
                        break  # likely exhausted
        finally:
            await context.close()
            await browser.close()
