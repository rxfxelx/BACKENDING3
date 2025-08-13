import unicodedata, random, urllib.parse, base64
from typing import AsyncGenerator, List, Set
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

# tbm=lcl + paginação correta (start=0,20,40...)
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit(537.36) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

def _norm_ascii(s: str) -> str:
    import unicodedata as u
    return "".join(ch for ch in u.normalize("NFKD", s or "") if not u.combining(ch))

def _uule_for_city(city: str) -> str:
    """
    UULE trava a geolocalização da SERP local (tbm=lcl).
    """
    canonical = city.strip()
    if "," not in canonical:
        # se não veio UF/país, completa genericamente
        canonical = f"{canonical},Brazil"
    b64 = base64.b64encode(canonical.encode("utf-8")).decode("ascii")
    return "&uule=" + urllib.parse.quote("w+CAIQICI" + b64, safe="")

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
    # anti-CAPTCHA leve
    try:
        await page.mouse.move(random.randint(50, 400), random.randint(60, 300), steps=random.randint(5, 15))
        await page.evaluate("() => { window.scrollBy(0, Math.floor(200 + Math.random()*300)); }")
        await page.wait_for_timeout(random.randint(250, 600))
    except Exception:
        pass

async def _extract_phones_from_page(page) -> List[str]:
    """
    1) tel: (href e texto)
    2) regex apenas nos containers dos cartões locais
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

def _city_variants(city: str) -> List[str]:
    c = city.strip()
    base = [c, f"{c} MG", f"{c}, MG"]
    no_acc = list({ _norm_ascii(x) for x in base })
    # inclui versões com "em ..."
    variants = base + [f"em {x}" for x in base] + no_acc + [f"em {x}" for x in no_acc]
    # remove duplicatas preservando ordem
    return list(dict.fromkeys(variants))

async def search_numbers(nicho: str, locais: List[str], target: int, *, max_pages: int | None = None) -> AsyncGenerator[str, None]:
    """
    Sempre usa tbm=lcl. Pagina por start=0,20,40...
    Gera variantes por cidade e aplica UULE.
    Não corta por meta: quem decide parar é a orquestração.
    """
    seen: Set[str] = set()
    q_base = (nicho or "").strip()
    pages = max_pages or settings.MAX_PAGES_PER_QUERY

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
                uule = _uule_for_city(city)

                terms: List[str] = []
                for v in _city_variants(city):
                    t = f"{q_base} {v}".strip()
                    if t not in terms:
                        terms.append(t)

                for term in terms:
                    empty_pages = 0
                    for idx in range(pages):
                        start = idx * 20  # Google Local = múltiplos de 20
                        url = SEARCH_FMT.format(query=urllib.parse.quote_plus(term), start=start, uule=uule)

                        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                        await _try_accept_consent(page)
                        await _humanize(page)

                        try:
                            await page.wait_for_selector("a[href^='tel:']," + ",".join(RESULT_CONTAINERS), timeout=7000)
                        except PWTimeoutError:
                            pass

                        await page.wait_for_timeout(random.randint(350, 800))
                        phones = await _extract_phones_from_page(page)

                        new = 0
                        for ph in phones:
                            if ph not in seen:
                                seen.add(ph)
                                new += 1
                                yield ph

                        empty_pages = empty_pages + 1 if new == 0 else 0
                        if empty_pages >= 5:
                            break  # esgotou para este termo

                        await page.wait_for_timeout(random.randint(300, 700))
        finally:
            await context.close()
            await browser.close()
