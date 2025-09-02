# app/services/scraper.py
import random
import urllib.parse
import base64
import unicodedata
from typing import AsyncGenerator, List, Set, Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from ..config import settings
from ..utils.phone import extract_phones_from_text, normalize_br

SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={query}&start={start}{uule}"

RESULT_CONTAINERS = [
    ".rlfl__tls", ".VkpGBb", ".rllt__details", ".rllt__wrapped",
    "div[role='article']", "#search", "div[role='main']", "#rhs", ".kp-wholepage",
]

LISTING_LINK_SELECTORS = [
    "a[href*='/local/place']",
    "a[href*='://www.google.com/local/place']",
    "a[href*='://www.google.com/maps/place']",
    "a[href^='https://www.google.com/maps/place']",
    "a[href*='ludocid=']",
    "a[href*='/search?'][href*='ludocid']",
    "a[href*='/search?'][href*='lrd=']",
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
    "Mozilla/5.0 (Linux; Android 13; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
]

# ---------- utils ----------
def _norm_ascii(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

def _clean_query(s: str) -> str:
    s = (s or "").strip()
    return " ".join(s.split())

def _quoted_variants(q: str) -> List[str]:
    out = [q]
    if " " in q:
        out.append(f'"{q}"')
    if q.endswith("s"):
        out.append(q[:-1])
    return list(dict.fromkeys(out))

def _niche_variants(q: str) -> List[str]:
    q = _clean_query(q)
    base = [q]
    synonyms = {
        "restaurantes veganos": ["restaurante vegano", "comida vegana", "vegano"],
        "distribuidores de alimentos": ["distribuidora de alimentos", "atacadista de alimentos", "atacado de alimentos"],
        "médico": ["médicos", "consultório médico", "clínica"],
    }
    for k, alts in synonyms.items():
        if k in q.lower():
            base += alts
    out: List[str] = []
    for b in base:
        out += _quoted_variants(b)
    return list(dict.fromkeys([_clean_query(x) for x in out if x.strip()]))

def _city_alias(city: str) -> str:
    c = (city or "").strip()
    lower = c.lower()
    aliases = {
        "bh": "Belo Horizonte, MG",
        "belo horizonte": "Belo Horizonte, MG",
        "sp": "São Paulo, SP",
        "rj": "Rio de Janeiro, RJ",
        "poa": "Porto Alegre, RS",
        "sampa": "São Paulo, SP",
    }
    return aliases.get(lower, c)

def _uule_for_city(city: str) -> str:
    c = _city_alias(city)
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
    try:
        await page.mouse.move(random.randint(40, 420), random.randint(60, 320), steps=random.randint(6, 14))
        await page.evaluate("() => { window.scrollBy(0, Math.floor(180 + Math.random()*280)); }")
        await page.wait_for_timeout(random.randint(260, 520))
    except Exception:
        pass

async def _extract_phones_from_page(page) -> List[str]:
    phones: Set[str] = set()
    try:
        hrefs = await page.eval_on_selector_all("a[href^='tel:']", "els => els.map(e => e.getAttribute('href'))")
        for h in hrefs or []:
            n = normalize_br((h or "").replace("tel:", ""))
            if n:
                phones.add(n)
        texts = await page.eval_on_selector_all("a[href^='tel:']", "els => els.map(e => e.innerText || e.textContent || '')")
        for t in texts or []:
            n = normalize_br(t)
            if n:
                phones.add(n)
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
    c = _city_alias(city)
    base = [c, f"{c} MG", f"{c}, MG"]
    no_acc = list({_norm_ascii(x) for x in base})
    variants = base + [f"em {x}" for x in base] + no_acc + [f"em {x}" for x in no_acc]
    return list(dict.fromkeys(variants))

async def _is_captcha_or_sorry(page) -> bool:
    try:
        txt = (await page.content())[:120000].lower()
        if "/sorry/" in txt or "unusual traffic" in txt or "recaptcha" in txt or "g-recaptcha" in txt:
            return True
        sel_hit = await page.locator("form[action*='/sorry'], iframe[src*='recaptcha'], #recaptcha").count()
        return sel_hit > 0
    except Exception:
        return False

def _cooldown_secs(hit: int) -> int:
    base = 18
    mx = 110
    return min(mx, int(base * (1.6 ** max(0, hit - 1))) + random.randint(0, 9))

# ---------- Playwright singleton ----------
_pw = None
_browser = None
_context = None

async def _ensure_context():
    """
    Garante um browser/contexto único reutilizável.
    Evita abrir/fechar transporte em cada request (corta InvalidStateError).
    """
    global _pw, _browser, _context
    if _pw is None:
        _pw = await async_playwright().start()
    if _browser is None:
        launch_args = {
            "headless": settings.HEADLESS,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        }
        _browser = await getattr(_pw, settings.BROWSER).launch(**launch_args)
    if _context is None:
        ua = settings.USER_AGENT or random.choice(UA_POOL)
        _context = await _browser.new_context(
            user_agent=ua,
            locale="pt-BR",
            timezone_id=random.choice(["America/Sao_Paulo", "America/Bahia"]),
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"},
            viewport={"width": random.randint(1200, 1360), "height": random.randint(820, 920)},
        )
        await _context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
            window.chrome = { runtime: {} };
            """
        )
    return _context

# ---------- scraping ----------
async def _open_and_extract_from_listing(context, href: str, seen: Set[str]) -> List[str]:
    out: List[str] = []
    if not href:
        return out
    if href.startswith("/"):
        href = "https://www.google.com" + href

    page2 = await context.new_page()
    try:
        await page2.goto(href, wait_until="domcontentloaded", timeout=30000)
        for sel in [
            "button:has-text('Telefone')",
            "button:has-text('Ligar')",
            "a[aria-label^='Ligar']",
            "[aria-label*='Telefone']",
        ]:
            try:
                loc = page2.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click()
                    await page2.wait_for_timeout(350)
            except Exception:
                pass
        await page2.wait_for_timeout(1200)
        phones = await _extract_phones_from_page(page2)
        for ph in phones:
            if ph not in seen:
                seen.add(ph)
                out.append(ph)
    except Exception:
        pass
    finally:
        try:
            await page2.close()
        except Exception:
            pass
    return out

async def search_numbers(
    nicho: str,
    locais: List[str],
    target: int,
    *,
    max_pages: Optional[int] = None,
) -> AsyncGenerator[str, None]:
    """
    Varre Google Local até atingir o target ou esgotar resultados.
    Usa contexto compartilhado e fecha apenas a aba usada.
    """
    seen: Set[str] = set()
    q_base = _clean_query(nicho)
    empty_limit = int(getattr(settings, "MAX_EMPTY_PAGES", 14))
    captcha_hits_global = 0

    context = await _ensure_context()
    page = await context.new_page()
    page.set_default_timeout(20000)

    # NÃO usar route(); apenas seguir normal (evita tasks pendentes)
    try:
        total_yield = 0
        for local in locais:
            city = (local or "").strip()
            if not city:
                continue
            uule = _uule_for_city(city)

            terms: List[str] = []
            for v in _city_variants(city):
                for qv in _niche_variants(q_base):
                    t = f"{qv} {v}".strip()
                    if t and t not in terms:
                        terms.append(t)

            for term in terms:
                empty_pages = 0
                idx = 0
                captcha_hits_term = 0

                while True:
                    if target and total_yield >= target:
                        return
                    if max_pages is not None and idx >= max_pages:
                        break

                    start = idx * 20
                    q = term
                    if captcha_hits_term > 0:
                        decorations = ["", " ", "  ", " ★", " ✔", " ✓"]
                        q = (term + random.choice(decorations)).strip()

                    url = SEARCH_FMT.format(query=urllib.parse.quote_plus(q), start=start, uule=uule)

                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        idx += 1
                        continue

                    await _try_accept_consent(page)
                    await _humanize(page)

                    if await _is_captcha_or_sorry(page):
                        captcha_hits_term += 1
                        captcha_hits_global += 1
                        await page.wait_for_timeout(_cooldown_secs(captcha_hits_global) * 1000)
                        if captcha_hits_term >= 2:
                            break
                        idx += 1
                        continue

                    try:
                        await page.wait_for_selector("a[href^='tel:']," + ",".join(RESULT_CONTAINERS), timeout=8000)
                    except PWTimeoutError:
                        pass

                    phones = await _extract_phones_from_page(page)

                    if not phones:
                        try:
                            cards = page.locator(",".join(LISTING_LINK_SELECTORS))
                            count = await cards.count()
                            to_open = min(count, 22)
                            for i in range(to_open):
                                try:
                                    href = await cards.nth(i).get_attribute("href")
                                except Exception:
                                    href = None
                                extracted = await _open_and_extract_from_listing(context, href, seen)
                                phones.extend(extracted)
                                if len(phones) >= 25:
                                    break
                        except Exception:
                            pass

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
                        break

                    wait_ms = random.randint(320, 620) + min(1800, int(idx * 48 + random.randint(140, 300)))
                    await page.wait_for_timeout(wait_ms)
                    idx += 1
    finally:
        try:
            await page.close()
        except Exception:
            pass

# ---------- shutdown ----------
async def shutdown_playwright():
    global _pw, _browser, _context
    try:
        if _context:
            await _context.close()
    except Exception:
        pass
    finally:
        _context = None
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    finally:
        _browser = None
    try:
        if _pw:
            await _pw.stop()
    except Exception:
        pass
    finally:
        _pw = None
