# app/services/scraper.py
import os
import re
import random
import base64
import urllib.parse
import unicodedata
import asyncio
from typing import AsyncGenerator, List, Set, Optional

import httpx

from ..utils.phone import extract_phones_from_text, normalize_br

# Google Local
SEARCH_FMT = "https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={query}&start={start}{uule}"

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

# Heurísticas simples de navegação/extração
TEL_HREF_RE = re.compile(r'href=["\']tel:\s*([^"\']+)["\']', re.I)
NEXT_RE = re.compile(r'aria-label=["\'](Próxima|Next)["\']|id=["\']pnnext["\']', re.I)
SORRY_RE = re.compile(r"unusual\s+traffic|recaptcha|g-recaptcha|/sorry/", re.I)
RESULT_HINT = [
    "rlfl__tls", "VkpGBb", "rllt__details", "rllt__wrapped",
    'role="article"', "role='article'", 'id="search"', "id='search'",
    'role="main"', "role='main'"
]

# ENV opcionais para tunar HTTP
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "1.6"))
BACKOFF_JITTER = int(os.getenv("HTTP_BACKOFF_JITTER_MS", "400"))

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

def _city_variants(city: str) -> List[str]:
    c = (city or "").strip()
    base = [c, f"{c} MG", f"{c}, MG"]
    no_acc = list({_norm_ascii(x) for x in base})
    variants = base + [f"em {x}" for x in base] + no_acc + [f"em {x}" for x in no_acc]
    out, seen = [], set()
    for v in variants:
        v = v.strip()
        if not v: continue
        if v not in seen:
            seen.add(v); out.append(v)
    return out

def _cooldown_secs(hit: int) -> int:
    base = 20; mx = 120
    return min(mx, int(base * (1.6 ** max(0, hit-1))) + random.randint(0, 10))

async def _sleep_ms(ms: int):
    await asyncio.sleep(ms / 1000)

def _extract_phones_from_html(html: str) -> List[str]:
    phones: Set[str] = set()
    # 1) href tel:
    for m in TEL_HREF_RE.finditer(html or ""):
        n = normalize_br(m.group(1) or "")
        if n: phones.add(n)
    # 2) texto cru limitado
    snippet = html if len(html) <= 400_000 else html[:400_000]
    for n in extract_phones_from_text(snippet):
        phones.add(n)
    return list(phones)

def _has_next(html: str) -> bool:
    return NEXT_RE.search(html or "") is not None

def _looks_like_results(html: str) -> bool:
    if not html: return False
    return any(tok in html for tok in RESULT_HINT)

async def _fetch_html(client: httpx.AsyncClient, url: str) -> Optional[str]:
    headers = {
        "User-Agent": random.choice(UA_POOL),
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    for i in range(max(1, HTTP_RETRIES)):
        try:
            r = await client.get(url, headers=headers)
            if r.status_code >= 500:
                await _sleep_ms(int(400 * (BACKOFF_BASE ** i)) + random.randint(0, BACKOFF_JITTER))
                continue
            return r.text
        except Exception:
            await _sleep_ms(int(400 * (BACKOFF_BASE ** i)) + random.randint(0, BACKOFF_JITTER))
            continue
    return None

async def search_numbers(
    nicho: str,
    locais: List[str],
    target: int,                 # usado para parar cedo quando atingir a meta
    *,
    max_pages: int | None = None,
) -> AsyncGenerator[str, None]:
    """
    Scraper HTTP puro para tbm=lcl.
    Varre até não ter próxima página OU atingir 'max_pages'.
    Para o chamador, você receberá números até atingir 'target' ou esgotar.
    """
    q_base = (nicho or "").strip()
    seen: Set[str] = set()
    yielded = 0
    captcha_hits_global = 0

    proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
    timeout = httpx.Timeout(HTTP_TIMEOUT)
    limits = httpx.Limits(max_keepalive_connections=4, max_connections=8)

    async with httpx.AsyncClient(
        timeout=timeout, limits=limits, http2=True,
        proxies=proxy if proxy else None
    ) as client:
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
                idx = 0
                empty_pages = 0

                while True:
                    if max_pages is not None and idx >= max_pages:
                        break
                    if target and yielded >= target:
                        return

                    start = idx * 20
                    url = SEARCH_FMT.format(
                        query=urllib.parse.quote_plus(term),
                        start=start,
                        uule=uule,
                    )

                    html = await _fetch_html(client, url)
                    if not html:
                        idx += 1
                        continue

                    if SORRY_RE.search(html or ""):
                        captcha_hits_global += 1
                        await _sleep_ms(_cooldown_secs(captcha_hits_global) * 1000)
                        idx += 1
                        continue

                    looks_ok = _looks_like_results(html)
                    phones = _extract_phones_from_html(html) if looks_ok else []

                    new = 0
                    for ph in phones:
                        if ph not in seen:
                            seen.add(ph)
                            new += 1
                            yielded += 1
                            yield ph
                            if target and yielded >= target:
                                return

                    # fim real: sem novos e não há próxima
                    if new == 0 and not _has_next(html):
                        break

                    empty_pages = empty_pages + 1 if new == 0 else 0
                    if empty_pages >= 3:
                        break

                    await _sleep_ms(180 + min(1200, int(idx * 35 + random.randint(80, 180))))
                    idx += 1

# Compatibilidade com o main
async def shutdown_playwright():
    return
