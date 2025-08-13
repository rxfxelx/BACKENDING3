import asyncio, re, random
from typing import Iterable, List, Tuple
import httpx
from ..config import settings

def _to_digits_br(phone: str) -> str:
    d = re.sub(r"\D", "", phone or "")
    if d.startswith("55"):
        return d
    if len(d) in (10, 11):
        return "55" + d
    return d

def _is_positive(data) -> bool:
    if not isinstance(data, dict):
        return False
    # chaves comuns que significam "tem WhatsApp"
    for k in (
        "exists", "whatsapp", "is_whatsapp", "isWhatsapp",
        "valid", "is_valid", "hasWhatsapp", "has_whatsapp",
    ):
        v = data.get(k)
        if isinstance(v, bool) and v is True:
            return True
        if isinstance(v, str) and v.lower() in ("true", "yes", "ok", "exists", "valid", "success"):
            return True
    v = data.get("result") or data.get("status")
    if isinstance(v, str) and v.lower() in ("ok", "success", "valid", "exists", "true"):
        return True
    if isinstance(v, (int, float)) and int(v) == 200:
        return True
    return False

async def _check_one(client: httpx.AsyncClient, phone: str) -> Tuple[str, bool]:
    """
    UAZAPI /chat/check
    Header: token: <INSTANCE_TOKEN>
    Body:   {"phone": "5511999999999"}  # apenas dígitos, sem '+'
    """
    url = settings.UAZAPI_CHECK_URL
    headers = {
        "token": settings.UAZAPI_INSTANCE_TOKEN or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = {"phone": _to_digits_br(phone)}

    retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 2)))
    base_sleep = 0.35
    for attempt in range(retries + 1):
        try:
            r = await client.post(url, json=body, headers=headers, timeout=settings.UAZAPI_TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                # backoff exponencial com jitter
                await asyncio.sleep(base_sleep * (2 ** attempt) + random.uniform(0, 0.2))
                continue
            try:
                data = r.json()
            except Exception:
                data = {}
            return phone, _is_positive(data)
        except Exception:
            await asyncio.sleep(base_sleep * (2 ** attempt) + random.uniform(0, 0.2))
    return phone, False

async def verify_batch(phones: Iterable[str]) -> Tuple[List[str], List[str]]:
    # dedup preservando ordem
    phones = list(dict.fromkeys([p for p in phones if p]))
    ok_list: List[str] = []
    bad_list: List[str] = []

    # Limite baixo para evitar 429 intermitente
    conc = max(1, int(getattr(settings, "UAZAPI_MAX_CONCURRENCY", 2)))
    limits = httpx.Limits(
        max_keepalive_connections=conc,
        max_connections=max(conc, 4),
    )
    timeout = settings.UAZAPI_TIMEOUT

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(conc)

        async def task(p: str):
            async with sem:
                phone, ok = await _check_one(client, p)
                await asyncio.sleep(getattr(settings, "UAZAPI_THROTTLE_MS", 200) / 1000.0)
                (ok_list if ok else bad_list).append(phone)

        await asyncio.gather(*[task(p) for p in phones])

    return ok_list, bad_list
