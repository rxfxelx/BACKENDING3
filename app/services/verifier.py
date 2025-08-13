import asyncio
from typing import Iterable, List, Tuple
import httpx
from ..config import settings

def _positive_from_json(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    # chaves comuns usadas pela UAZAPI e similares
    for k in ("exists", "whatsapp", "is_whatsapp", "isWhatsapp",
              "valid", "is_valid", "hasWhatsapp", "has_whatsapp"):
        v = data.get(k)
        if isinstance(v, bool) and v:
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
    UAZAPI /chat/check:
      - Header: token: <INSTANCE_TOKEN>
      - Body:   {"phone": "<E164>"}
    Backoff em 429/5xx.
    """
    url = settings.UAZAPI_CHECK_URL
    headers = {
        "token": settings.UAZAPI_INSTANCE_TOKEN or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = {"phone": phone}

    retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 2)))
    for attempt in range(retries + 1):
        try:
            r = await client.post(url, json=body, headers=headers, timeout=settings.UAZAPI_TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                # backoff e tenta de novo
                await asyncio.sleep(0.35 * (attempt + 1))
                continue
            # interpretar respostas 2xx/4xx
            try:
                data = r.json()
            except Exception:
                data = {}
            return phone, _positive_from_json(data)
        except Exception:
            await asyncio.sleep(0.35 * (attempt + 1))
    return phone, False

async def verify_batch(phones: Iterable[str]) -> Tuple[List[str], List[str]]:
    # dedup preservando ordem
    phones = list(dict.fromkeys([p for p in phones if p]))
    ok_list: List[str] = []
    bad_list: List[str] = []

    limits = httpx.Limits(
        max_keepalive_connections=settings.UAZAPI_MAX_CONCURRENCY,
        max_connections=max(settings.UAZAPI_MAX_CONCURRENCY, 6),
    )
    timeout = settings.UAZAPI_TIMEOUT

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(settings.UAZAPI_MAX_CONCURRENCY)

        async def task(p: str):
            async with sem:
                phone, ok = await _check_one(client, p)
                # leve espaçamento entre chamadas concorrentes
                await asyncio.sleep(getattr(settings, "UAZAPI_THROTTLE_MS", 120) / 1000.0)
                (ok_list if ok else bad_list).append(phone)

        await asyncio.gather(*[task(p) for p in phones])

    return ok_list, bad_list
