import asyncio, json, httpx, random
from typing import Iterable, List, Tuple
from ..config import settings

def _positive_from_json(data: dict) -> bool:
    # Accept various possible shapes
    for k in ("exists","whatsapp","is_whatsapp","isWhatsapp","valid","is_valid"):
        if k in data and isinstance(data[k], bool):
            return bool(data[k])
    if "status" in data and isinstance(data["status"], str):
        return data["status"].lower() in ("ok","success","valid","exists","true")
    if "result" in data and isinstance(data["result"], str):
        return data["result"].lower() in ("ok","success","valid","exists","true")
    return False

async def _check_one(client: httpx.AsyncClient, phone: str) -> Tuple[str, bool]:
    url = settings.UAZAPI_CHECK_URL
    headers = {"Instance-Token": settings.UAZAPI_INSTANCE_TOKEN} if settings.UAZAPI_INSTANCE_TOKEN else {}
    # Try POST with common bodies, then GET fallbacks
    payloads = [
        {"phone": phone},
        {"number": phone},
        {"to": phone},
    ]
    for attempt in range(settings.UAZAPI_RETRIES + 1):
        try:
            # Try POST variants
            for body in payloads:
                r = await client.post(url, json=body, headers=headers, timeout=settings.UAZAPI_TIMEOUT)
                if r.status_code < 500:
                    try:
                        data = r.json()
                    except Exception:
                        data = {"status": str(r.status_code), "text": r.text[:200]}
                    ok = _positive_from_json(data)
                    return phone, ok
            # GET fallbacks
            for qp in ("phone","number","to"):
                r = await client.get(url, params={qp: phone}, headers=headers, timeout=settings.UAZAPI_TIMEOUT)
                if r.status_code < 500:
                    try:
                        data = r.json()
                    except Exception:
                        data = {"status": str(r.status_code), "text": r.text[:200]}
                    ok = _positive_from_json(data)
                    return phone, ok
        except Exception:
            await asyncio.sleep(0.2 * (attempt + 1))
    # If nothing worked, treat as False
    return phone, False

async def verify_batch(phones: Iterable[str]) -> Tuple[List[str], List[str]]:
    phones = list(dict.fromkeys(phones))  # dedupe preserve order
    ok_list: List[str] = []
    bad_list: List[str] = []
    limits = httpx.Limits(max_keepalive_connections=settings.UAZAPI_MAX_CONCURRENCY,
                          max_connections=max(settings.UAZAPI_MAX_CONCURRENCY, 5))
    async with httpx.AsyncClient(limits=limits, timeout=settings.UAZAPI_TIMEOUT) as client:
        sem = asyncio.Semaphore(settings.UAZAPI_MAX_CONCURRENCY)
        async def task(p: str):
            async with sem:
                phone, ok = await _check_one(client, p)
                await asyncio.sleep(settings.UAZAPI_THROTTLE_MS / 1000)
                (ok_list if ok else bad_list).append(phone)
        await asyncio.gather(*[task(p) for p in phones])
    return ok_list, bad_list
