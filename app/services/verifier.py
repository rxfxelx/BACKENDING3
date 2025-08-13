import asyncio, httpx
from typing import Iterable, List, Tuple
from ..config import settings

def _positive_from_json(data: dict) -> bool:
    # cobre respostas comuns
    for k in ("exists","whatsapp","is_whatsapp","isWhatsapp","valid","is_valid"):
        if k in data and isinstance(data[k], bool):
            return bool(data[k])
    if "result" in data and isinstance(data["result"], str):
        if data["result"].lower() in ("ok","success","valid","exists","true"):
            return True
    if "status" in data:
        val = data["status"]
        if isinstance(val, str) and val.lower() in ("ok","success","valid","exists","true"):
            return True
        if isinstance(val, (int, float)) and int(val) == 200:
            return True
    return False

async def _check_one(client: httpx.AsyncClient, phone: str) -> Tuple[str, bool]:
    url = settings.UAZAPI_CHECK_URL
    token = settings.UAZAPI_INSTANCE_TOKEN or ""

    # testamos múltiplas variações de header aceitas por provedores comuns
    header_variants = [
        {"Instance-Token": token},
        {"instance-token": token},
        {"X-Instance-Token": token},
        {"Authorization": f"Bearer {token}"},
        {"apikey": token},
    ] if token else [{}]

    body_variants = [
        {"phone": phone},
        {"number": phone},
        {"to": phone},
    ]

    for attempt in range(settings.UAZAPI_RETRIES + 1):
        try:
            for h in header_variants:
                for b in body_variants:
                    r = await client.post(url, json=b, headers=h, timeout=settings.UAZAPI_TIMEOUT)
                    if r.status_code < 500:
                        try:
                            data = r.json()
                        except Exception:
                            data = {"status": r.status_code}
                        return phone, _positive_from_json(data)

            for h in header_variants:
                for qp in ("phone","number","to"):
                    r = await client.get(url, params={qp: phone}, headers=h, timeout=settings.UAZAPI_TIMEOUT)
                    if r.status_code < 500:
                        try:
                            data = r.json()
                        except Exception:
                            data = {"status": r.status_code}
                        return phone, _positive_from_json(data)

        except Exception:
            await asyncio.sleep(0.25 * (attempt + 1))

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

    async with httpx.AsyncClient(limits=limits, timeout=settings.UAZAPI_TIMEOUT) as client:
        sem = asyncio.Semaphore(settings.UAZAPI_MAX_CONCURRENCY)

        async def task(p: str):
            async with sem:
                phone, ok = await _check_one(client, p)
                await asyncio.sleep(settings.UAZAPI_THROTTLE_MS / 1000.0)
                (ok_list if ok else bad_list).append(phone)

        await asyncio.gather(*[task(p) for p in phones])

    return ok_list, bad_list
