import asyncio
from typing import Iterable, List, Tuple
import httpx
from ..config import settings

CHECK_URL = settings.UAZAPI_CHECK_URL
TOKEN = settings.UAZAPI_INSTANCE_TOKEN

def _chunks(seq: List[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

async def _check_once(client: httpx.AsyncClient, numbers: List[str]) -> Tuple[List[str], List[str]]:
    try:
        r = await client.post(
            CHECK_URL,
            json={"numbers": numbers},
            headers={"Accept": "application/json", "token": TOKEN, "Content-Type": "application/json"},
            timeout=settings.UAZAPI_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json() or []
        ok = [str(x.get("query") or x.get("number")) for x in data if x.get("isInWhatsapp") is True]
        bad = [str(x.get("query") or x.get("number")) for x in data if x.get("isInWhatsapp") is False]
        return ok, bad
    except Exception:
        return [], numbers

async def verify_batch(numbers: Iterable[str], *, batch_size: int | None = None) -> Tuple[List[str], List[str]]:
    """Verifica números na UAZAPI em paralelo com HTTP/2."""
    nums = list(dict.fromkeys(str(n) for n in numbers if n))
    if not nums:
        return [], []

    bs = batch_size or settings.UAZAPI_BATCH_SIZE
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    timeout = httpx.Timeout(connect=10.0, read=settings.UAZAPI_TIMEOUT, write=10.0)

    async with httpx.AsyncClient(http2=True, limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(settings.UAZAPI_MAX_CONCURRENCY)
        tasks = []
        for chunk in _chunks(nums, bs):
            async def run(c=chunk):
                async with sem:
                    return await _check_once(client, c)
            tasks.append(asyncio.create_task(run()))
        results = await asyncio.gather(*tasks, return_exceptions=False)

    ok_all: List[str] = []
    bad_all: List[str] = []
    for ok, bad in results:
        ok_all.extend(ok); bad_all.extend(bad)
    return ok_all, bad_all
