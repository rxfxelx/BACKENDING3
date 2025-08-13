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
    # Requisita exatamente como a API exige: header 'token' e body {"numbers":[...]}
    r = await client.post(
        CHECK_URL,
        json={"numbers": [str(n) for n in numbers]},
        headers={"Accept": "application/json", "token": TOKEN, "Content-Type": "application/json"},
    )
    r.raise_for_status()
    data = r.json() or []
    ok, bad = [], []
    for item in data:
        q = str(item.get("query") or item.get("number") or "")
        if item.get("isInWhatsapp") is True:
            ok.append(q)
        elif item.get("isInWhatsapp") is False:
            bad.append(q)
    return ok, bad

async def verify_batch(numbers: Iterable[str], *, batch_size: int | None = None) -> Tuple[List[str], List[str]]:
    """
    Verifica números na UAZAPI em paralelo.
    - Corrige httpx.Timeout: define default + connect/read/write/pool.
    - HTTP/2 ativo para menor latência.
    - Retry simples com throttle para estabilidade.
    """
    # de-dup mantendo ordem
    seen, dedup = set(), []
    for n in (str(x) for x in numbers if x):
        if n not in seen:
            seen.add(n); dedup.append(n)
    if not dedup:
        return [], []

    bs = batch_size or int(settings.UAZAPI_BATCH_SIZE)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    t = float(settings.UAZAPI_TIMEOUT)
    # >>> ponto crítico: timeout com default + 4 campos evita o ValueError
    timeout = httpx.Timeout(t, connect=t, read=t, write=t, pool=t)  # :contentReference[oaicite:2]{index=2}

    async with httpx.AsyncClient(http2=True, limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(int(settings.UAZAPI_MAX_CONCURRENCY))
        tasks = []

        for chunk in _chunks(dedup, bs):
            async def run(c=chunk):
                async with sem:
                    # retries
                    retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 0)))
                    delay = float(getattr(settings, "UAZAPI_THROTTLE_MS", 0)) / 1000.0
                    for i in range(retries + 1):
                        try:
                            return await _check_once(client, c)
                        except Exception:
                            if i == retries:
                                # Em último caso, marcamos o lote como 'bad' para não travar a busca
                                return [], c
                            await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(run()))

        results = await asyncio.gather(*tasks, return_exceptions=False)

    ok_all: List[str] = []
    bad_all: List[str] = []
    for ok, bad in results:
        ok_all.extend(ok)
        bad_all.extend(bad)
    return ok_all, bad_all
