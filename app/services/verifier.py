import asyncio
from typing import Iterable, List, Tuple
import httpx
from ..config import settings

CHECK_URL = settings.UAZAPI_CHECK_URL
TOKEN = settings.UAZAPI_INSTANCE_TOKEN

# Detecta suporte a HTTP/2; se não houver 'h2', cai para HTTP/1.1
try:
    import h2.config  # noqa: F401
    _HTTP2_AVAILABLE = True
except Exception:
    _HTTP2_AVAILABLE = False

def _chunks(seq: List[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

async def _check_once(client: httpx.AsyncClient, numbers: List[str]) -> Tuple[List[str], List[str]]:
    """
    Chamada exata da UAZAPI:
      POST {CHECK_URL}
      headers: token
      body:   {"numbers": ["55...","55..."]}
    Resposta: lista com isInWhatsapp True/False.
    """
    r = await client.post(
        CHECK_URL,
        json={"numbers": [str(n) for n in numbers]},
        headers={
            "Accept": "application/json",
            "token": TOKEN,
            "Content-Type": "application/json",
        },
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
    - Timeout correto (default + connect/read/write/pool).
    - HTTP/2 quando disponível; fallback automático para HTTP/1.1 se 'h2' não estiver instalado.
    - Reintento simples respeitando UAZAPI_RETRIES e UAZAPI_THROTTLE_MS.
    """
    # de-dup mantendo ordem
    seen, dedup = set(), []
    for n in (str(x) for x in numbers if x):
        if n not in seen:
            seen.add(n)
            dedup.append(n)
    if not dedup:
        return [], []

    bs = batch_size or int(settings.UAZAPI_BATCH_SIZE)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    t = float(settings.UAZAPI_TIMEOUT)
    timeout = httpx.Timeout(t, connect=t, read=t, write=t, pool=t)  # evita ValueError do httpx

    async with httpx.AsyncClient(http2=_HTTP2_AVAILABLE, limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(int(settings.UAZAPI_MAX_CONCURRENCY))
        tasks = []

        for chunk in _chunks(dedup, bs):
            async def run(c=chunk):
                async with sem:
                    retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 0)))
                    delay = float(getattr(settings, "UAZAPI_THROTTLE_MS", 0)) / 1000.0
                    for i in range(retries + 1):
                        try:
                            return await _check_once(client, c)
                        except Exception:
                            if i == retries:
                                # Última tentativa: não travar o fluxo
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
