import asyncio
from typing import Iterable, List, Tuple
import httpx
from ..config import settings

CHECK_URL = settings.UAZAPI_CHECK_URL
TOKEN = settings.UAZAPI_INSTANCE_TOKEN

# HTTP/2 se disponível; fallback automático p/ HTTP/1.1
try:
    import h2.config  # noqa: F401
    _HTTP2_AVAILABLE = True
except Exception:
    _HTTP2_AVAILABLE = False


def _chunks(seq: List[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


async def _check_once(
    client: httpx.AsyncClient, numbers: List[str]
) -> Tuple[List[str], List[str], List[str]]:
    """
    POST {CHECK_URL}
    headers: token
    body:   {"numbers": ["55...","55..."]}
    Resposta (lista de objetos):
      { query|number: "...", isInWhatsapp: true|false }
    Retorna: (ok, bad, missing) — missing = números não retornados pela API.
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
    seen = set()

    for item in data:
        q = str(item.get("query") or item.get("number") or "").strip()
        if not q:
            continue
        seen.add(q)
        if item.get("isInWhatsapp") is True:
            ok.append(q)
        elif item.get("isInWhatsapp") is False:
            bad.append(q)

    # Tudo que enviamos e não voltou na resposta = missing
    sent_set = set(str(n) for n in numbers)
    missing = [n for n in numbers if n not in seen]

    return ok, bad, missing


async def verify_batch(
    numbers: Iterable[str], *, batch_size: int | None = None
) -> Tuple[List[str], List[str]]:
    """
    Verifica números na UAZAPI, garantindo cobertura:
    - de-dup com preservação de ordem
    - re-tenta APENAS os 'missing' até cobrir ou esgotar retries
    - HTTP/2 se disponível; timeout/limits configurados
    """
    # de-dup mantendo ordem
    seen, dedup = set(), []
    for n in (str(x).strip() for x in numbers if x):
        if n and n not in seen:
            seen.add(n)
            dedup.append(n)
    if not dedup:
        return [], []

    # batch "seguro" (às vezes 50+ sofre drop parcial)
    env_bs = int(getattr(settings, "UAZAPI_BATCH_SIZE", 50))
    bs = min(batch_size or env_bs, 25)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    t = float(getattr(settings, "UAZAPI_TIMEOUT", 15))
    timeout = httpx.Timeout(t, connect=t, read=t, write=t, pool=t)

    retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 3)))
    delay = float(getattr(settings, "UAZAPI_THROTTLE_MS", 250)) / 1000.0
    max_cc = max(1, int(getattr(settings, "UAZAPI_MAX_CONCURRENCY", 2)))

    ok_all: List[str] = []
    bad_all: List[str] = []

    async with httpx.AsyncClient(http2=_HTTP2_AVAILABLE, limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(max_cc)

        async def process_chunk(initial_chunk: List[str]):
            to_verify = list(initial_chunk)
            local_ok: List[str] = []
            local_bad: List[str] = []

            attempt = 0
            while to_verify:
                attempt += 1
                try:
                    ok, bad, missing = await _check_once(client, to_verify)
                except Exception:
                    # falha total desta rodada -> espera e re-tenta tudo
                    if attempt > retries:
                        # marca tudo como bad para não “perder” números
                        local_bad.extend(to_verify)
                        break
                    await asyncio.sleep(delay)
                    continue

                local_ok.extend(ok)
                local_bad.extend(bad)

                # re-tenta apenas os 'missing'
                to_verify = missing
                if not to_verify:
                    break
                if attempt > retries:
                    # se ainda restar missing após retries, classifica como 'bad'
                    local_bad.extend(to_verify)
                    break

                await asyncio.sleep(delay)

            return local_ok, local_bad

        tasks = []
        for chunk in _chunks(dedup, bs):
            async def run(c=chunk):
                async with sem:
                    return await process_chunk(c)
            tasks.append(asyncio.create_task(run()))

        results = await asyncio.gather(*tasks)

    for ok, bad in results:
        ok_all.extend(ok)
        bad_all.extend(bad)

    return ok_all, bad_all
