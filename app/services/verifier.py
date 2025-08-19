# app/services/verifier.py
import asyncio
from typing import Iterable, List, Tuple, Any
import httpx
from ..config import settings

CHECK_URL = settings.UAZAPI_CHECK_URL
TOKEN     = settings.UAZAPI_INSTANCE_TOKEN

# HTTP/2 só se 'h2' estiver instalado; caso contrário usa HTTP/1.1
try:
    import h2.config  # noqa: F401
    _HTTP2_AVAILABLE = True
except Exception:
    _HTTP2_AVAILABLE = False


def _chunks(seq: List[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _parse_payload(payload: Any, expected: List[str]) -> Tuple[List[str], List[str]]:
    """Extrai (ok, bad) de formatos de resposta comuns da UAZAPI.
       Só coloca em 'bad' quando a API afirma claramente que NÃO é WhatsApp.
    """
    ok: List[str] = []
    bad: List[str] = []

    if isinstance(payload, list):
        # lista simples de objetos { query/isInWhatsapp }
        for it in payload:
            try:
                q = str(it.get("query") or it.get("number") or "")
                v = it.get("isInWhatsapp")
                if v is True:
                    ok.append(q)
                elif v is False:
                    bad.append(q)
            except Exception:
                continue

    elif isinstance(payload, dict):
        # lista em 'results' / 'items' / 'data'
        lst = payload.get("results") or payload.get("items") or payload.get("data")
        if isinstance(lst, list):
            for it in lst:
                try:
                    q = str(it.get("query") or it.get("number") or "")
                    v = it.get("isInWhatsapp") or it.get("has_whatsapp")
                    if v is True:
                        ok.append(q)
                    elif v is False:
                        bad.append(q)
                except Exception:
                    continue
        # chaves diretas
        for k in ("ok", "valid", "whatsapp"):
            if isinstance(payload.get(k), list):
                ok.extend([str(x) for x in payload[k]])
        for k in ("bad", "invalid", "nowhatsapp", "not_whatsapp"):
            if isinstance(payload.get(k), list):
                bad.extend([str(x) for x in payload[k]])

    # filtra para somente o que pedimos
    expected_set = set(expected)
    ok = [p for p in dict.fromkeys(ok) if p in expected_set]
    bad = [p for p in dict.fromkeys(bad) if p in expected_set and p not in set(ok)]
    return ok, bad


async def _check_once(client: httpx.AsyncClient, numbers: List[str]) -> Tuple[List[str], List[str]]:
    """Chamada direta na UAZAPI. Em erro, deixa o lote sem classificar."""
    if not CHECK_URL or not TOKEN or not numbers:
        return [], []

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
    return _parse_payload(data, [str(n) for n in numbers])


async def verify_batch(
    numbers: Iterable[str],
    *,
    batch_size: int | None = None
) -> Tuple[List[str], List[str]]:
    """
    Verifica números em lotes com paralelismo controlado.
    - Não marca 'bad' em caso de erro/timeout; apenas quando a API afirma False.
    - Fallback automático para HTTP/1.1 se HTTP/2 indisponível.
    - Respeita UAZAPI_* do settings.
    """
    # de-dup (ordem estável)
    seen, dedup = set(), []
    for n in (str(x) for x in numbers if x):
        if n not in seen:
            seen.add(n)
            dedup.append(n)
    if not dedup:
        return [], []

    bs = batch_size or int(settings.UAZAPI_BATCH_SIZE or 20)

    # timeouts e limites
    t = float(settings.UAZAPI_TIMEOUT or 15)
    timeout = httpx.Timeout(t, connect=t, read=t, write=t, pool=t)
    limits  = httpx.Limits(
        max_keepalive_connections=int(settings.UAZAPI_MAX_CONCURRENCY or 2),
        max_connections=int(settings.UAZAPI_MAX_CONCURRENCY or 2),
    )

    async with httpx.AsyncClient(http2=_HTTP2_AVAILABLE, timeout=timeout, limits=limits) as client:
        sem = asyncio.Semaphore(int(settings.UAZAPI_MAX_CONCURRENCY or 2))
        retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 0)))
        delay   = float(getattr(settings, "UAZAPI_THROTTLE_MS", 0)) / 1000.0

        async def run_chunk(chunk: List[str]) -> Tuple[List[str], List[str]]:
            async with sem:
                for i in range(retries + 1):
                    try:
                        ok, bad = await _check_once(client, chunk)
                        return ok, bad
                    except Exception:
                        if i == retries:
                            # ERRO: não classifica como 'bad'
                            return [], []
                        await asyncio.sleep(delay)

        tasks = [asyncio.create_task(run_chunk(c)) for c in _chunks(dedup, bs)]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    ok_all: List[str] = []
    bad_all: List[str] = []
    for ok, bad in results:
        ok_all.extend(ok)
        bad_all.extend(bad)

    # dedup final e sem sobreposição
    ok_all  = list(dict.fromkeys(ok_all))
    bad_all = [p for p in list(dict.fromkeys(bad_all)) if p not in set(ok_all)]
    return ok_all, bad_all
