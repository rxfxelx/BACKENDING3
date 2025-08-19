import asyncio
from typing import Iterable, List, Tuple, Optional
import httpx
from ..config import settings

CHECK_URL = settings.UAZAPI_CHECK_URL
TOKEN = settings.UAZAPI_INSTANCE_TOKEN

# HTTP/2 auto: se 'h2' não estiver instalado, seguimos em HTTP/1.1
try:
    import h2.config  # noqa: F401
    _HTTP2_AVAILABLE = True
except Exception:
    _HTTP2_AVAILABLE = False


def _e164(n: str) -> Optional[str]:
    """Normaliza para E.164 simples: só dígitos, exige começando por 55 e 12~13 dígitos."""
    if not n:
        return None
    d = "".join(ch for ch in str(n) if ch.isdigit())
    if d.startswith("550"):
        d = "55" + d[3:]  # remove zero fantasma após 55
    if not d.startswith("55"):
        d = "55" + d  # garante o país
    # Brasil: 55 + (10 ou 11 dígitos) -> total 12 ou 13
    if 12 <= len(d) <= 13:
        return d
    return None


def _chunks(seq: List[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


async def _check_once(client: httpx.AsyncClient, numbers: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    Chamada exata da UAZAPI:
      POST {CHECK_URL}
      headers: token
      body:   {"numbers": ["55...","55..."]}
    Resposta: lista com isInWhatsapp True/False.

    Retorno: (ok, bad, unknown)
    """
    try:
        r = await client.post(
            CHECK_URL,
            json={"numbers": numbers},
            headers={
                "Accept": "application/json",
                "token": TOKEN,
                "Content-Type": "application/json",
            },
        )
        r.raise_for_status()
        data = r.json() or []
    except Exception:
        # falhou o lote inteiro
        return [], [], numbers[:]

    ok, bad, unknown = [], [], []
    for item in data:
        q = str(item.get("query") or item.get("number") or "")
        if item.get("isInWhatsapp") is True:
            ok.append(q)
        elif item.get("isInWhatsapp") is False:
            bad.append(q)
        else:
            unknown.append(q)
    return ok, bad, unknown


async def _wa_me_probe(client: httpx.AsyncClient, n: str) -> Optional[bool]:
    """
    Heurística leve: GET https://wa.me/<n>. Se o HTML contiver a mensagem
    padrão de inválido, devolve False; caso contrário, True (best-effort).
    """
    try:
        r = await client.get(f"https://wa.me/{n}", timeout=10.0)
        txt = (r.text or "").lower()
        # mensagens comuns (pt/en) que indicam número inválido
        signals = [
            "phone number shared via url is invalid",
            "o número de telefone compartilhado via url é inválido",
            "número de telefone via url é inválido",
        ]
        if any(s in txt for s in signals):
            return False
        return True
    except Exception:
        return None


async def verify_batch(numbers: Iterable[str], *, batch_size: int | None = None) -> Tuple[List[str], List[str]]:
    """
    Verifica números na UAZAPI em paralelo.

    - NUNCA conta 'unknown' como não-WA.
    - Retenta 'unknown' respeitando UAZAPI_RETRIES/UAZAPI_THROTTLE_MS.
    - Opcionalmente, revalida os 'bad' com wa.me se WA_ME_SECOND_PASS=1.
    """
    # de-dup + normalização
    seen, dedup = set(), []
    for raw in (str(x) for x in numbers if x):
        n = _e164(raw)
        if not n:
            continue
        if n not in seen:
            seen.add(n)
            dedup.append(n)
    if not dedup:
        return [], []

    bs = batch_size or int(getattr(settings, "UAZAPI_BATCH_SIZE", 50))

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    t = float(getattr(settings, "UAZAPI_TIMEOUT", 15))
    timeout = httpx.Timeout(t, connect=t, read=t, write=t, pool=t)

    async with httpx.AsyncClient(http2=_HTTP2_AVAILABLE, limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(int(getattr(settings, "UAZAPI_MAX_CONCURRENCY", 2)))

        async def run_chunk(chunk: List[str]):
            retries = max(0, int(getattr(settings, "UAZAPI_RETRIES", 3)))
            delay = float(getattr(settings, "UAZAPI_THROTTLE_MS", 250)) / 1000.0

            cur = chunk[:]
            ok_all, bad_all = [], []
            for attempt in range(retries + 1):
                ok, bad, unknown = await _check_once(client, cur)
                ok_all.extend(ok)
                bad_all.extend(bad)
                if not unknown:
                    break
                if attempt == retries:
                    # esgotou: unknown NÃO vira bad; só abandona
                    break
                await asyncio.sleep(delay)
                cur = unknown[:]  # re-loteia só os que não definiram

            # segundo passe opcional para recuperar falsos negativos
            if bad_all and str(getattr(settings, "WA_ME_SECOND_PASS", "0")) == "1":
                probe_ok = []
                # pequena limitação para não estourar tempo
                wa_me_limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
                async with httpx.AsyncClient(limits=wa_me_limits, timeout=10.0) as probe_client:
                    tasks = [asyncio.create_task(_wa_me_probe(probe_client, b)) for b in bad_all]
                    results = await asyncio.gather(*tasks, return_exceptions=False)
                for i, res in enumerate(results):
                    if res is True:
                        probe_ok.append(bad_all[i])
                if probe_ok:
                    # move os que "parecem WA" pelo wa.me do bad -> ok
                    ok_all.extend(probe_ok)
                    bad_all = [b for b in bad_all if b not in set(probe_ok)]

            return ok_all, bad_all

        tasks = [asyncio.create_task(run_chunk(c)) for c in _chunks(dedup, bs)]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    ok_final: List[str] = []
    bad_final: List[str] = []
    for ok, bad in results:
        ok_final.extend(ok)
        bad_final.extend(bad)
    return ok_final, bad_final
