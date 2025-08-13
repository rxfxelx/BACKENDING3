import asyncio, re, random
from typing import Iterable, List, Tuple, Dict, Set
import httpx
from ..config import settings

def _to_digits_br(phone: str) -> str:
    d = re.sub(r"\D", "", phone or "")
    if d.startswith("55"):
        return d
    if len(d) in (10, 11):
        return "55" + d
    return d

def _truth(v) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) in (1, 200)
    if isinstance(v, str): return v.lower() in ("true","yes","ok","exists","valid","success")
    return False

def _parse_batch(data, sent: List[str]) -> Set[str]:
    ok: Set[str] = set()
    if isinstance(data, dict):
        # formato { "<num>": true/false, ... }
        if all(k.isdigit() for k in data.keys() if isinstance(k, str)):
            for k,v in data.items():
                if _truth(v): ok.add(k)
            return ok
        # formato {"data":[{"number":"5511...", "exists":true},...]} ou {"results":[...]}
        lst = None
        for key in ("data","results","numbers","items"):
            if isinstance(data.get(key), list):
                lst = data[key]; break
        if lst is not None:
            for row in lst:
                if not isinstance(row, dict): continue
                num = row.get("number") or row.get("phone") or row.get("to") or ""
                num = re.sub(r"\D","", str(num))
                val = False
                for k in ("exists","whatsapp","is_whatsapp","isWhatsapp","valid","is_valid","hasWhatsapp","has_whatsapp","status","result"):
                    if k in row:
                        val = _truth(row[k]); break
                if num and val: ok.add(num)
            return ok
    if isinstance(data, list):
        # lista de números válidos
        for x in data:
            if isinstance(x, str) and x.isdigit():
                ok.add(x)
    return ok

async def _post_batch(client: httpx.AsyncClient, numbers: List[str]) -> Dict:
    url = settings.UAZAPI_CHECK_URL
    headers = {
        "token": settings.UAZAPI_INSTANCE_TOKEN or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"numbers": numbers}
    r = await client.post(url, json=payload, headers=headers, timeout=settings.UAZAPI_TIMEOUT)
    try:
        return r.json()
    except Exception:
        return {}

async def _post_single(client: httpx.AsyncClient, number: str) -> bool:
    url = settings.UAZAPI_CHECK_URL
    headers = {
        "token": settings.UAZAPI_INSTANCE_TOKEN or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"phone": number}
    r = await client.post(url, json=payload, headers=headers, timeout=settings.UAZAPI_TIMEOUT)
    try:
        data = r.json()
    except Exception:
        data = {}
    # interpreta resposta single reaproveitando parser batch
    return _truth(data) or (number in _parse_batch(data, [number]))

async def verify_batch(phones: Iterable[str]) -> Tuple[List[str], List[str]]:
    """
    Verifica preferindo o formato em lote {"numbers":[...]}.
    Fallback para chamadas individuais {"phone": "..."} quando necessário.
    Mantém ordem de entrada.
    """
    original = list(dict.fromkeys([p for p in phones if p]))  # dedup mantém ordem
    digits = [_to_digits_br(p) for p in original]

    ok_set: Set[str] = set()
    bad_set: Set[str] = set()

    # client com limites conservadores
    conc = max(1, int(getattr(settings, "UAZAPI_MAX_CONCURRENCY", 2)))
    limits = httpx.Limits(max_keepalive_connections=conc, max_connections=max(conc, 4))
    timeout = settings.UAZAPI_TIMEOUT

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        # tenta em blocos no endpoint de lote
        chunk_size = max(5, int(getattr(settings, "UAZAPI_BATCH_SIZE", 50)))
        i = 0
        while i < len(digits):
            chunk = digits[i:i+chunk_size]
            try:
                data = await _post_batch(client, chunk)
                oks = _parse_batch(data, chunk)
                if oks:
                    ok_set.update(oks)
                    # marca como desconhecidos os que não vieram
                    for n in chunk:
                        if n not in oks:
                            bad_set.add(n)
                else:
                    # lote não suportado → cai para single para todo o bloco
                    for n in chunk:
                        try:
                            is_ok = await _post_single(client, n)
                        except Exception:
                            is_ok = False
                        (ok_set if is_ok else bad_set).add(n)
                        await asyncio.sleep(getattr(settings, "UAZAPI_THROTTLE_MS", 250) / 1000.0)
                # backoff leve entre blocos
                await asyncio.sleep(0.25 + random.uniform(0, 0.15))
            except Exception:
                # erro no bloco inteiro → tenta singles
                for n in chunk:
                    try:
                        is_ok = await _post_single(client, n)
                    except Exception:
                        is_ok = False
                    (ok_set if is_ok else bad_set).add(n)
                    await asyncio.sleep(getattr(settings, "UAZAPI_THROTTLE_MS", 250) / 1000.0)
            i += chunk_size

    # reconstrói nas listas mantendo ordem do input
    ok_list, bad_list = [], []
    for orig, num in zip(original, digits):
        if num in ok_set: ok_list.append(orig)
        elif num in bad_set: bad_list.append(orig)
        else: bad_list.append(orig)
    return ok_list, bad_list
