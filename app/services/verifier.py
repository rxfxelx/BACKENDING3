import asyncio, re
from typing import Iterable, List, Tuple
import httpx
from ..config import settings

def _to_digits_br(phone: str) -> str:
    d = re.sub(r"\D", "", phone or "")
    if d.startswith("55"):
        return d
    if len(d) in (10, 11):
        return "55" + d
    return d

async def verify_batch(phones: Iterable[str]) -> Tuple[List[str], List[str]]:
    """
    Usa o endpoint em lote da UAZAPI:
      POST https://helsenia.uazapi.com/chat/check
      body: {"numbers": ["5511...", ...]}
      header: token: <INSTANCE_TOKEN>
    Resposta esperada: lista de objetos com campos
      {"query": "...", "isInWhatsapp": true|false, ...}
    Retorna (ok_list, bad_list) preservando a ordem original.
    """
    originals = list(dict.fromkeys([p for p in phones if p]))  # dedup preservando ordem
    digits = [_to_digits_br(p) for p in originals]

    ok_digits: set[str] = set()
    bad_digits: set[str] = set()

    headers = {
        "token": settings.UAZAPI_INSTANCE_TOKEN or "",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    chunk_size = max(10, int(getattr(settings, "UAZAPI_BATCH_SIZE", 50)))

    limits = httpx.Limits(max_keepalive_connections=2, max_connections=4)
    async with httpx.AsyncClient(limits=limits, timeout=settings.UAZAPI_TIMEOUT) as client:
        for i in range(0, len(digits), chunk_size):
            chunk = [d for d in digits[i:i + chunk_size] if d]
            if not chunk:
                continue
            try:
                r = await client.post(settings.UAZAPI_CHECK_URL, json={"numbers": chunk}, headers=headers)
                data = r.json()
            except Exception:
                data = None

            if isinstance(data, list):
                # formato da doc: [{query, isInWhatsapp, ...}, ...]
                for obj in data:
                    try:
                        q = re.sub(r"\D", "", str(obj.get("query", "")))
                        is_wa = bool(obj.get("isInWhatsapp") or obj.get("isInWhatsApp"))
                        if q:
                            (ok_digits if is_wa else bad_digits).add(q)
                    except Exception:
                        continue
            else:
                # fallback 1-a-1 se o lote não veio como lista
                for d in chunk:
                    try:
                        rr = await client.post(settings.UAZAPI_CHECK_URL, json={"phone": d}, headers=headers)
                        dd = rr.json()
                    except Exception:
                        dd = {}
                    is_wa = False
                    if isinstance(dd, dict):
                        v = dd.get("isInWhatsapp") or dd.get("isInWhatsApp") or dd.get("exists") \
                            or dd.get("whatsapp") or dd.get("valid") or dd.get("is_valid")
                        if isinstance(v, bool):
                            is_wa = v
                        elif isinstance(v, str):
                            is_wa = v.lower() in ("true", "yes", "ok", "exists", "valid", "success")
                    (ok_digits if is_wa else bad_digits).add(d)
                    await asyncio.sleep(getattr(settings, "UAZAPI_THROTTLE_MS", 250) / 1000.0)

            # pequena pausa entre blocos para evitar 429
            await asyncio.sleep(getattr(settings, "UAZAPI_THROTTLE_MS", 250) / 1000.0)

    ok_list, bad_list = [], []
    for orig, d in zip(originals, digits):
        if d in ok_digits:
            ok_list.append(orig)
        elif d in bad_digits:
            bad_list.append(orig)
        else:
            bad_list.append(orig)
    return ok_list, bad_list
