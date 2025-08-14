import json
from io import StringIO
from typing import List
from asyncio import CancelledError

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers     # async generator
from .services.verifier import verify_batch      # async, retorna (ok:list, bad:list)

app = FastAPI(title="ClickLeads Backend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# ---------- util ----------
def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

def _batch_size(n: int) -> int:
    if n <= 5: return 6
    if n <= 20: return 10
    if n <= 100: return 20
    return 30

_UF = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"}
def _parse_cidades(local: str) -> List[str]:
    out, seen = [], set()
    for tok in (local or "").split(","):
        t = tok.strip()
        if not t: continue
        if t.upper() in _UF or len(t) <= 2:  # ignora SP/MG etc.
            continue
        if t not in seen:
            seen.add(t); out.append(t)
    return out or [""]

# ---------- health ----------
@app.get("/health")
async def health():
    return {"status": "ok"}

# ---------- STREAM ----------
@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    cidades = _parse_cidades(local)
    target = n

    async def gen():
        delivered = 0            # conta enviados ao cliente (WA quando verify=1)
        non_wa = 0               # verificados sem WA
        searched = 0             # telefones raspados (brutos)
        vistos = set()
        batch_sz = _batch_size(target)

        try:
            yield sse("start", {"message": "started"})

            for cidade in cidades:
                # cidade atual
                yield sse("city", {"status": "start", "name": cidade})

                # se já atingiu antes de entrar, finalize
                if delivered >= target:
                    yield sse("city", {"status": "done", "name": cidade})
                    break

                scrape_cap = 0 if somente_wa else max(0, target - delivered)
                pool: List[str] = []

                # 1) processa a cidade ATÉ O GERADOR ACABAR
                async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                    if delivered >= target:
                        break
                    if not ph or ph in vistos:
                        continue

                    vistos.add(ph)
                    searched += 1

                    if not somente_wa:
                        delivered += 1
                        yield sse("item", {"phone": ph})
                        yield sse("progress", {
                            "wa_count": delivered,
                            "non_wa_count": non_wa,
                            "searched": searched,
                            "city": cidade
                        })
                        continue

                    # Somente WhatsApp: acumula e verifica por lote
                    pool.append(ph)
                    if len(pool) >= batch_sz:
                        ok, bad = await verify_batch(pool, batch_size=batch_sz)
                        pool.clear()
                        non_wa += len(bad)

                        for p in ok:
                            if delivered < target:
                                delivered += 1
                                yield sse("item", {"phone": p, "has_whatsapp": True})
                                if delivered >= target: break

                        yield sse("progress", {
                            "wa_count": delivered,
                            "non_wa_count": non_wa,
                            "searched": searched,
                            "city": cidade
                        })
                        if delivered >= target: break

                # 2) flush final da cidade (resto do pool)
                if somente_wa and pool and delivered < target:
                    ok, bad = await verify_batch(pool, batch_size=batch_sz)
                    pool.clear()
                    non_wa += len(bad)
                    for p in ok:
                        if delivered < target:
                            delivered += 1
                            yield sse("item", {"phone": p, "has_whatsapp": True})
                            if delivered >= target: break
                    yield sse("progress", {
                        "wa_count": delivered,
                        "non_wa_count": non_wa,
                        "searched": searched,
                        "city": cidade
                    })

                # 3) cidade terminou. se não bateu meta e há mais cidades, o for segue.
                yield sse("city", {"status": "done", "name": cidade})
                if delivered >= target:
                    break

            exhausted = delivered < target
            yield sse("done", {
                "wa_count": delivered,
                "non_wa_count": non_wa,
                "searched": searched,
                "exhausted": exhausted
            })

        except CancelledError:
            # cliente fechou SSE; encerra silenciosamente
            return
        except Exception as e:
            # erro interno; tenta finalizar com estado atual
            yield sse("progress", {
                "error": str(e),
                "wa_count": delivered,
                "non_wa_count": non_wa,
                "searched": searched
            })
            yield sse("done", {
                "wa_count": delivered,
                "non_wa_count": non_wa,
                "searched": searched,
                "exhausted": delivered < target
            })

    return StreamingResponse(gen(), media_type="text/event-stream")

# ---------- JSON (fallback) ----------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    cidades = _parse_cidades(local)
    target = n

    items: List[str] = []
    delivered = 0
    non_wa = 0
    searched = 0
    vistos = set()
    batch_sz = _batch_size(target)

    try:
        for cidade in cidades:
            if delivered >= target: break

            scrape_cap = 0 if somente_wa else max(0, target - delivered)
            pool: List[str] = []

            async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                if delivered >= target: break
                if not ph or ph in vistos: continue

                vistos.add(ph)
                searched += 1

                if not somente_wa:
                    items.append(ph); delivered += 1
                    continue

                pool.append(ph)
                if len(pool) >= batch_sz:
                    ok, bad = await verify_batch(pool, batch_size=batch_sz)
                    pool.clear()
                    non_wa += len(bad)
                    for p in ok:
                        if delivered < target:
                            items.append(p); delivered += 1
                            if delivered >= target: break

            if somente_wa and pool and delivered < target:
                ok, bad = await verify_batch(pool, batch_size=batch_sz)
                pool.clear()
                non_wa += len(bad)
                for p in ok:
                    if delivered < target:
                        items.append(p); delivered += 1
                        if delivered >= target: break

            if delivered >= target: break

    except Exception:
        pass

    data = [{"phone": p, "has_whatsapp": bool(verify)} for p in items[:target]]
    return JSONResponse({
        "items": data,
        "leads": data,
        "wa_count": delivered,
        "non_wa_count": non_wa,
        "searched": searched
    })

# ---------- CSV ----------
def _csv_response(csv_bytes: bytes, filename: str) -> Response:
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.get("/export")
async def export_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    resp = await leads(nicho=nicho, local=local, n=n, verify=verify)
    payload = json.loads(resp.body)
    phones = [row["phone"] for row in payload.get("items", [])]
    buf = StringIO(); buf.write("phone\n")
    for p in phones: buf.write(str(p).strip() + "\n")
    csv = buf.getvalue().encode("utf-8")
    filename = f"leads_{nicho.strip().replace(' ','_')}_{local.strip().replace(' ','_')}.csv"
    return _csv_response(csv, filename)
