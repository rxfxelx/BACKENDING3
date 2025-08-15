import json
from io import StringIO
from typing import List
from asyncio import CancelledError

from fastapi import FastAPI, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

# >>> Auth
from .auth import router as auth_router, verify_access_via_query

app = FastAPI(title="ClickLeads Backend", version="2.0.2")

# CORS: permite qualquer origem; não use credentials com "*"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# registra rotas de auth
app.include_router(auth_router)

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
        if t.upper() in _UF or len(t) <= 2:
            continue
        if t not in seen:
            seen.add(t); out.append(t)
    return out or [""]

@app.get("/health")
async def health():
    return {"status": "ok"}

# ================= STREAM =================
@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
    # >>> proteção (1 sessão ativa). Lê access/sid/device da query automaticamente.
    auth = Depends(verify_access_via_query),
):
    _uid, _sid, _dev = auth  # não usados abaixo

    somente_wa = verify == 1
    cidades = _parse_cidades(local)
    target = n

    async def gen():
        delivered = 0
        non_wa = 0
        searched = 0
        vistos = set()
        batch_sz = _batch_size(target)
        sent_done = False

        try:
            yield sse("start", {"message": "started"})

            for cidade in cidades:
                yield sse("city", {"status": "start", "name": cidade})
                if delivered >= target:
                    yield sse("city", {"status": "done", "name": cidade})
                    break

                scrape_cap = max((target - delivered) * 12, 200) if somente_wa else max(0, target - delivered)
                pool: List[str] = []

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
                        yield sse("progress", {"wa_count": delivered, "non_wa_count": non_wa, "searched": searched, "city": cidade})
                        continue

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
                        yield sse("progress", {"wa_count": delivered, "non_wa_count": non_wa, "searched": searched, "city": cidade})
                        if delivered >= target: break

                if somente_wa and pool and delivered < target:
                    ok, bad = await verify_batch(pool, batch_size=batch_sz)
                    pool.clear()
                    non_wa += len(bad)
                    for p in ok:
                        if delivered < target:
                            delivered += 1
                            yield sse("item", {"phone": p, "has_whatsapp": True})
                            if delivered >= target: break
                    yield sse("progress", {"wa_count": delivered, "non_wa_count": non_wa, "searched": searched, "city": cidade})

                yield sse("city", {"status": "done", "name": cidade})
                if delivered >= target:
                    break

            exhausted = delivered < target
            yield sse("done", {"wa_count": delivered, "non_wa_count": non_wa, "searched": searched, "exhausted": exhausted})
            sent_done = True

        except CancelledError:
            return
        except Exception as e:
            yield sse("progress", {"error": str(e), "wa_count": delivered, "non_wa_count": non_wa, "searched": searched})
            yield sse("done", {"wa_count": delivered, "non_wa_count": non_wa, "searched": searched, "exhausted": delivered < target})
            sent_done = True
        finally:
            if not sent_done:
                yield sse("done", {"wa_count": delivered, "non_wa_count": non_wa, "searched": searched, "exhausted": delivered < target})

    return StreamingResponse(gen(), media_type="text/event-stream")

# ================= JSON (fallback) =================
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
            scrape_cap = max((target - delivered) * 12, 200) if somente_wa else max(0, target - delivered)
            pool: List[str] = []

            async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                if delivered >= target: break
                if not ph or ph in vistos: continue
                vistos.add(ph); searched += 1

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

            if somente_wa && pool && delivered < target:
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

# ================= CSV =================
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
