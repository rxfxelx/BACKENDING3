import json
from io import StringIO
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.7.8")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

def _effective_sizes(target: int) -> tuple[int, int]:
    if target <= 1:   return 4, 8
    if target <= 3:   return 6, 14
    if target <= 10:  return 8, max(24, target * 3)
    if target <= 50:  return 12, max(90, target * 3)
    return 16, max(240, target * 2)

# =============== STREAM ===============
@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    cidades = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    async def gen():
        yield sse("start", {"message": "started"})

        delivered = 0               # se verify=1: conta APENAS WhatsApp
        checked_bad = 0
        searched = 0
        seen = set()

        batch_sz, _ = _effective_sizes(target)

        try:
            for cidade in cidades:
                if delivered >= target:
                    break

                # no modo WA, não limitamos a coleta pelo 'n'; deixamos o scraper esgotar a cidade
                scrape_cap = 0 if somente_wa else (target - delivered)

                raw_pool: List[str] = []
                empty_wa_batches = 0  # 3 lotes seguidos sem WA -> troca de cidade

                async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                    if delivered >= target:
                        break
                    if ph in seen:
                        continue
                    seen.add(ph)
                    searched += 1

                    if not somente_wa:
                        delivered += 1
                        yield sse("item", {"phone": ph})
                        yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched})
                        if delivered >= target:
                            break
                        continue

                    # Somente WhatsApp: acumula e verifica em lotes
                    raw_pool.append(ph)
                    if len(raw_pool) >= batch_sz:
                        ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                        raw_pool.clear()
                        checked_bad += len(bad)

                        if ok:
                            empty_wa_batches = 0
                            for p in ok:
                                if delivered < target:
                                    delivered += 1
                                    yield sse("item", {"phone": p, "has_whatsapp": True})
                                    if delivered >= target:
                                        break
                        else:
                            empty_wa_batches += 1
                            if empty_wa_batches >= 3:
                                # cidade ficou fraca em WA -> passa para a próxima
                                break

                        yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched})
                        if delivered >= target:
                            break

                # flush final da cidade
                if somente_wa and raw_pool and delivered < target:
                    ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                    raw_pool.clear()
                    checked_bad += len(bad)
                    if ok:
                        for p in ok:
                            if delivered < target:
                                delivered += 1
                                yield sse("item", {"phone": p, "has_whatsapp": True})
                                if delivered >= target:
                                    break
                    yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched})

        except Exception as e:
            yield sse("progress", {"error": str(e), "wa_count": delivered, "non_wa_count": checked_bad, "searched": searched})

        exhausted = delivered < target
        yield sse("done", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched, "exhausted": exhausted})

    return StreamingResponse(gen(), media_type="text/event-stream")

# =============== JSON (fallback) ===============
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    cidades = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    delivered: List[str] = []
    checked_bad = 0
    searched = 0
    seen = set()

    batch_sz, _ = _effective_sizes(target)

    try:
        for cidade in cidades:
            if len(delivered) >= target:
                break

            scrape_cap = 0 if somente_wa else (target - len(delivered))

            raw_pool: List[str] = []
            empty_wa_batches = 0

            async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                if len(delivered) >= target:
                    break
                if ph in seen:
                    continue
                seen.add(ph)
                searched += 1

                if not somente_wa:
                    if len(delivered) < target:
                        delivered.append(ph)
                    if len(delivered) >= target:
                        break
                    continue

                raw_pool.append(ph)
                if len(raw_pool) >= batch_sz:
                    ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                    raw_pool.clear()
                    checked_bad += len(bad)

                    if ok:
                        empty_wa_batches = 0
                        for p in ok:
                            if len(delivered) < target:
                                delivered.append(p)
                                if len(delivered) >= target:
                                    break
                    else:
                        empty_wa_batches += 1
                        if empty_wa_batches >= 3:
                            break

            if somente_wa and raw_pool and len(delivered) < target:
                ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                raw_pool.clear()
                checked_bad += len(bad)
                for p in ok:
                    if len(delivered) < target:
                        delivered.append(p)
                        if len(delivered) >= target:
                            break

    except Exception:
        pass

    items = [{"phone": p, "has_whatsapp": bool(verify)} for p in delivered[:target]]
    return JSONResponse({"items": items, "leads": items, "wa_count": len(delivered), "non_wa_count": checked_bad, "searched": searched})

# =============== CSV ===============
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
