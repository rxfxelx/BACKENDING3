import json
from io import StringIO
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.6.1")

# CORS correto: passa a CLASSE, não a instância
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
    if target <= 1:   return 8, 12
    if target <= 3:   return 8, max(18, target * 8)
    if target <= 10:  return 12, max(40, target * 6)
    if target <= 50:  return 16, max(120, target * 4)
    return min(24, settings.UAZAPI_BATCH_SIZE), max(200, target * 3)

# ------------------------ STREAM ------------------------
@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    locais = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    async def gen():
        yield sse("start", {"message": "started"})

        delivered = 0
        checked_bad = 0
        searched = 0
        seen = set()

        MAX_ATTEMPTS = 3
        attempt = 1
        while delivered < target and attempt <= MAX_ATTEMPTS:
            raw_pool: List[str] = []
            new_this_attempt = 0
            pages = settings.MAX_PAGES_PER_QUERY * attempt
            batch_sz, pool_cap = _effective_sizes(target)

            try:
                async for ph in search_numbers(nicho, locais, target, max_pages=pages):
                    if ph in seen:
                        continue
                    seen.add(ph)
                    new_this_attempt += 1
                    searched += 1

                    if not somente_wa:
                        if delivered < target:
                            delivered += 1
                            yield sse("item", {"phone": ph})
                            yield sse("progress", {
                                "attempt": attempt,
                                "wa_count": delivered,
                                "non_wa_count": checked_bad,
                                "searched": searched
                            })
                        if delivered >= target:
                            break
                        continue

                    raw_pool.append(ph)
                    if len(raw_pool) >= batch_sz or len(raw_pool) >= pool_cap:
                        ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                        raw_pool.clear()
                        checked_bad += len(bad)
                        for p in ok:
                            if delivered < target:
                                delivered += 1
                                yield sse("item", {"phone": p, "has_whatsapp": True})
                                if delivered >= target:
                                    break
                        yield sse("progress", {
                            "attempt": attempt,
                            "wa_count": delivered,
                            "non_wa_count": checked_bad,
                            "searched": searched
                        })
                        if delivered >= target:
                            break

                if somente_wa and raw_pool and delivered < target:
                    ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                    checked_bad += len(bad)
                    for p in ok:
                        if delivered < target:
                            delivered += 1
                            yield sse("item", {"phone": p, "has_whatsapp": True})

                yield sse("progress", {
                    "attempt": attempt,
                    "wa_count": delivered,
                    "non_wa_count": checked_bad,
                    "searched": searched
                })

            except Exception as e:
                yield sse("progress", {"attempt": attempt, "error": str(e)})

            if delivered >= target or new_this_attempt == 0:
                break
            attempt += 1

        exhausted = delivered < target
        yield sse("done", {
            "wa_count": delivered,
            "non_wa_count": checked_bad,
            "searched": searched,
            "exhausted": exhausted
        })

    return StreamingResponse(gen(), media_type="text/event-stream")

# ------------------------ JSON (fallback) ------------------------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    locais = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    delivered: List[str] = []
    checked_bad = 0
    searched = 0
    seen = set()

    MAX_ATTEMPTS = 3
    attempt = 1
    while len(delivered) < target and attempt <= MAX_ATTEMPTS:
        raw_pool: List[str] = []
        new_this_attempt = 0
        pages = settings.MAX_PAGES_PER_QUERY * attempt
        batch_sz, pool_cap = _effective_sizes(target)

        async for ph in search_numbers(nicho, locais, target, max_pages=pages):
            if ph in seen:
                continue
            seen.add(ph)
            new_this_attempt += 1
            searched += 1

            if not somente_wa:
                if len(delivered) < target:
                    delivered.append(ph)
                if len(delivered) >= target:
                    break
                continue

            raw_pool.append(ph)
            if len(raw_pool) >= batch_sz or len(raw_pool) >= pool_cap:
                ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                raw_pool.clear()
                checked_bad += len(bad)
                for p in ok:
                    if len(delivered) < target:
                        delivered.append(p)
                if len(delivered) >= target:
                    break

        if somente_wa and raw_pool and len(delivered) < target:
            ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
            checked_bad += len(bad)
            for p in ok:
                if len(delivered) < target:
                    delivered.append(p)

        if len(delivered) >= target or new_this_attempt == 0:
            break
        attempt += 1

    items = [{"phone": p, "has_whatsapp": bool(verify)} for p in delivered[:target]]
    return JSONResponse({
        "items": items,
        "leads": items,
        "wa_count": len(delivered),
        "non_wa_count": checked_bad,
        "searched": searched
    })

# ------------------------ CSV (opcional) ------------------------
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
    buf = StringIO()
    buf.write("phone\n")
    for p in phones:
        buf.write(str(p).strip() + "\n")
    csv = buf.getvalue().encode("utf-8")
    filename = f"leads_{nicho.strip().replace(' ','_')}_{local.strip().replace(' ','_')}.csv"
    return _csv_response(csv, filename)
