import json
from io import StringIO
from typing import List, Tuple
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.4.3")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

def sse_format(event: str, data: dict) -> str:
    # SSE: linhas "event:" e "data:" separadas por linha em branco
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

# ---------- STREAM ----------
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

    async def event_gen():
        yield sse_format("start", {"message": "started"})

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
                            yield sse_format("item", {"phone": ph})
                            yield sse_format("progress", {
                                "attempt": attempt,
                                "wa_count": delivered,
                                "non_wa_count": checked_bad,
                                "searched": searched
                            })
                        if delivered >= target:
                            break
                        continue

                    raw_pool.append(ph)
                    if len(raw_pool) >= settings.UAZAPI_BATCH_SIZE:
                        ok, bad = await verify_batch(raw_pool)
                        raw_pool.clear()
                        checked_bad += len(bad)
                        for p in ok:
                            if delivered < target:
                                delivered += 1
                                yield sse_format("item", {"phone": p})
                                if delivered >= target:
                                    break
                        yield sse_format("progress", {
                            "attempt": attempt,
                            "wa_count": delivered,
                            "non_wa_count": checked_bad,
                            "searched": searched
                        })
                        if delivered >= target:
                            break

                if somente_wa and raw_pool and delivered < target:
                    ok, bad = await verify_batch(raw_pool)
                    checked_bad += len(bad)
                    for p in ok:
                        if delivered < target:
                            delivered += 1
                            yield sse_format("item", {"phone": p})

                yield sse_format("progress", {
                    "attempt": attempt,
                    "wa_count": delivered,
                    "non_wa_count": checked_bad,
                    "searched": searched
                })

            except Exception as e:
                yield sse_format("progress", {"attempt": attempt, "error": str(e)})

            if delivered >= target or new_this_attempt == 0:
                break
            attempt += 1

        exhausted = delivered < target
        yield sse_format("done", {
            "wa_count": delivered,
            "non_wa_count": checked_bad,
            "searched": searched,
            "exhausted": exhausted,
            "ready_to_export": True
        })

    return StreamingResponse(event_gen(), media_type="text/event-stream")

# ---------- JSON (fallback) ----------
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
            if len(raw_pool) >= settings.UAZAPI_BATCH_SIZE:
                ok, bad = await verify_batch(raw_pool)
                raw_pool.clear()
                checked_bad += len(bad)
                for p in ok:
                    if len(delivered) < target:
                        delivered.append(p)
                if len(delivered) >= target:
                    break

        if somente_wa and raw_pool and len(delivered) < target:
            ok, bad = await verify_batch(raw_pool)
            checked_bad += len(bad)
            for p in ok:
                if len(delivered) < target:
                    delivered.append(p)

        if len(delivered) >= target or new_this_attempt == 0:
            break
        attempt += 1

    items = [{"phone": p} for p in delivered[:target]]
    return JSONResponse({
        "leads": items,
        "wa_count": len(delivered),
        "non_wa_count": checked_bad,
        "searched": searched
    })

# ---------- CSV helpers ----------
async def _make_csv(nicho: str, local: str, n: int, verify: int) -> Tuple[bytes, str]:
    resp = await leads(nicho=nicho, local=local, n=n, verify=verify)
    payload = json.loads(resp.body)
    phones = [row["phone"] for row in payload.get("leads", [])]
    buf = StringIO()
    buf.write("phone\n")
    for p in phones:
        buf.write(str(p).strip() + "\n")
    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f"leads_{nicho.strip().replace(' ','_')}_{local.strip().replace(' ','_')}.csv"
    return csv_bytes, filename

def _csv_response(csv_bytes: bytes, filename: str) -> Response:
    # Content-Disposition: attachment -> força download no browser
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ---------- EXPORT (compatível com qualquer front) ----------
@app.get("/leads/csv")
async def leads_csv_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    csv_bytes, filename = await _make_csv(nicho, local, n, verify)
    return _csv_response(csv_bytes, filename)

@app.get("/leads/export")
async def leads_export_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    csv_bytes, filename = await _make_csv(nicho, local, n, verify)
    return _csv_response(csv_bytes, filename)

@app.get("/export")
async def export_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    csv_bytes, filename = await _make_csv(nicho, local, n, verify)
    return _csv_response(csv_bytes, filename)

@app.post("/leads/export")
async def leads_export_post(req: Request):
    data = await req.json()
    csv_bytes, filename = await _make_csv(
        nicho=str(data.get("nicho", "")),
        local=str(data.get("local", "")),
        n=int(data.get("n", 0)),
        verify=int(data.get("verify", 0)),
    )
    return _csv_response(csv_bytes, filename)

@app.post("/export")
async def export_post(req: Request):
    data = await req.json()
    csv_bytes, filename = await _make_csv(
        nicho=str(data.get("nicho", "")),
        local=str(data.get("local", "")),
        n=int(data.get("n", 0)),
        verify=int(data.get("verify", 0)),
    )
    return _csv_response(csv_bytes, filename)
