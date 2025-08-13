import json
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.2.0")

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
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

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

        wa_count = 0
        non_wa_count = 0
        searched = 0
        yielded = 0

        raw_pool: List[str] = []
        seen = set()

        try:
            async for ph in search_numbers(nicho, locais, target):
                if ph in seen:
                    continue
                seen.add(ph)
                searched += 1
                raw_pool.append(ph)

                if not somente_wa:
                    if yielded < target:
                        yielded += 1
                        yield sse_format("item", {"phone": ph})
                    yield sse_format("progress", {"wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched})
                    if yielded >= target:
                        break
                    continue

                # Somente WhatsApp: verifica por lote quando atingir o batch
                if len(raw_pool) >= settings.UAZAPI_BATCH_SIZE:
                    ok, bad = await verify_batch(raw_pool)
                    raw_pool.clear()
                    wa_count += len(ok)
                    non_wa_count += len(bad)
                    for p in ok:
                        if yielded < target:
                            yielded += 1
                            yield sse_format("item", {"phone": p})
                            if yielded >= target:
                                break
                    yield sse_format("progress", {"wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched})
                    if yielded >= target:
                        break

            # esgotou scraping → verifica o que restou
            if somente_wa and raw_pool and yielded < target:
                ok, bad = await verify_batch(raw_pool)
                wa_count += len(ok)
                non_wa_count += len(bad)
                for p in ok:
                    if yielded < target:
                        yielded += 1
                        yield sse_format("item", {"phone": p})

            exhausted = yielded < target
            yield sse_format("done", {"wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched, "exhausted": exhausted})

        except Exception as e:
            yield sse_format("done", {"error": str(e), "wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched, "exhausted": False})

    return StreamingResponse(event_gen(), media_type="text/event-stream")

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

    wa_count = 0
    non_wa_count = 0
    searched = 0
    yielded: List[str] = []
    raw_pool: List[str] = []
    seen = set()

    try:
        async for ph in search_numbers(nicho, locais, target):
            if ph in seen:
                continue
            seen.add(ph)
            searched += 1
            raw_pool.append(ph)

            if not somente_wa:
                if len(yielded) < target:
                    yielded.append(ph)
                if len(yielded) >= target:
                    break
                continue

            if len(raw_pool) >= settings.UAZAPI_BATCH_SIZE:
                ok, bad = await verify_batch(raw_pool)
                raw_pool.clear()
                wa_count += len(ok)
                non_wa_count += len(bad)
                for p in ok:
                    if len(yielded) < target:
                        yielded.append(p)
                if len(yielded) >= target:
                    break

        if somente_wa and raw_pool and len(yielded) < target:
            ok, bad = await verify_batch(raw_pool)
            wa_count += len(ok)
            non_wa_count += len(bad)
            for p in ok:
                if len(yielded) < target:
                    yielded.append(p)

        items = [{"phone": p} for p in yielded[:target]]
        return JSONResponse({"leads": items, "wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched})
    except Exception as e:
        return JSONResponse({"error": str(e), "leads": [], "wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched}, status_code=500)
