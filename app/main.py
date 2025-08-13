import asyncio, json
from typing import List
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.0.0")

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
async def leads_stream(nicho: str = Query(...), local: str = Query(...),
                       n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
                       verify: int = Query(0)):
    somente_wa = verify == 1
    locais = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    async def event_gen():
        wa_count = 0
        non_wa_count = 0
        searched = 0
        yielded = 0
        buffer: List[str] = []

        yield sse_format("start", {"message": "started"})
        try:
            async for ph in search_numbers(nicho, locais, target):
                searched += 1
                buffer.append(ph)
                # flush in batches to verifier or emit directly if not verifying
                flush = len(buffer) >= max(10, settings.UAZAPI_BATCH_SIZE//2)
                done = False

                if somente_wa and flush:
                    ok, bad = await verify_batch(buffer)
                    buffer.clear()
                    wa_count += len(ok)
                    non_wa_count += len(bad)
                    for p in ok:
                        yielded += 1
                        yield sse_format("item", {"phone": p})
                        if yielded >= target:
                            done = True
                            break
                elif not somente_wa:
                    # emit directly, but still optionally verify for counters
                    while buffer and yielded < target:
                        p = buffer.pop(0)
                        yielded += 1
                        yield sse_format("item", {"phone": p})
                    # Optionally verify a small sample to populate counters
                    if flush:
                        ok, bad = await verify_batch(buffer[:settings.UAZAPI_BATCH_SIZE])
                        wa_count += len(ok)
                        non_wa_count += len(bad)
                        buffer.clear()

                # progress
                yield sse_format("progress", {"wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched})

                if done:
                    break

            exhausted = yielded < target
            yield sse_format("done", {"wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched, "exhausted": exhausted})
        except Exception as e:
            yield sse_format("done", {"error": str(e), "wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched, "exhausted": False})

    return StreamingResponse(event_gen(), media_type="text/event-stream")

@app.get("/leads")
async def leads(nicho: str = Query(...), local: str = Query(...),
                n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
                verify: int = Query(0)):
    somente_wa = verify == 1
    locais = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    wa_count = 0
    non_wa_count = 0
    searched = 0
    found: List[str] = []

    try:
        buffer: List[str] = []
        async for ph in search_numbers(nicho, locais, target):
            searched += 1
            buffer.append(ph)

            # Verify in batches or pass-through
            if somente_wa:
                if len(buffer) >= settings.UAZAPI_BATCH_SIZE:
                    ok, bad = await verify_batch(buffer)
                    wa_count += len(ok)
                    non_wa_count += len(bad)
                    buffer.clear()
                    for p in ok:
                        if len(found) < target:
                            found.append(p)
                if len(found) >= target:
                    break
            else:
                # not somente_wa: emit directly until target
                while buffer and len(found) < target:
                    found.append(buffer.pop(0))

        # Final flush
        if somente_wa and buffer and len(found) < target:
            ok, bad = await verify_batch(buffer)
            wa_count += len(ok)
            non_wa_count += len(bad)
            for p in ok:
                if len(found) < target:
                    found.append(p)

        items = [{"phone": p} for p in found[:target]]
        return JSONResponse({"leads": items, "wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched})
    except Exception as e:
        return JSONResponse({"error": str(e), "leads": [], "wa_count": wa_count, "non_wa_count": non_wa_count, "searched": searched}, status_code=500)
