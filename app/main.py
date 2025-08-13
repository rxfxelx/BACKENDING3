import json
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.4.1")

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

        delivered = 0                 # << só o que foi ENTREGUE ao front
        checked_bad = 0               # verificados sem WhatsApp
        searched = 0
        seen = set()

        MAX_ATTEMPTS = 3
        attempt = 1
        while delivered < target and attempt <= MAX_ATTEMPTS:
            raw_pool: List[str] = []
            new_this_attempt = 0
            pages = settings.MAX_PAGES_PER_QUERY * attempt  # aprofunda a cada passada

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
                                "wa_count": delivered,         # barra do front usa este campo
                                "non_wa_count": checked_bad,
                                "searched": searched
                            })
                            if delivered >= target:
                                break
                        continue

                    # Somente WA: acumula e verifica em lote
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
                            "wa_count": delivered,     # agora reflete ENTREGUES
                            "non_wa_count": checked_bad,
                            "searched": searched
                        })
                        if delivered >= target:
                            break

                # Flush final da passada
                if somente_wa and raw_pool and delivered < target:
                    ok, bad = await verify_batch(raw_pool)
                    checked_bad += len(bad)
                    for p in ok:
                        if delivered < target:
                            delivered += 1
                            yield sse_format("item", {"phone": p})

                # Progresso da tentativa
                yield sse_format("progress", {
                    "attempt": attempt,
                    "wa_count": delivered,
                    "non_wa_count": checked_bad,
                    "searched": searched
                })

            except Exception as e:
                yield sse_format("progress", {"attempt": attempt, "error": str(e)})

            # só tenta de novo se AINDA faltar entregar
            if delivered >= target or new_this_attempt == 0:
                break
            attempt += 1

        # Final: sempre libera exportação
        yield sse_format("done", {
            "wa_count": delivered,          # entregue ao usuário
            "non_wa_count": checked_bad,    # verificados sem WA
            "searched": searched,
            "exhausted": True               # sinaliza que terminou (alcançou n OU esgotou)
        })

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
