# app/main.py
import json
from io import StringIO
from typing import List, Optional
from fastapi import FastAPI, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.5.1")

# expõe Content-Disposition (se usar /export)
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
    # SSE = linhas "event:" e "data:" separadas por linha em branco (UTF-8). :contentReference[oaicite:0]{index=0}
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

# ------------------------ STREAM (usado pelo seu JS) ------------------------
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

        delivered = 0        # entregues ao cliente
        checked_bad = 0      # verificados sem WA
        searched = 0         # números coletados do Google (antes do filtro)
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
                            # seu JS aceita sem has_whatsapp quando verify=0
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

                    # Somente WhatsApp: verifica em lote
                    raw_pool.append(ph)
                    if len(raw_pool) >= settings.UAZAPI_BATCH_SIZE:
                        ok, bad = await verify_batch(raw_pool)
                        raw_pool.clear()
                        checked_bad += len(bad)
                        for p in ok:
                            if delivered < target:
                                delivered += 1
                                # >>> chave que seu JS exige quando Somente WhatsApp
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

                # flush final desta passada
                if somente_wa and raw_pool and delivered < target:
                    ok, bad = await verify_batch(raw_pool)
                    checked_bad += len(bad)
                    for p in ok:
                        if delivered < target:
                            delivered += 1
                            yield sse("item", {"phone": p, "has_whatsapp": True})

                # progresso da tentativa
                yield sse("progress", {
                    "attempt": attempt,
                    "wa_count": delivered,
                    "non_wa_count": checked_bad,
                    "searched": searched
                })

            except Exception as e:
                yield sse("progress", {"attempt": attempt, "error": str(e)})

            # só tenta de novo se ainda faltar
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

# ------------------------ JSON (fallback do seu JS) ------------------------
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

    # >>> seu fallback lê "items" e já filtra pelo has_whatsapp
    items = [{"phone": p, "has_whatsapp": bool(verify)} for p in delivered[:target]]
    return JSONResponse({
        "items": items,
        "leads": items,
        "wa_count": len(delivered),
        "non_wa_count": checked_bad,
        "searched": searched
    })

# ------------------------ Export opcional (não usado pelo seu JS) ------------------------
def _csv_response(csv_bytes: bytes, filename: str) -> Response:
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )

async def _csv_from_params(nicho: str, local: str, n: int, verify: int) -> Response:
    resp = await leads(nicho=nicho, local=local, n=n, verify=verify)
    payload = json.loads(resp.body)
    phones = [row["phone"] for row in payload.get("items", [])]
    buf = StringIO(); buf.write("phone\n")
    for p in phones: buf.write(str(p).strip() + "\n")
    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f"leads_{nicho.strip().replace(' ','_')}_{local.strip().replace(' ','_')}.csv"
    return _csv_response(csv_bytes, filename)

@app.get("/export")
async def export_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    return await _csv_from_params(nicho, local, n, verify)

@app.get("/leads/export")
async def leads_export_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    return await _csv_from_params(nicho, local, n, verify)

@app.get("/leads/csv")
async def leads_csv_get(nicho: str = Query(...), local: str = Query(...), n: int = Query(...), verify: int = Query(0)):
    return await _csv_from_params(nicho, local, n, verify)
