import json, time
from io import StringIO
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.7.7")

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

# =============== STREAM (round-robin por cidades) ===============
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

    MAX_SEARCH_SECONDS = int(getattr(settings, "MAX_SEARCH_SECONDS", 900))
    CITY_SLICE_PHONES = int(getattr(settings, "CITY_SLICE_PHONES", 40))  # qtd nova por rodada/cidade

    async def gen():
        yield sse("start", {"message": "started"})
        t0 = time.monotonic()

        delivered = 0                  # quando verify=1: conta só WA
        checked_bad = 0
        searched_total = 0
        seen = set()

        raw_pool: List[str] = []
        batch_sz, _ = _effective_sizes(target)

        try:
            while delivered < target and (time.monotonic() - t0) < MAX_SEARCH_SECONDS:
                progressed_cycle = False

                for cidade in cidades:
                    if delivered >= target or (time.monotonic() - t0) >= MAX_SEARCH_SECONDS:
                        break

                    # fatia: coleta até CITY_SLICE_PHONES números novos dessa cidade
                    novos_da_cidade = 0
                    async for ph in search_numbers(nicho, [cidade], 0, max_pages=None):
                        if ph in seen:
                            continue
                        seen.add(ph)
                        searched_total += 1
                        progressed_cycle = True
                        novos_da_cidade += 1

                        if not somente_wa:
                            delivered += 1
                            yield sse("item", {"phone": ph})
                            yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})
                            if delivered >= target:
                                break
                        else:
                            raw_pool.append(ph)

                        if novos_da_cidade >= CITY_SLICE_PHONES or delivered >= target:
                            break

                    # verifica ao fim da fatia e segue para a próxima cidade
                    if somente_wa and raw_pool and delivered < target:
                        ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                        raw_pool.clear()
                        checked_bad += len(bad)
                        for p in ok:
                            if delivered < target:
                                delivered += 1
                                yield sse("item", {"phone": p, "has_whatsapp": True})
                                if delivered >= target:
                                    break
                        yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})
                        if delivered >= target:
                            break

                if not progressed_cycle:
                    break  # nada novo em nenhuma cidade -> esgotou

        except Exception as e:
            yield sse("progress", {"error": str(e), "wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})

        exhausted = delivered < target
        yield sse("done", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total, "exhausted": exhausted})

    return StreamingResponse(gen(), media_type="text/event-stream")

# =============== JSON (fallback; round-robin também) ===============
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

    MAX_SEARCH_SECONDS = int(getattr(settings, "MAX_SEARCH_SECONDS", 900))
    CITY_SLICE_PHONES = int(getattr(settings, "CITY_SLICE_PHONES", 40))

    t0 = time.monotonic()
    delivered: List[str] = []
    checked_bad = 0
    searched_total = 0
    seen = set()

    raw_pool: List[str] = []
    batch_sz, _ = _effective_sizes(target)

    try:
        while len(delivered) < target and (time.monotonic() - t0) < MAX_SEARCH_SECONDS:
            progressed_cycle = False

            for cidade in cidades:
                if len(delivered) >= target or (time.monotonic() - t0) >= MAX_SEARCH_SECONDS:
                    break

                novos_da_cidade = 0
                async for ph in search_numbers(nicho, [cidade], 0, max_pages=None):
                    if ph in seen:
                        continue
                    seen.add(ph)
                    searched_total += 1
                    progressed_cycle = True
                    novos_da_cidade += 1

                    if not somente_wa:
                        if len(delivered) < target:
                            delivered.append(ph)
                        if len(delivered) >= target:
                            break
                    else:
                        raw_pool.append(ph)

                    if novos_da_cidade >= CITY_SLICE_PHONES or len(delivered) >= target:
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
                if len(delivered) >= target:
                    break

            if not progressed_cycle:
                break
    except Exception:
        pass

    items = [{"phone": p, "has_whatsapp": bool(verify)} for p in delivered[:target]]
    return JSONResponse({"items": items, "leads": items, "wa_count": len(delivered), "non_wa_count": checked_bad, "searched": searched_total})

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
