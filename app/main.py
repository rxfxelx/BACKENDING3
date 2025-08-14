import json, time
from io import StringIO
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.7.6")

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

# ================= STREAM =================
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

    # guardas (podem ser definidos no .env; se não, usa defaults)
    MAX_SEARCH_SECONDS = int(getattr(settings, "MAX_SEARCH_SECONDS", 480))   # total
    MAX_NO_WA_ROUNDS   = int(getattr(settings, "MAX_NO_WA_ROUNDS", 6))       # lotes seguidos sem WA
    CITY_STALL_DELTA   = int(getattr(settings, "CITY_STALL_DELTA", 220))     # telefones sem crescer WA
    CITY_MAX_SECONDS   = int(getattr(settings, "CITY_MAX_SECONDS", 240))     # tempo por cidade
    VERIFY_EVERY_SEC   = int(getattr(settings, "VERIFY_EVERY_SECONDS", 8))   # verificação por tempo

    async def gen():
        yield sse("start", {"message": "started"})

        t0_total = time.monotonic()
        delivered = 0            # quando verify=1: conta APENAS WhatsApp
        checked_bad = 0
        searched_total = 0
        seen = set()

        raw_pool: List[str] = []
        batch_sz, pool_cap = _effective_sizes(target)

        try:
            for cidade in cidades:
                if time.monotonic() - t0_total > MAX_SEARCH_SECONDS or delivered >= target:
                    break

                t0_city = time.monotonic()
                city_no_wa_rounds = 0
                city_since_last_wa = 0
                last_verify_ts = time.monotonic()

                # sem teto de scraping quando somente_wa
                scrape_cap = 0 if somente_wa else (target - delivered)

                async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                    if time.monotonic() - t0_total > MAX_SEARCH_SECONDS or delivered >= target:
                        break

                    # limite por cidade
                    if somente_wa and time.monotonic() - t0_city > CITY_MAX_SECONDS:
                        break

                    if ph in seen:
                        continue
                    seen.add(ph)
                    searched_total += 1
                    city_since_last_wa += 1

                    if not somente_wa:
                        delivered += 1
                        yield sse("item", {"phone": ph})
                        yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})
                        if delivered >= target:
                            break
                        continue

                    # Somente WhatsApp: acumula e verifica por tamanho/tempo
                    raw_pool.append(ph)

                    # cidade “estagnada”: muitos telefones sem aumentar WA -> troca de cidade
                    if city_since_last_wa >= CITY_STALL_DELTA:
                        break

                    # gatilho por tempo
                    if raw_pool and (time.monotonic() - last_verify_ts) >= VERIFY_EVERY_SEC:
                        ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                        raw_pool.clear()
                        last_verify_ts = time.monotonic()
                        checked_bad += len(bad)

                        if ok:
                            city_no_wa_rounds = 0
                            for p in ok:
                                if delivered < target:
                                    delivered += 1
                                    city_since_last_wa = 0
                                    yield sse("item", {"phone": p, "has_whatsapp": True})
                                    if delivered >= target:
                                        break
                        else:
                            city_no_wa_rounds += 1
                            if city_no_wa_rounds >= MAX_NO_WA_ROUNDS:
                                break

                        yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})
                        if delivered >= target:
                            break

                    # gatilho por lote
                    if len(raw_pool) >= batch_sz or len(raw_pool) >= pool_cap:
                        ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                        raw_pool.clear()
                        last_verify_ts = time.monotonic()
                        checked_bad += len(bad)

                        if ok:
                            city_no_wa_rounds = 0
                            for p in ok:
                                if delivered < target:
                                    delivered += 1
                                    city_since_last_wa = 0
                                    yield sse("item", {"phone": p, "has_whatsapp": True})
                                    if delivered >= target:
                                        break
                        else:
                            city_no_wa_rounds += 1
                            if city_no_wa_rounds >= MAX_NO_WA_ROUNDS:
                                break

                        yield sse("progress", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})
                        if delivered >= target:
                            break

                # flush da cidade
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

        except Exception as e:
            yield sse("progress", {"error": str(e), "wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total})

        exhausted = delivered < target
        yield sse("done", {"wa_count": delivered, "non_wa_count": checked_bad, "searched": searched_total, "exhausted": exhausted})

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
    cidades = [x.strip() for x in local.split(",") if x.strip()]
    target = n

    MAX_SEARCH_SECONDS = int(getattr(settings, "MAX_SEARCH_SECONDS", 480))
    MAX_NO_WA_ROUNDS   = int(getattr(settings, "MAX_NO_WA_ROUNDS", 6))
    CITY_STALL_DELTA   = int(getattr(settings, "CITY_STALL_DELTA", 220))
    CITY_MAX_SECONDS   = int(getattr(settings, "CITY_MAX_SECONDS", 240))
    VERIFY_EVERY_SEC   = int(getattr(settings, "VERIFY_EVERY_SECONDS", 8))

    t0_total = time.monotonic()
    delivered: List[str] = []
    checked_bad = 0
    searched_total = 0
    seen = set()

    raw_pool: List[str] = []
    batch_sz, pool_cap = _effective_sizes(target)

    for cidade in cidades:
        if time.monotonic() - t0_total > MAX_SEARCH_SECONDS or len(delivered) >= target:
            break

        t0_city = time.monotonic()
        city_no_wa_rounds = 0
        city_since_last_wa = 0
        last_verify_ts = time.monotonic()
        scrape_cap = 0 if somente_wa else (target - len(delivered))

        async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
            if time.monotonic() - t0_total > MAX_SEARCH_SECONDS or len(delivered) >= target:
                break
            if somente_wa and time.monotonic() - t0_city > CITY_MAX_SECONDS:
                break

            if ph in seen:
                continue
            seen.add(ph)
            searched_total += 1
            city_since_last_wa += 1

            if not somente_wa:
                delivered.append(ph)
                if len(delivered) >= target:
                    break
                continue

            raw_pool.append(ph)

            if city_since_last_wa >= CITY_STALL_DELTA:
                break

            if raw_pool and (time.monotonic() - last_verify_ts) >= VERIFY_EVERY_SEC:
                ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                raw_pool.clear()
                last_verify_ts = time.monotonic()
                checked_bad += len(bad)

                for p in ok:
                    if len(delivered) < target:
                        delivered.append(p)
                        city_since_last_wa = 0
                if not ok:
                    city_no_wa_rounds += 1
                    if city_no_wa_rounds >= MAX_NO_WA_ROUNDS:
                        break
                if len(delivered) >= target:
                    break

            if len(raw_pool) >= batch_sz or len(raw_pool) >= pool_cap:
                ok, bad = await verify_batch(raw_pool, batch_size=batch_sz)
                raw_pool.clear()
                last_verify_ts = time.monotonic()
                checked_bad += len(bad)

                for p in ok:
                    if len(delivered) < target:
                        delivered.append(p)
                        city_since_last_wa = 0
                if not ok:
                    city_no_wa_rounds += 1
                    if city_no_wa_rounds >= MAX_NO_WA_ROUNDS:
                        break
                if len(delivered) >= target:
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

    items = [{"phone": p, "has_whatsapp": bool(verify)} for p in delivered[:target]]
    return JSONResponse({"items": items, "leads": items, "wa_count": len(delivered), "non_wa_count": checked_bad, "searched": searched_total})

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
