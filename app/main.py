import json
from io import StringIO
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response

from .config import settings
from .services.scraper import search_numbers
from .services.verifier import verify_batch

app = FastAPI(title="ClickLeads Backend", version="1.9.0")

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

def _effective_batch(target: int) -> int:
    if target <= 5: return 6
    if target <= 20: return 10
    if target <= 100: return 20
    return 30

# --- cidades por vírgula; ignora UF ---
_UF = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"}
def _parse_cidades(local: str) -> List[str]:
    raw = [x.strip() for x in (local or "").split(",")]
    out, seen = [], set()
    for t in raw:
        if not t: continue
        if t.upper() in _UF or len(t) <= 2:  # ignora “SP”, “MG”, etc.
            continue
        if t not in seen:
            seen.add(t); out.append(t)
    return out or [""]  # garante lista

# ===================== STREAM =====================
@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(..., ge=1, le=min(500, settings.MAX_RESULTS)),
    verify: int = Query(0),
):
    somente_wa = verify == 1
    cidades = _parse_cidades(local)
    target = n

    async def gen():
        yield sse("start", {"message": "started"})

        wa = 0                # quando somente_wa: conta APENAS WhatsApp
        non_wa = 0
        searched = 0
        vistos = set()
        batch_sz = _effective_batch(target)

        try:
            for cidade in cidades:
                if wa >= target and somente_wa: break

                # se não for somente_wa, só precisamos do que falta
                scrape_cap = 0 if somente_wa else max(0, target - wa)
                pool = []

                # 1) varre a CIDADE até o gerador ACABAR
                async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                    if not ph or ph in vistos:
                        continue
                    vistos.add(ph)
                    searched += 1

                    if not somente_wa:
                        wa += 1
                        yield sse("item", {"phone": ph})
                        yield sse("progress", {"wa_count": wa, "non_wa_count": non_wa, "searched": searched, "city": cidade})
                        if wa >= target:
                            break
                        continue

                    # somente_wa: acumula para verificar
                    pool.append(ph)
                    if len(pool) >= batch_sz:
                        ok, bad = await verify_batch(pool, batch_size=batch_sz)
                        pool.clear()
                        non_wa += len(bad)
                        for p in ok:
                            if wa < target:
                                wa += 1
                                yield sse("item", {"phone": p, "has_whatsapp": True})
                                if wa >= target: break
                        yield sse("progress", {"wa_count": wa, "non_wa_count": non_wa, "searched": searched, "city": cidade})
                        if wa >= target:
                            break

                # 2) flush final da CIDADE (acabou a cidade)
                if somente_wa and pool and wa < target:
                    ok, bad = await verify_batch(pool, batch_size=batch_sz)
                    pool.clear()
                    non_wa += len(bad)
                    for p in ok:
                        if wa < target:
                            wa += 1
                            yield sse("item", {"phone": p, "has_whatsapp": True})
                            if wa >= target: break
                    yield sse("progress", {"wa_count": wa, "non_wa_count": non_wa, "searched": searched, "city": cidade})

                # aqui a cidade ACABOU. se ainda faltar, segue para a PRÓXIMA
                if (somente_wa and wa >= target) or (not somente_wa and wa >= target):
                    break

        except Exception as e:
            yield sse("progress", {"error": str(e), "wa_count": wa, "non_wa_count": non_wa, "searched": searched})

        exhausted = (wa < target)  # se não bateu meta e não há mais cidades/itens
        yield sse("done", {"wa_count": wa, "non_wa_count": non_wa, "searched": searched, "exhausted": exhausted})

    return StreamingResponse(gen(), media_type="text/event-stream")

# ===================== JSON (fallback) =====================
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

    itens: List[str] = []
    wa = 0
    non_wa = 0
    searched = 0
    vistos = set()
    batch_sz = _effective_batch(target)

    try:
        for cidade in cidades:
            if wa >= target and somente_wa: break

            scrape_cap = 0 if somente_wa else max(0, target - wa)
            pool = []

            async for ph in search_numbers(nicho, [cidade], scrape_cap, max_pages=None):
                if not ph or ph in vistos:
                    continue
                vistos.add(ph)
                searched += 1

                if not somente_wa:
                    itens.append(ph); wa += 1
                    if wa >= target: break
                    continue

                pool.append(ph)
                if len(pool) >= batch_sz:
                    ok, bad = await verify_batch(pool, batch_size=batch_sz)
                    pool.clear()
                    non_wa += len(bad)
                    for p in ok:
                        if wa < target:
                            itens.append(p); wa += 1
                            if wa >= target: break
                    if wa >= target: break

            if somente_wa and pool and wa < target:
                ok, bad = await verify_batch(pool, batch_size=batch_sz)
                pool.clear()
                non_wa += len(bad)
                for p in ok:
                    if wa < target:
                        itens.append(p); wa += 1
                        if wa >= target: break

            if (somente_wa and wa >= target) or (not somente_wa and wa >= target):
                break

    except Exception:
        pass

    data = [{"phone": p, "has_whatsapp": bool(verify)} for p in itens[:target]]
    return JSONResponse({
        "items": data,
        "leads": data,
        "wa_count": wa,
        "non_wa_count": non_wa,
        "searched": searched
    })

# ===================== CSV =====================
def _csv_response(csv_bytes: bytes, filename: str) -> Response:
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
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
