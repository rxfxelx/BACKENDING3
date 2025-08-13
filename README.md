# ClickLeads Backend

Endpoints esperados pelo seu frontend:
- `GET /health`
- `GET /leads?nicho=...&local=...&n=...&verify=0|1`
- `GET /leads/stream?nicho=...&local=...&n=...&verify=0|1` (SSE com eventos: `start`, `progress`, `item`, `done`)

## Execução local

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

`.env` (opcional):
```
UAZAPI_CHECK_URL=https://helsenia.uazapi.com/chat/check
UAZAPI_INSTANCE_TOKEN=55b903f3-9c7c-4457-9ffd-7296a35d832e
HEADLESS=1
UAZAPI_BATCH_SIZE=50
UAZAPI_MAX_CONCURRENCY=3
UAZAPI_RETRIES=2
UAZAPI_THROTTLE_MS=120
UAZAPI_TIMEOUT=12
```

## Como funciona
- Scraping via Playwright no Google Local (`tbm=lcl`) paginando por `start`=0,20,40...
- Extração de telefones via regex BR e normalização para `+55DDDNXXXXXXXX`.
- Verificação WhatsApp via UAZAPI em lotes com **concorrência** controlada.
- Lógica "completar até bater a meta": quando `verify=1`, busca mais páginas até atingir `n` números **válidos no WhatsApp** ou esgotar resultados.
- SSE envia:
  - `progress`: `{ wa_count, non_wa_count, searched }`
  - `item`: `{ phone }`
  - `done`: `{ wa_count, non_wa_count, searched, exhausted }`

> Observação: Scraping de resultados do Google pode violar termos. Use por sua conta e risco e adicione backoff se notar bloqueios.

## Implantação
- Railway/Fly/Render: lembre de rodar `playwright install chromium` no build. Em Docker:
```dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt &&     python -m playwright install-deps &&     python -m playwright install chromium

COPY app ./app
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8080"]
```
