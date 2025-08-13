# Railway deploy

1) Variables (.env/Project Variables)
- UAZAPI_CHECK_URL=https://helsenia.uazapi.com/chat/check
- UAZAPI_INSTANCE_TOKEN=55b903f3-9c7c-4457-9ffd-7296a35d832e
- HEADLESS=1

2) Deploy
- Ensure `requirements.txt` is detected
- Procfile sets start to `bash start.sh` which installs Playwright and runs Uvicorn

3) Health check
- GET /health
