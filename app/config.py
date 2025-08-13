from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    UAZAPI_CHECK_URL: str = "https://helsenia.uazapi.com/chat/check"
    UAZAPI_INSTANCE_TOKEN: str = ""
    HEADLESS: bool = True
    BROWSER: str = "chromium"  # chromium, firefox, webkit
    USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    MAX_RESULTS: int = 500
    PAGE_SIZE: int = 20  # google lcl step
    MAX_PAGES_PER_QUERY: int = 80
    UAZAPI_BATCH_SIZE: int = 50
    UAZAPI_MAX_CONCURRENCY: int = 3
    UAZAPI_RETRIES: int = 2
    UAZAPI_THROTTLE_MS: int = 120
    UAZAPI_TIMEOUT: float = 12.0

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
