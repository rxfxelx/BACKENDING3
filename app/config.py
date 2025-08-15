from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Auth/crypto
    JWT_SECRET: str = "helsenia_jwt_secret"   # usa sua env
    AUTH_SECRET: str | None = None            # compat, se existir
    @property
    def SECRET(self) -> str:
        return self.AUTH_SECRET or self.JWT_SECRET

    # UAZAPI
    UAZAPI_CHECK_URL: str = "https://hia-clientes.uazapi.com/chat/check"
    UAZAPI_INSTANCE_TOKEN: str = ""

    # Scraper
    HEADLESS: bool = True
    BROWSER: str = "chromium"
    USER_AGENT: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36")
    MAX_RESULTS: int = 500
    PAGE_SIZE: int = 20
    MAX_PAGES_PER_QUERY: int = 1000  # da sua env

    # Verifier
    UAZAPI_BATCH_SIZE: int = 50
    UAZAPI_MAX_CONCURRENCY: int = 2
    UAZAPI_RETRIES: int = 3
    UAZAPI_THROTTLE_MS: int = 250
    UAZAPI_TIMEOUT: float = 15.0

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
