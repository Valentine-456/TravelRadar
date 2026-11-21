from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=BACKEND_DIR / ".env", extra="ignore")

    mapbox_token: str = ""
    mapbox_style: str = "mapbox://styles/mapbox/dark-v11"
    static_dir: Path = BACKEND_DIR / "static"
    redis_host: str = "localhost"
    redis_port: int = 6379


@lru_cache
def get_settings() -> Settings:
    return Settings()
