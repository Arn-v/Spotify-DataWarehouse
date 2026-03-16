"""Application settings loaded from environment variables via Pydantic."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Spotify API
    spotify_client_id: str = ""
    spotify_client_secret: str = ""

    # Database
    database_url: str = "postgresql://spotify:spotify@localhost:5432/spotify_dw"

    # Logging
    log_level: str = "INFO"

    # Pipeline
    batch_size: int = 1000

    # Scheduler
    scheduler_interval_hours: int = 6


@lru_cache
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()
