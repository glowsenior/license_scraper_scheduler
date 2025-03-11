from typing import List
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ORIGINS: List[str] = ["*"]
    TWO_CAPTCHA_API_KEY: str
    ANTI_CAPTCHA_API_KEY: str
    PROXY_URL: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings():
    return Settings()