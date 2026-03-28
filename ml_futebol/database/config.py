import os
from dotenv import load_dotenv

load_dotenv(encoding="utf-8")


class Settings:
    POSTGRES_DB = os.getenv("POSTGRES_DB", "ml_futebol")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "55432"))

    @property
    def conninfo(self) -> str:
        return (
            f"dbname={self.POSTGRES_DB} "
            f"user={self.POSTGRES_USER} "
            f"password={self.POSTGRES_PASSWORD} "
            f"host={self.POSTGRES_HOST} "
            f"port={self.POSTGRES_PORT}"
        )


settings = Settings()