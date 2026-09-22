import os
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic_settings import BaseSettings, SettingsConfigDict

# Защита от поврежденной переменной SSL_CERT_FILE (директория вместо файла на Windows)
if "SSL_CERT_FILE" in os.environ and not os.path.isfile(
    os.environ["SSL_CERT_FILE"]
):
    os.environ.pop("SSL_CERT_FILE", None)

# Базовый каталог бэкенда
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Путь к файлу переменных окружения
env_path = BASE_DIR / ".env"


class Settings(BaseSettings):
    """Класс настроек окружения на базе Pydantic Settings."""

    model_config = SettingsConfigDict(
        env_file=env_path, case_sensitive=False, extra="ignore"
    )

    PROJECT_NAME: str = "Support AI RAG Platform"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"
    TIMEZONE: ZoneInfo = ZoneInfo("Europe/Moscow")

    # PostgreSQL
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "rag_user"
    DB_PASS: str = "rag_password"
    DB_NAME: str = "rag_db"
    DB_ECHO: bool = False

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    CHAT_CONTEXT_TTL_SECONDS: int = 1800
    CHAT_CONTEXT_MAX_MESSAGES: int = 10

    # Qdrant
    QDRANT_HOST: str = "localhost"
    QDRANT_PORT: int = 6333
    QDRANT_GRPC_PORT: int = 6334
    QDRANT_COLLECTION_NAME: str = "tender_chunks"

    # CORS
    CORS_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]

    # JWT и безопасность
    JWT_SECRET_KEY: str = (
        "super-secret-jwt-key-for-dev-environment-change-in-prod"
    )
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 30

    # Хранилище документов базы знаний
    KB_STORAGE_DIR: Path = BASE_DIR / "storage" / "kb_documents"

    # Ollama и генеративный контур
    OLLAMA_BASE_URL: str = "http://192.168.31.214:11434"
    OLLAMA_MODEL: str = "qwen3.5:4b"
    OLLAMA_TIMEOUT_SECONDS: float = 60.0

    # Эмбеддинги (bge-m3 1024D)
    EMBEDDING_MODEL_NAME: str = "BAAI/bge-m3"
    EMBEDDING_DIM: int = 1024
    EMBEDDING_DEVICE: str = "cpu"
    ENABLE_LOCAL_NEURAL_EMBEDDINGS: bool = True

    @property
    def DATABASE_URL(self) -> str:
        """Строка подключения к PostgreSQL для асинхронного драйвера asyncpg."""
        return (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @property
    def REDIS_URL(self) -> str:
        """Строка подключения к Redis."""
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Settings()
