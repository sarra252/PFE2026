from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Teradata SQL Copilot"
    app_version: str = "0.1.0"
    log_level: str = "INFO"
    api_key: str = "changeme"

    llm_mode: str = "mock"
    llm_timeout_s: int = 30
    llm_fallback_to_mock: bool = True

    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini"
    openai_base_url: str = ""

    offline_data_dir: str = "data_synth/raw"
    rag_metadata_dir: str = "data_synth/metadata"
    db_backend: str = "offline"

    teradata_host: str = ""
    teradata_user: str = ""
    teradata_password: str = ""
    teradata_database: str = ""

    rag_backend: str = "vector"
    qdrant_url: str = "http://localhost:6333"
    qdrant_api_key: str = ""
    qdrant_collection: str = "teradata_metadata"
    embedding_model: str = "intfloat/multilingual-e5-base"
    rag_top_k: int = 4

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
