import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
LAKE_DIR = DATA_DIR / "lake"
OUTBOX_DIR = DATA_DIR / "stream_outbox"
INVOICES_DIR = DATA_DIR / "sample_invoices"
FILES_DIR = DATA_DIR / "sources_files"
SHAREPOINT_EXPORT_DIR = FILES_DIR / "sharepoint_export"

BRONZE_TXN_DIR = LAKE_DIR / "bronze" / "transactions"
BRONZE_INVOICE_DIR = LAKE_DIR / "bronze" / "invoices"
BRONZE_BUDGET_DIR = LAKE_DIR / "bronze" / "budgets"
BRONZE_MASTER_DIR = LAKE_DIR / "bronze" / "masters"

SQL_MODELS_DIR = PROJECT_ROOT / "transform" / "sql"


def _secret(key: str, default: str = "") -> str:
    value = os.getenv(key)
    if value:
        return value
    try:
        import streamlit as st

        return str(st.secrets.get(key, default))
    except Exception:
        return default


load_dotenv(PROJECT_ROOT / ".env")

GROQ_API_KEY = _secret("GROQ_API_KEY")
GEMINI_API_KEY = _secret("GEMINI_API_KEY")
MISTRAL_API_KEY = _secret("MISTRAL_API_KEY")
DEEPSEEK_API_KEY = _secret("DEEPSEEK_API_KEY")
OPENROUTER_API_KEY = _secret("OPENROUTER_API_KEY")

GROQ_MODEL = _secret("GROQ_MODEL", "openai/gpt-oss-120b")
GEMINI_MODEL = _secret("GEMINI_MODEL", "gemini-flash-latest")
MISTRAL_MODEL = _secret("MISTRAL_MODEL", "mistral-small-latest")
OPENROUTER_MODEL = _secret("OPENROUTER_MODEL", "nvidia/nemotron-3-super-120b-a12b:free")

PROVIDER_ORDER = [
    p.strip()
    for p in _secret(
        "LLM_PROVIDER_ORDER",
        "groq,gemini,mistral,openrouter",
    ).split(",")
    if p.strip()
]

UPSTASH_KAFKA_REST_URL = _secret("UPSTASH_KAFKA_REST_URL").rstrip("/")
UPSTASH_KAFKA_REST_USERNAME = _secret("UPSTASH_KAFKA_REST_USERNAME")
UPSTASH_KAFKA_REST_PASSWORD = _secret("UPSTASH_KAFKA_REST_PASSWORD")
KAFKA_TOPIC = _secret("KAFKA_TOPIC", "finance.transactions")

AZURE_STORAGE_CONNECTION_STRING = _secret("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = _secret("AZURE_CONTAINER", "lakehouse-bronze")

DUCKDB_PATH = Path(_secret("DUCKDB_PATH", "data/warehouse.duckdb"))
if not DUCKDB_PATH.is_absolute():
    DUCKDB_PATH = PROJECT_ROOT / DUCKDB_PATH
MOTHERDUCK_TOKEN = _secret("MOTHERDUCK_TOKEN")

POSTGRES_DSN = _secret("POSTGRES_DSN")
BIGQUERY_PROJECT = _secret("BIGQUERY_PROJECT")
GOOGLE_APPLICATION_CREDENTIALS = _secret("GOOGLE_APPLICATION_CREDENTIALS")
MSSQL_DSN = _secret("MSSQL_DSN")

SHAREPOINT_TENANT_ID = _secret("SHAREPOINT_TENANT_ID")
SHAREPOINT_CLIENT_ID = _secret("SHAREPOINT_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = _secret("SHAREPOINT_CLIENT_SECRET")
SHAREPOINT_SITE_HOST = _secret("SHAREPOINT_SITE_HOST")
SHAREPOINT_DRIVE_ID = _secret("SHAREPOINT_DRIVE_ID")
SHAREPOINT_FOLDER_PATH = _secret("SHAREPOINT_FOLDER_PATH", "finance-exports")

POWERBI_EMBED_URL = _secret("POWERBI_EMBED_URL")
SIM_TXNS_PER_DAY = int(_secret("SIM_TXNS_PER_DAY", "25"))

KAFKA_MODE = "upstash_rest" if UPSTASH_KAFKA_REST_URL else "local_file"

for _d in (DATA_DIR, LAKE_DIR, OUTBOX_DIR, INVOICES_DIR, FILES_DIR, SHAREPOINT_EXPORT_DIR):
    _d.mkdir(parents=True, exist_ok=True)


def ensure_dirs() -> None:
    for d in (
        BRONZE_TXN_DIR,
        BRONZE_INVOICE_DIR,
        BRONZE_BUDGET_DIR,
        BRONZE_MASTER_DIR,
        OUTBOX_DIR,
        DUCKDB_PATH.parent,
    ):
        d.mkdir(parents=True, exist_ok=True)
