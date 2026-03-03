# #!/usr/bin/env python3
# """
# Configuration Settings - Property Management Agentic RAG Chatbot
# MySQL 5.7 + FAISS (single index file)
# """

# import os
# from dotenv import load_dotenv

# load_dotenv()

# # ── OpenAI ────────────────────────────────────────────────────────────────────
# OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
# LLM_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
# LLM_TEMPERATURE: float = float(os.getenv("OPENAI_TEMPERATURE", "0.1"))
# LLM_MAX_TOKENS: int = int(os.getenv("OPENAI_MAX_TOKENS", "2000"))

# # ── MySQL 5.7 ─────────────────────────────────────────────────────────────────
# DB_CONFIG = {
#     "host":       os.getenv("SQL_SERVER",   "localhost"),
#     "port":       int(os.getenv("SQL_PORT", "3306")),
#     "database":   os.getenv("SQL_DATABASE", "lease_management_db"),
#     "user":       os.getenv("SQL_USERNAME", "root"),
#     "password":   os.getenv("SQL_PASSWORD", ""),
#     "charset":    "utf8mb4",
#     "autocommit": True,
# }

# # ── FAISS Vector Store ────────────────────────────────────────────────────────
# FAISS_INDEX_PATH: str    = os.getenv("FAISS_INDEX_PATH",    "E:/file/faiss.index")
# FAISS_METADATA_PATH: str = os.getenv("FAISS_METADATA_PATH", "E:/file/metadata.pkl")

# # Embedding model (MUST match what was used to build the FAISS index)
# EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
# EMBEDDING_DIM: int   = int(os.getenv("EMBEDDING_DIM",  "384"))
# VECTOR_TOP_K: int    = int(os.getenv("VECTOR_TOP_K",   "8"))   # raised from 5 → 8

# # Adaptive similarity thresholds (spec §3.4)
# VECTOR_DEFAULT_THRESHOLD: float = float(os.getenv("VECTOR_DEFAULT_THRESHOLD", "0.65"))
# VECTOR_EXPAND_THRESHOLD: float  = float(os.getenv("VECTOR_EXPAND_THRESHOLD",  "0.45"))
# VECTOR_MIN_RESULTS: int         = int(os.getenv("VECTOR_MIN_RESULTS",         "3"))

# # ── Agent Settings ────────────────────────────────────────────────────────────
# MAX_SQL_RETRIES: int      = int(os.getenv("MAX_SQL_RETRIES",      "4"))
# SQL_QUERY_HARD_LIMIT: int = int(os.getenv("SQL_QUERY_HARD_LIMIT", "200"))

# # ── FastAPI ───────────────────────────────────────────────────────────────────
# API_HOST: str  = os.getenv("API_HOST", "0.0.0.0")
# API_PORT: int  = int(os.getenv("API_PORT", "8000"))
# CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")

# # ── Logging ───────────────────────────────────────────────────────────────────
# LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


#!/usr/bin/env python3
"""
Configuration Settings - Property Management Agentic RAG Chatbot
MySQL 5.7 + FAISS (single index file)
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── OpenAI ────────────────────────────────────────────────────────────────────
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
LLM_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
LLM_TEMPERATURE: float = float(os.getenv("OPENAI_TEMPERATURE", "0.1"))
LLM_MAX_TOKENS: int = int(os.getenv("OPENAI_MAX_TOKENS", "2000"))

# ── MySQL 5.7 ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":       os.getenv("SQL_SERVER",   "localhost"),
    "port":       int(os.getenv("SQL_PORT", "3306")),
    "database":   os.getenv("SQL_DATABASE", "lease_management_db"),
    "user":       os.getenv("SQL_USERNAME", "root"),
    "password":   os.getenv("SQL_PASSWORD", ""),
    "charset":    "utf8mb4",
    "autocommit": True,
}

# ── FAISS Vector Store ────────────────────────────────────────────────────────
FAISS_INDEX_PATH: str    = os.getenv("FAISS_INDEX_PATH",    "E:/file/faiss.index")
FAISS_METADATA_PATH: str = os.getenv("FAISS_METADATA_PATH", "E:/file/metadata.pkl")

# Embedding model — OpenAI text-embedding-3-small (1536 dims)
# Options:
#   text-embedding-3-small  → 1536 dims, fast, cheap  ← DEFAULT
#   text-embedding-3-large  → 3072 dims, highest quality (update EMBEDDING_DIM to 3072)
#   text-embedding-ada-002  → 1536 dims, legacy
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM: int   = int(os.getenv("EMBEDDING_DIM",  "1536"))   # 1536 for 3-small/ada-002, 3072 for 3-large
VECTOR_TOP_K: int    = int(os.getenv("VECTOR_TOP_K",   "8"))

# Adaptive similarity thresholds (spec §3.4)
VECTOR_DEFAULT_THRESHOLD: float = float(os.getenv("VECTOR_DEFAULT_THRESHOLD", "0.65"))
VECTOR_EXPAND_THRESHOLD: float  = float(os.getenv("VECTOR_EXPAND_THRESHOLD",  "0.45"))
VECTOR_MIN_RESULTS: int         = int(os.getenv("VECTOR_MIN_RESULTS",         "3"))

# ── Agent Settings ────────────────────────────────────────────────────────────
MAX_SQL_RETRIES: int      = int(os.getenv("MAX_SQL_RETRIES",      "4"))
SQL_QUERY_HARD_LIMIT: int = int(os.getenv("SQL_QUERY_HARD_LIMIT", "200"))

# ── FastAPI ───────────────────────────────────────────────────────────────────
API_HOST: str  = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int  = int(os.getenv("API_PORT", "8000"))
CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")