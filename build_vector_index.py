# #!/usr/bin/env python3
# """
# build_vector_index.py
# =====================
# Builds (or rebuilds) the FAISS index from your MySQL database.

# Ingests qualitative text from:
#   1. TERP_MAINT_INCIDENTS         — maintenance complaint descriptions
#   2. TERP_LS_TICKET_TENANT        — move-out remarks / tenant tickets
#   3. TERP_LS_LEGAL_TENANT_REQUEST — legal tenant requests
#   4. TERP_LS_CONTRACT             — contract remarks / notes

# Run once to populate the FAISS index, re-run to refresh.

# Usage:
#   python build_vector_index.py
#   python build_vector_index.py --dry-run    # preview rows without building
#   python build_vector_index.py --scan       # scan columns in complaint tables first
# """

# import argparse
# import logging
# import os
# import pickle
# import sys
# from pathlib import Path
# from typing import Any, Dict, List, Tuple

# import faiss
# import numpy as np
# from sentence_transformers import SentenceTransformer

# # ── Load .env FIRST — before any other import ────────────────────────────────
# def _load_env():
#     try:
#         from dotenv import load_dotenv
#         # Search for .env starting from script location upward
#         script_dir = Path(__file__).parent.resolve()
#         for directory in [script_dir, script_dir.parent, script_dir.parent.parent]:
#             env_file = directory / ".env"
#             if env_file.exists():
#                 load_dotenv(dotenv_path=str(env_file), override=True)
#                 print(f"✅ .env loaded: {env_file}")
#                 # Verify key vars loaded
#                 host = os.getenv("SQL_SERVER", "NOT SET")
#                 db   = os.getenv("SQL_DATABASE", "NOT SET")
#                 port = os.getenv("SQL_PORT", "NOT SET")
#                 print(f"   SQL_SERVER={host}  SQL_PORT={port}  SQL_DATABASE={db}")
#                 return
#         print("⚠️  .env file not found in script directory or parents")
#     except ImportError:
#         print("⚠️  python-dotenv not installed")

# _load_env()

# # ── Now import config (it will re-run load_dotenv but env vars are already set) 
# try:
#     import config as _cfg

#     FAISS_INDEX_PATH    = _cfg.FAISS_INDEX_PATH
#     FAISS_METADATA_PATH = _cfg.FAISS_METADATA_PATH
#     EMBEDDING_MODEL     = _cfg.EMBEDDING_MODEL
#     EMBEDDING_DIM       = _cfg.EMBEDDING_DIM

#     # Use DB_CONFIG dict directly — same as the rest of your app
#     DB_CONFIG = _cfg.DB_CONFIG

#     print(f"✅ Config loaded")
#     print(f"   DB        : {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
#     print(f"   FAISS idx : {FAISS_INDEX_PATH}")
#     print(f"   FAISS meta: {FAISS_METADATA_PATH}")
#     print(f"   Model     : {EMBEDDING_MODEL}")

# except Exception as e:
#     print(f"❌ Could not load config.py: {e}")
#     print("   Make sure you run this script from E:\\files (1)\\ directory")
#     sys.exit(1)

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s  %(levelname)-8s  %(message)s",
# )
# logger = logging.getLogger(__name__)

# BATCH_SIZE = 256   # embeddings per batch

# # ── Table definitions — what to ingest and what metadata to keep ─────────────
# # text_candidates: column names tried IN ORDER — first one that exists is used
# # If NONE exist, a fallback string is built from metadata columns

# TABLE_DEFINITIONS = [
#     {
#         "label":           "Maintenance Incidents",
#         "source":          "maintenance_incidents",
#         "table":           "TERP_MAINT_INCIDENTS",
#         "alias":           "mi",
#         "text_candidates": [
#             "DESCRIPTION", "REMARKS", "NOTES", "COMPLAINT_TEXT",
#             "DETAILS", "SUBJECT", "INCIDENT_NOTES", "TITLE",
#             "CATEGORY", "TYPE", "INCIDENT_TYPE",
#         ],
#         "meta_candidates": ["TENANT_NAME", "INCIDENT_DATE", "RESOLVED_DATE", "DUE_DATE"],
#         "joins": """
#             LEFT JOIN TERP_LS_PROPERTY_UNIT pu ON pu.ID = mi.PROPERTY_UNIT
#             LEFT JOIN TERP_LS_PROPERTY p ON p.ID = pu.PROPERTY_ID
#         """,
#         "extra_select": "p.NAME AS PROPERTY_NAME",
#         "extra_meta":   ["PROPERTY_NAME"],
#         "where":        "mi.TENANT_NAME IS NOT NULL",
#         "limit":        10000,
#     },
#     {
#         "label":           "Move-out Tickets",
#         "source":          "ticket_tenant",
#         "table":           "TERP_LS_TICKET_TENANT",
#         "alias":           "tt",
#         "text_candidates": [
#             "REMARKS", "DESCRIPTION", "NOTES", "COMPLAINT",
#             "FEEDBACK", "TICKET_TEXT", "COMMENT", "SUBJECT",
#             "REASON", "DETAILS", "TITLE", "TYPE",
#         ],
#         "meta_candidates": ["STATUS", "CREATED_DATE", "TENANT_NAME", "UNIT_ID"],
#         "joins":        "",
#         "extra_select": "CASE WHEN tt.STATUS=1 THEN 'Resolved' ELSE 'Open' END AS STATUS_LABEL",
#         "extra_meta":   ["STATUS_LABEL"],
#         "where":        "",
#         "limit":        10000,
#     },
#     {
#         "label":           "Legal Tenant Requests",
#         "source":          "legal_tenant_request",
#         "table":           "TERP_LS_LEGAL_TENANT_REQUEST",
#         "alias":           "lr",
#         "text_candidates": [
#             "REQUEST_DETAILS", "DESCRIPTION", "NOTES", "REMARKS",
#             "REQUEST_TYPE", "LEGAL_NOTES", "SUBJECT", "DETAILS",
#             "TITLE", "TYPE", "CATEGORY", "REASON",
#         ],
#         "meta_candidates": ["TENANT_NAME", "REQUEST_DATE", "STATUS", "TYPE"],
#         "joins":        "",
#         "extra_select": "",
#         "extra_meta":   [],
#         "where":        "",
#         "limit":        5000,
#     },
#     {
#         "label":           "Contracts",
#         "source":          "contract",
#         "table":           "TERP_LS_CONTRACT",
#         "alias":           "c",
#         "text_candidates": [
#             "REMARKS", "NOTES", "DESCRIPTION", "SPECIAL_CONDITIONS",
#             "TERMS", "COMMENT", "DETAILS",
#         ],
#         "meta_candidates": ["START_DATE", "END_DATE", "ACTIVE"],
#         "joins": """
#             LEFT JOIN TERP_LS_TENANTS t ON t.ID = c.TENANT
#             LEFT JOIN TERP_LS_CONTRACT_UNIT cu ON cu.CONTRACT_ID = c.ID
#             LEFT JOIN TERP_LS_PROPERTY_UNIT pu ON pu.ID = cu.UNIT_ID
#             LEFT JOIN TERP_LS_PROPERTY p ON p.ID = pu.PROPERTY_ID
#         """,
#         "extra_select": "c.CONTRACT_NO, t.NAME AS TENANT_NAME, p.NAME AS PROPERTY_NAME",
#         "extra_meta":   ["CONTRACT_NO", "TENANT_NAME", "PROPERTY_NAME"],
#         "where":        "c.ACTIVE = 1",
#         "limit":        20000,
#     },
# ]


# # ── Schema discovery helpers ───────────────────────────────────────────────────

# def get_table_columns(conn, table_name: str) -> set:
#     """Return set of actual column names for a table (uppercased)."""
#     try:
#         with conn.cursor() as cur:
#             cur.execute(f"DESCRIBE `{table_name}`")
#             rows = cur.fetchall()
#             cols = set()
#             for r in rows:
#                 col = r.get("Field") if isinstance(r, dict) else r[0]
#                 if col:
#                     cols.add(col.upper())
#             return cols
#     except Exception as e:
#         logger.warning("Could not DESCRIBE %s: %s", table_name, e)
#         return set()


# def build_query(conn, defn: dict) -> tuple:
#     """
#     Discover which text column actually exists, build the SELECT query.
#     Returns (sql, text_col_name, meta_cols_found) or None if table missing.
#     """
#     table   = defn["table"]
#     alias   = defn["alias"]
#     actual_cols = get_table_columns(conn, table)

#     if not actual_cols:
#         logger.warning("Table %s not found or empty schema", table)
#         return None

#     # Find first existing text candidate
#     text_col = None
#     for candidate in defn["text_candidates"]:
#         if candidate.upper() in actual_cols:
#             text_col = candidate
#             logger.info("  [%s] Using text column: %s", defn["label"], text_col)
#             break

#     # Find existing meta columns
#     meta_cols = [c for c in defn["meta_candidates"] if c.upper() in actual_cols]

#     # Build SELECT parts
#     select_parts = [f"`{alias}`.`ID`"]

#     for mc in meta_cols:
#         select_parts.append(f"`{alias}`.`{mc}`")

#     if defn.get("extra_select"):
#         select_parts.append(defn["extra_select"])

#     if text_col:
#         select_parts.append(f"`{alias}`.`{text_col}` AS TEXT_CONTENT")
#     else:
#         # No text column — build fallback from metadata
#         logger.warning("  [%s] No text column found. Building fallback from: %s",
#                        defn["label"], meta_cols)
#         fallback_parts = []
#         for mc in meta_cols[:3]:
#             fallback_parts.append(f"IFNULL(`{alias}`.`{mc}`, '')")
#         if fallback_parts:
#             fallback_expr = "CONCAT_WS(' | ', " + ", ".join(fallback_parts) + ")"
#         else:
#             fallback_expr = "CONCAT('" + defn["label"] + " #', `" + alias + "`.`ID`)"
#         select_parts.append(f"{fallback_expr} AS TEXT_CONTENT")

#     where_clause = f"WHERE {defn['where']}" if defn.get("where") else ""
#     joins = defn.get("joins", "").strip()

#     sql = f"""
#         SELECT {', '.join(select_parts)}
#         FROM `{table}` {alias}
#         {joins}
#         {where_clause}
#         LIMIT {defn['limit']}
#     """

#     all_meta = meta_cols + defn.get("extra_meta", [])
#     return sql, text_col, all_meta


# # ── DB connection ─────────────────────────────────────────────────────────────

# def get_connection():
#     import pymysql
#     conn_args = {
#         "host":     DB_CONFIG["host"],
#         "port":     int(DB_CONFIG["port"]),
#         "user":     DB_CONFIG["user"],
#         "password": DB_CONFIG["password"],
#         "database": DB_CONFIG["database"],
#         "charset":  "utf8mb4",
#         "cursorclass": pymysql.cursors.DictCursor,
#         "connect_timeout": 10,
#     }
#     return pymysql.connect(**conn_args)


# # ── Main build logic ──────────────────────────────────────────────────────────

# def fetch_rows(conn, sql: str) -> List[Dict[str, Any]]:
#     with conn.cursor() as cur:
#         cur.execute(sql)
#         return cur.fetchall()


# def run(dry_run: bool = False):
#     logger.info("Connecting to MySQL: %s@%s:%s/%s",
#                 DB_CONFIG['user'], DB_CONFIG['host'],
#                 DB_CONFIG['port'], DB_CONFIG['database'])
#     conn = get_connection()
#     logger.info("✅ Connected to MySQL")

#     logger.info("Loading embedding model: %s", EMBEDDING_MODEL)
#     encoder = SentenceTransformer(EMBEDDING_MODEL)

#     all_texts:    List[str]             = []
#     all_metadata: List[Dict[str, Any]]  = []

#     # ── Fetch rows using schema-aware queries ─────────────────────────────────
#     for defn in TABLE_DEFINITIONS:
#         label = defn["label"]
#         logger.info("Fetching: %s ...", label)

#         result = build_query(conn, defn)
#         if result is None:
#             logger.warning("  ⚠️  SKIP %s — table not found", label)
#             continue

#         sql, text_col, meta_cols = result

#         try:
#             rows = fetch_rows(conn, sql)
#         except Exception as exc:
#             logger.warning("  ⚠️  SKIP %s — query error: %s", label, exc)
#             continue

#         skipped = 0
#         for row in rows:
#             if isinstance(row, tuple):
#                 # Shouldn't happen with DictCursor but handle gracefully
#                 row = dict(zip([f"col{i}" for i in range(len(row))], row))

#             text = str(row.get("TEXT_CONTENT") or "").strip()

#             # Skip rows with no meaningful text
#             if not text or len(text) < 5:
#                 skipped += 1
#                 continue

#             idx = len(all_texts)
#             all_texts.append(text)

#             # Build metadata entry
#             entry: Dict[str, Any] = {
#                 "_global_idx": idx,
#                 "source":      defn["source"],
#                 "text":        text,
#                 "table":       defn["table"],
#             }
#             for col in meta_cols:
#                 val = row.get(col)
#                 if val is not None:
#                     entry[col.lower()] = str(val)

#             all_metadata.append(entry)

#         logger.info("  → %d rows fetched, %d indexed, %d skipped",
#                     len(rows), len(rows) - skipped, skipped)

#     conn.close()
#     logger.info("Total records to embed: %d", len(all_texts))

#     if dry_run:
#         logger.info("DRY RUN — first 5 texts:")
#         for i, t in enumerate(all_texts[:5]):
#             src = all_metadata[i].get("source", "?")
#             logger.info("  [%d] %-30s: %s", i, src, t[:120])
#         return

#     if not all_texts:
#         logger.error(
#             "No text records collected.\n"
#             "Run:  python build_vector_index.py --scan\n"
#             "to see your actual table columns, then re-run."
#         )
#         sys.exit(1)

#     # ── Embed in batches ──────────────────────────────────────────────────────
#     logger.info("Embedding %d texts in batches of %d ...", len(all_texts), BATCH_SIZE)
#     all_vectors = []
#     for start in range(0, len(all_texts), BATCH_SIZE):
#         batch = all_texts[start : start + BATCH_SIZE]
#         vecs  = encoder.encode(batch, normalize_embeddings=True, show_progress_bar=False)
#         all_vectors.append(vecs.astype("float32"))
#         if (start // BATCH_SIZE) % 5 == 0:
#             logger.info("  Embedded %d / %d", start + len(batch), len(all_texts))

#     matrix = np.vstack(all_vectors)
#     logger.info("Embedding matrix shape: %s", matrix.shape)

#     # ── Build FAISS index ─────────────────────────────────────────────────────
#     index = faiss.IndexFlatL2(EMBEDDING_DIM)
#     index.add(matrix)
#     logger.info("FAISS index built: %d vectors", index.ntotal)

#     # ── Save ──────────────────────────────────────────────────────────────────
#     idx_path  = Path(FAISS_INDEX_PATH)
#     meta_path = Path(FAISS_METADATA_PATH)
#     idx_path.parent.mkdir(parents=True, exist_ok=True)
#     meta_path.parent.mkdir(parents=True, exist_ok=True)

#     faiss.write_index(index, str(idx_path))
#     logger.info("✅ FAISS index saved: %s", idx_path)

#     with open(meta_path, "wb") as f:
#         pickle.dump(all_metadata, f)
#     logger.info("✅ Metadata saved: %s  (%d entries)", meta_path, len(all_metadata))

#     # ── Summary ───────────────────────────────────────────────────────────────
#     from collections import Counter
#     source_counts = Counter(m["source"] for m in all_metadata)
#     logger.info("Index composition:")
#     for src, cnt in source_counts.most_common():
#         logger.info("  %-35s  %d vectors", src, cnt)


# def scan_columns():
#     """
#     Connects to DB and prints all columns in the complaint tables.
#     Run this first to find your actual text column names.
#     """
#     import pymysql
#     print("\n" + "="*60)
#     print("SCANNING complaint table columns...")
#     print("="*60)
#     conn = get_connection()
#     tables = [
#         "TERP_MAINT_INCIDENTS",
#         "TERP_LS_TICKET_TENANT",
#         "TERP_LS_LEGAL_TENANT_REQUEST",
#     ]
#     with conn.cursor() as cur:
#         for table in tables:
#             try:
#                 cur.execute(f"DESCRIBE `{table}`")
#                 rows = cur.fetchall()
#                 print(f"\n📋 {table}:")
#                 for r in rows:
#                     col  = r.get("Field", r[0] if isinstance(r, tuple) else "?")
#                     typ  = r.get("Type",  r[1] if isinstance(r, tuple) else "?")
#                     null = r.get("Null",  "")
#                     print(f"   {col:<35} {typ:<25} {null}")

#                 # Show 3 sample rows
#                 cur.execute(f"SELECT * FROM `{table}` LIMIT 3")
#                 sample = cur.fetchall()
#                 if sample:
#                     print(f"   --- Sample row (first) ---")
#                     row = sample[0]
#                     if isinstance(row, dict): 
#                         for k, v in row.items():
#                             if v is not None:
#                                 print(f"   {k}: {str(v)[:80]}")
#                     else:
#                         print(f"   {row}")
#             except Exception as e:
#                 print(f"   ⚠️  {table}: {e}")
#     conn.close()
#     print("\n" + "="*60)
#     print("👆 Look for TEXT/VARCHAR columns with complaint descriptions above.")
#     print("   Update INGESTION_QUERIES in this file with the correct column names.")
#     print("="*60 + "\n")


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Build FAISS vector index from MySQL")
#     parser.add_argument("--dry-run", action="store_true",
#                         help="Fetch rows and preview without embedding or saving")
#     parser.add_argument("--scan", action="store_true",
#                         help="Scan complaint table columns and show sample rows (run this first)")
#     args = parser.parse_args()

#     if args.scan:
#         scan_columns()
#     else:
#         run(dry_run=args.dry_run)


#!/usr/bin/env python3
"""
build_vector_index.py  — Full-Database FAISS Index Builder
===========================================================
Indexes ALL meaningful text from your MySQL DB using OpenAI embeddings.

Strategy:
  - Scans every table in the DB for TEXT/VARCHAR columns
  - Skips lookup/reference tables (no useful text)
  - Concatenates all text columns per row into a rich searchable document
  - Stores full metadata so RAG can cite source, tenant, property, date

This makes the vector store work for ANY question, not just complaints.

Usage:
  python build_vector_index.py              # build full index
  python build_vector_index.py --dry-run    # preview without embedding
  python build_vector_index.py --scan       # inspect tables first
  python build_vector_index.py --tables X,Y # only index specific tables
"""

import argparse
import logging
import os
import pickle
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import faiss
import numpy as np

# ── Load .env FIRST ──────────────────────────────────────────────────────────
def _load_env():
    try:
        from dotenv import load_dotenv
        script_dir = Path(__file__).parent.resolve()
        for directory in [script_dir, script_dir.parent, script_dir.parent.parent]:
            env_file = directory / ".env"
            if env_file.exists():
                load_dotenv(dotenv_path=str(env_file), override=True)
                print(f"✅ .env loaded: {env_file}")
                print(f"   SQL_SERVER={os.getenv('SQL_SERVER','?')}  "
                      f"SQL_PORT={os.getenv('SQL_PORT','?')}  "
                      f"SQL_DATABASE={os.getenv('SQL_DATABASE','?')}")
                return
        print("⚠️  .env not found")
    except ImportError:
        print("⚠️  python-dotenv not installed")

_load_env()

try:
    import config as _cfg
    FAISS_INDEX_PATH    = _cfg.FAISS_INDEX_PATH
    FAISS_METADATA_PATH = _cfg.FAISS_METADATA_PATH
    EMBEDDING_MODEL     = _cfg.EMBEDDING_MODEL
    EMBEDDING_DIM       = _cfg.EMBEDDING_DIM
    OPENAI_API_KEY      = _cfg.OPENAI_API_KEY
    DB_CONFIG           = _cfg.DB_CONFIG
    print(f"✅ Config loaded")
    print(f"   DB    : {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"   FAISS : {FAISS_INDEX_PATH}")
    print(f"   Model : {EMBEDDING_MODEL}  (dim={EMBEDDING_DIM})")
except Exception as e:
    print(f"❌ config.py failed: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 100   # OpenAI embeddings per API call (safe limit)

# ── Tables to skip (pure numeric lookup / auth / system tables) ──────────────
SKIP_TABLES = {
    # Auth / sessions
    "TERP_AUTH_USER", "TERP_AUTH_TOKEN", "TERP_AUTH_SESSION",
    "TERP_USERS", "TERP_USER_ROLES", "TERP_ROLES", "TERP_PERMISSIONS",
    # Voucher line items / accounting ledger (high volume, no text)
    "TERP_ACC_VOUCHER_LINES", "TERP_ACC_VOUCHER_ITEMS",
    "TERP_ACC_GL_ENTRIES", "TERP_ACC_JOURNAL",
    # Lookup / status / type tables (just label + ID)
    "TERP_LS_PROPERTY_UNIT_STATUS", "TERP_LS_PROPERTY_UNIT_TYPE",
    "TERP_LS_CONTRACT_STATUS", "TERP_LS_INCIDENT_TYPE",
    "TERP_LS_COMPLAINT_CATEGORY", "TERP_LS_PAYMENT_TERM",
    # System / config
    "TERP_SYS_CONFIG", "TERP_SYS_SETTINGS", "TERP_AUDIT_LOG",
    "TERP_NOTIFICATIONS", "TERP_EMAIL_LOG",
    # Signature blobs (base64 images stored as text — not useful for RAG)
    # detected dynamically by column name
}

# Column names that are definitely NOT useful text for RAG
SKIP_COLUMNS = {
    "MANAGER_SIGNATURE", "TENANT_SIGNATURE", "MGR_PARTNER_SIGN",
    "DEP_MANAGER_SIGNATURE", "SIGNATURE", "PHOTO", "IMAGE", "LOGO",
    "PASSWORD", "TOKEN", "SECRET", "HASH", "SALT",
    "CREATED_BY", "LAST_UPDATED_BY", "ISSUED_BY", "RESOLVED_BY",
    "ASSIGNED_BY", "VERIFIED_BY",
    "OFFICE_ID", "SITE_ID", "FLAG",
    "TIME_TAKEN", "REQUEST_TIME",
    "PDC", "PDC_DATE", "COLLECTED",
    "BUSINESS_HOURS_FROM", "BUSINESS_HOURS_TO",
}

# Priority tables — always index these first, with richer metadata
PRIORITY_TABLE_CONFIG = {
    "TERP_MAINT_INCIDENTS": {
        "source": "maintenance_incidents",
        "primary_text_cols": ["COMPLAINT_DESCRIPTION", "RESOLUTION_NOTES", "VERIFICATION_NOTES",
                               "ASSIGNMENT_NOTES", "DELETE_NOTES", "ADMIN_RESOLVE_NOTES",
                               "MAT_REQUIRED_DESCRIPTION", "MAT_USED_REMARKS", "RETURN_REASON"],
        "metadata_cols":     ["INCIDENT_DATE", "RESOLVED_DATE", "DUE_DATE", "STATUS",
                               "INCIDENT_NO", "PRIORITY"],
        "name_lookups": {
            "TENANT_NAME":   ("TERP_LS_TENANTS",   "ID", "NAME"),
            "PROPERTY_NAME": ("TERP_LS_PROPERTY",   "ID", "NAME"),
            "INCIDENT_TYPE": ("TERP_LS_INCIDENT_TYPE", "ID", "NAME"),
            "COMPLAINT_CATEGORY": ("TERP_LS_COMPLAINT_CATEGORY", "ID", "NAME"),
        },
        "where": "COMPLAINT_DESCRIPTION IS NOT NULL AND COMPLAINT_DESCRIPTION != ''",
        "limit": 20000,
    },
    "TERP_LS_CONTRACT": {
        "source": "contract",
        "primary_text_cols": ["NOTES", "BUSINESS_TYPE", "LEGAL_DESCRIPTION",
                               "CANCEL_COMMENT", "TENANT_COMMENTS", "APPROVAL_COMMENTS",
                               "RENEWAL_REQUEST_COMMENT", "RENEWAL_SENT_COMMENTS",
                               "AUTO_RENEWAL_COMMENTS"],
        "metadata_cols":     ["CONTRACT_NUMBER", "START_DATE", "END_DATE", "ACTIVE"],
        "name_lookups": {
            "TENANT": ("TERP_LS_TENANTS", "ID", "NAME"),
        },
        "where": "ACTIVE = 1",
        "limit": 20000,
    },
    "TERP_LS_TENANTS": {
        "source": "tenant",
        "primary_text_cols": ["NAME", "EMAIL", "PHONE", "ADDRESS", "NOTES",
                               "DESCRIPTION", "REMARKS"],
        "metadata_cols":     ["TYPE", "CREATED_AT"],
        "name_lookups":      {},
        "where": "",
        "limit": 10000,
    },
    "TERP_LS_PROPERTY": {
        "source": "property",
        "primary_text_cols": ["NAME", "ADDRESS", "DESCRIPTION", "NOTES",
                               "LOCATION", "REMARKS"],
        "metadata_cols":     ["TYPE", "STATUS"],
        "name_lookups":      {},
        "where": "",
        "limit": 1000,
    },
    "TERP_LS_LEGAL_TENANT_REQUEST": {
        "source": "legal_request",
        "primary_text_cols": ["DESCRIPTION", "MGMT_COMMENTS", "REQUEST_NO"],
        "metadata_cols":     ["DATE", "STATUS"],
        "name_lookups":      {},
        "where": "",
        "limit": 5000,
    },
    "TERP_LS_TICKET_TENANT": {
        "source": "ticket",
        "primary_text_cols": ["REMARKS", "DESCRIPTION", "NOTES", "COMMENT"],
        "metadata_cols":     ["STATUS", "CREATED_AT"],
        "name_lookups":      {},
        "where": "",
        "limit": 10000,
    },
}

# ── Database connection ───────────────────────────────────────────────────────

def get_connection():
    import pymysql
    return pymysql.connect(
        host     = DB_CONFIG["host"],
        port     = int(DB_CONFIG["port"]),
        user     = DB_CONFIG["user"],
        password = DB_CONFIG["password"],
        database = DB_CONFIG["database"],
        charset  = "utf8mb4",
        cursorclass = pymysql.cursors.DictCursor,
        connect_timeout = 10,
    )

def fetch_rows(conn, sql: str) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() or []

def get_all_tables(conn) -> List[str]:
    """Get all table names in the database."""
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
        tables = []
        for r in rows:
            name = list(r.values())[0] if isinstance(r, dict) else r[0]
            tables.append(str(name))
    return tables

def get_table_info(conn, table: str) -> Dict[str, str]:
    """Returns {col_name: col_type} for all columns."""
    with conn.cursor() as cur:
        cur.execute(f"DESCRIBE `{table}`")
        rows = cur.fetchall()
        result = {}
        for r in rows:
            col  = r.get("Field", r[0] if isinstance(r, tuple) else "?")
            typ  = r.get("Type",  r[1] if isinstance(r, tuple) else "?")
            result[col.upper()] = str(typ).upper()
    return result

def get_text_columns(col_info: Dict[str, str]) -> List[str]:
    """Return list of TEXT/VARCHAR column names, excluding useless ones."""
    text_cols = []
    for col, typ in col_info.items():
        if col in SKIP_COLUMNS:
            continue
        if any(t in typ for t in ["VARCHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "CHAR"]):
            # Skip if looks like base64 / binary data
            if col.endswith("_SIGNATURE") or col.endswith("_SIGN"):
                continue
            text_cols.append(col)
    return text_cols

def load_lookup_table(conn, table: str, id_col: str, name_col: str) -> Dict[str, str]:
    """Load a lookup table as {id: name} dict."""
    try:
        rows = fetch_rows(conn, f"SELECT `{id_col}`, `{name_col}` FROM `{table}` LIMIT 10000")
        return {str(r.get(id_col, r.get(id_col.lower(), ""))): str(r.get(name_col, r.get(name_col.lower(), "")))
                for r in rows if r}
    except Exception as e:
        logger.warning("Could not load lookup %s: %s", table, e)
        return {}

# ── OpenAI Embedder ───────────────────────────────────────────────────────────

def make_embedder():
    try:
        from openai import OpenAI
    except ImportError:
        logger.error("openai not installed. Run: pip install openai")
        sys.exit(1)
    client = OpenAI(api_key=OPENAI_API_KEY)
    logger.info("✅ OpenAI embedder ready: %s  dim=%d", EMBEDDING_MODEL, EMBEDDING_DIM)

    def embed_batch(texts: List[str]) -> np.ndarray:
        texts = [t.replace("\n", " ").strip() or " " for t in texts]
        for attempt in range(4):
            try:
                resp  = client.embeddings.create(input=texts, model=EMBEDDING_MODEL)
                vecs  = np.array([item.embedding for item in resp.data], dtype="float32")
                norms = np.linalg.norm(vecs, axis=1, keepdims=True)
                norms = np.where(norms == 0, 1, norms)
                return vecs / norms
            except Exception as exc:
                wait = 2 ** attempt
                logger.warning("Embed attempt %d/4: %s (retry in %ds)", attempt + 1, exc, wait)
                if attempt < 3:
                    time.sleep(wait)
                else:
                    raise
    return embed_batch

# ── Row → searchable document ─────────────────────────────────────────────────

def row_to_document(
    row: Dict,
    text_cols: List[str],
    meta_cols: List[str],
    source: str,
    lookups: Dict[str, Dict[str, str]],  # {col_name: {id: label}}
) -> Tuple[Optional[str], Dict]:
    """
    Build a rich text document and metadata dict from a DB row.
    Returns (document_text, metadata) or (None, {}) if row has no useful text.
    """
    parts = []
    metadata = {"source": source}

    # Resolve lookup IDs → human-readable names and add to parts
    for col, lookup_dict in lookups.items():
        raw_val = str(row.get(col) or row.get(col.lower()) or "").strip()
        if raw_val and raw_val not in ("None", "0", ""):
            label = lookup_dict.get(raw_val, raw_val)
            if label and label != raw_val or len(label) > 3:
                parts.append(f"{col.replace('_',' ').title()}: {label}")
                metadata[col.lower()] = label

    # Add metadata cols (dates, status, reference numbers)
    for col in meta_cols:
        val = str(row.get(col) or row.get(col.lower()) or "").strip()
        if val and val not in ("None", "null", "0", "1"):
            metadata[col.lower()] = val
            # Add short reference fields to text too (e.g. CONTRACT_NUMBER, INCIDENT_NO)
            if any(kw in col.upper() for kw in ["NUMBER", "NO", "REF", "DATE", "STATUS"]):
                parts.append(f"{col.replace('_',' ').title()}: {val}")

    # Add primary text columns
    text_found = False
    for col in text_cols:
        val = str(row.get(col) or row.get(col.lower()) or "").strip()
        if val and len(val) > 3 and val.lower() not in ("none", "null"):
            col_label = col.replace("_", " ").title()
            parts.append(f"{col_label}: {val}")
            metadata[col.lower()] = val[:500]  # cap metadata length
            text_found = True

    if not parts or not text_found:
        return None, {}

    document = "\n".join(parts)
    return document, metadata

# ── Priority table indexer ────────────────────────────────────────────────────

def index_priority_table(conn, table: str, config: Dict, all_texts: List, all_metadata: List):
    """Index a priority table using its custom config and lookup joins."""
    label  = config["source"]
    source = config["source"]
    logger.info("Indexing [PRIORITY] %s ...", table)

    col_info = get_table_info(conn, table)
    if not col_info:
        logger.warning("  Table %s not found — skipping", table)
        return

    # Find which primary text cols actually exist
    text_cols = [c for c in config["primary_text_cols"] if c.upper() in col_info]
    if not text_cols:
        logger.warning("  No text columns found in %s — skipping", table)
        return
    logger.info("  Text columns: %s", text_cols)

    # Find which metadata cols exist
    meta_cols = [c for c in config["metadata_cols"] if c.upper() in col_info]

    # Load lookup tables
    lookups: Dict[str, Dict[str, str]] = {}
    for col, (lk_table, lk_id, lk_name) in config.get("name_lookups", {}).items():
        if col.upper() in col_info:
            lookups[col] = load_lookup_table(conn, lk_table, lk_id, lk_name)

    # Build SELECT
    all_cols = ["ID"] + text_cols + meta_cols + [c for c in config.get("name_lookups", {}) if c.upper() in col_info]
    all_cols = list(dict.fromkeys(all_cols))  # deduplicate
    select   = ", ".join(f"`{c}`" for c in all_cols)
    where    = f"WHERE {config['where']}" if config.get("where") else ""
    sql      = f"SELECT {select} FROM `{table}` {where} LIMIT {config.get('limit', 10000)}"

    try:
        rows = fetch_rows(conn, sql)
    except Exception as e:
        logger.warning("  Query failed for %s: %s — skipping", table, e)
        return

    indexed = 0
    for row in rows:
        # Convert keys to upper for consistency
        row_upper = {k.upper(): v for k, v in row.items()}
        doc, meta = row_to_document(row_upper, text_cols, meta_cols, source, lookups)
        if doc and len(doc) >= 10:
            meta["table"]       = table.lower()
            meta["_row_id"]     = str(row_upper.get("ID", ""))
            all_texts.append(doc)
            all_metadata.append(meta)
            indexed += 1

    logger.info("  → %d / %d rows indexed", indexed, len(rows))

# ── Generic table indexer ─────────────────────────────────────────────────────

def index_generic_table(conn, table: str, all_texts: List, all_metadata: List):
    """Auto-detect text columns and index any remaining table."""
    col_info  = get_table_info(conn, table)
    text_cols = get_text_columns(col_info)

    if not text_cols:
        return  # No text columns at all

    # Quick check: does the table have any non-empty text rows?
    # Sample 10 rows to decide if it's worth indexing
    sample_col = text_cols[0]
    try:
        sample = fetch_rows(
            conn,
            f"SELECT `{sample_col}` FROM `{table}` "
            f"WHERE `{sample_col}` IS NOT NULL AND `{sample_col}` != '' LIMIT 10"
        )
        if not sample:
            return  # Table is empty or has no text
    except Exception:
        return

    # Get row count
    try:
        cnt_row = fetch_rows(conn, f"SELECT COUNT(*) AS N FROM `{table}`")
        row_count = int(list(cnt_row[0].values())[0]) if cnt_row else 0
        if row_count == 0:
            return
    except Exception:
        row_count = -1

    logger.info("  Indexing [AUTO] %s  (%d rows, %d text cols)", table, row_count, len(text_cols))

    # Build SELECT with ID + all text cols
    id_col    = "ID" if "ID" in col_info else list(col_info.keys())[0]
    all_cols  = [id_col] + text_cols[:15]  # cap at 15 text cols
    select    = ", ".join(f"`{c}`" for c in all_cols)

    try:
        rows = fetch_rows(conn, f"SELECT {select} FROM `{table}` LIMIT 5000")
    except Exception as e:
        logger.warning("  Query failed for %s: %s", table, e)
        return

    indexed = 0
    source  = table.lower().replace("terp_", "").replace("_", " ").strip()

    for row in rows:
        row_upper = {k.upper(): v for k, v in row.items()}
        doc, meta = row_to_document(row_upper, text_cols, [], source, {})
        if doc and len(doc) >= 15:
            meta["table"]   = table.lower()
            meta["_row_id"] = str(row_upper.get(id_col.upper(), ""))
            all_texts.append(doc)
            all_metadata.append(meta)
            indexed += 1

    if indexed:
        logger.info("    → %d indexed", indexed)

# ── Main build ────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, only_tables: Optional[List[str]] = None):
    logger.info("Connecting to MySQL...")
    conn = get_connection()
    logger.info("✅ Connected")

    embed_batch = make_embedder()

    all_tables = get_all_tables(conn)
    logger.info("Found %d tables in database", len(all_tables))

    all_texts:    List[str]        = []
    all_metadata: List[Dict]       = []

    # ── Phase 1: Index priority tables with rich config ───────────────────────
    logger.info("\n── Phase 1: Priority tables ──────────────────────────────")
    for table, config in PRIORITY_TABLE_CONFIG.items():
        if only_tables and table not in only_tables:
            continue
        if table in all_tables:
            index_priority_table(conn, table, config, all_texts, all_metadata)
        else:
            logger.info("  SKIP %s (not in DB)", table)

    priority_count = len(all_texts)
    logger.info("\nPhase 1 complete: %d documents from priority tables", priority_count)

    # ── Phase 2: Auto-index remaining tables ─────────────────────────────────
    logger.info("\n── Phase 2: Auto-indexing remaining tables ───────────────")
    priority_names = set(PRIORITY_TABLE_CONFIG.keys())
    skip_names     = SKIP_TABLES | priority_names

    for table in sorted(all_tables):
        if only_tables and table not in only_tables:
            continue
        if table.upper() in {t.upper() for t in skip_names}:
            continue
        try:
            index_generic_table(conn, table, all_texts, all_metadata)
        except Exception as e:
            logger.warning("  Error indexing %s: %s", table, e)

    conn.close()

    total = len(all_texts)
    logger.info("\n── Summary ───────────────────────────────────────────────")
    logger.info("  Priority tables : %d documents", priority_count)
    logger.info("  Other tables    : %d documents", total - priority_count)
    logger.info("  TOTAL           : %d documents to embed", total)

    if total == 0:
        logger.error("No documents collected. Run --scan to inspect your tables.")
        sys.exit(1)

    # ── Dry run preview ───────────────────────────────────────────────────────
    if dry_run:
        logger.info("\n── DRY RUN — first 10 documents ──────────────────────────")
        from collections import Counter
        source_counts = Counter(m.get("source", "?") for m in all_metadata)
        for src, cnt in source_counts.most_common():
            logger.info("  %-40s  %d docs", src, cnt)
        logger.info("\nSample documents:")
        for i in range(min(10, len(all_texts))):
            src = all_metadata[i].get("source", "?")
            logger.info("\n  [%d] source=%s\n  text=%s", i, src, all_texts[i][:200])
        return

    # ── Embed in batches ──────────────────────────────────────────────────────
    logger.info("\n── Embedding %d documents with %s ────────────────────────", total, EMBEDDING_MODEL)
    all_vectors = []
    for start in range(0, total, BATCH_SIZE):
        batch = all_texts[start : start + BATCH_SIZE]
        vecs  = embed_batch(batch)
        all_vectors.append(vecs)
        pct = (start + len(batch)) / total * 100
        if start % (BATCH_SIZE * 5) == 0 or start + BATCH_SIZE >= total:
            logger.info("  Embedded %d / %d  (%.0f%%)", start + len(batch), total, pct)

    matrix = np.vstack(all_vectors)
    logger.info("Embedding matrix: shape=%s  dtype=%s", matrix.shape, matrix.dtype)

    # ── Build and save FAISS index ────────────────────────────────────────────
    idx_path  = Path(FAISS_INDEX_PATH)
    meta_path = Path(FAISS_METADATA_PATH)
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    index = faiss.IndexFlatL2(EMBEDDING_DIM)
    index.add(matrix)
    logger.info("FAISS index: %d vectors  dim=%d", index.ntotal, index.d)

    faiss.write_index(index, str(idx_path))
    logger.info("✅ FAISS index saved: %s", idx_path)

    with open(meta_path, "wb") as f:
        pickle.dump(all_metadata, f)
    logger.info("✅ Metadata saved: %s  (%d entries)", meta_path, len(all_metadata))

    # ── Final summary ─────────────────────────────────────────────────────────
    from collections import Counter
    logger.info("\n══ INDEX COMPOSITION ═══════════════════════════════════════")
    source_counts = Counter(m.get("source", "?") for m in all_metadata)
    for src, cnt in source_counts.most_common():
        logger.info("  %-40s  %d vectors", src, cnt)
    logger.info("  %-40s  %d TOTAL", "───────────", index.ntotal)
    logger.info("══════════════════════════════════════════════════════════\n")

# ── Scan mode ─────────────────────────────────────────────────────────────────

def scan_columns(only_tables: Optional[List[str]] = None):
    """Inspect DB tables — shows all columns, types, and sample text values."""
    print("\n" + "="*70)
    print("SCANNING database tables...")
    print("="*70)
    conn    = get_connection()
    tables  = only_tables or get_all_tables(conn)

    with conn.cursor() as cur:
        for table in tables:
            try:
                cur.execute(f"DESCRIBE `{table}`")
                rows     = cur.fetchall()
                col_info = {(r.get("Field") or r[0]): (r.get("Type") or r[1]) for r in rows}

                text_cols = [c for c, t in col_info.items()
                             if any(x in str(t).upper() for x in ["VARCHAR","TEXT","CHAR"])
                             and c.upper() not in SKIP_COLUMNS]

                print(f"\n{'='*70}")
                print(f"📋 {table}  ({len(col_info)} cols, {len(text_cols)} text cols)")
                print(f"{'='*70}")
                for col, typ in col_info.items():
                    is_text = col in text_cols
                    print(f"   {col:<40} {str(typ):<25} {'← TEXT' if is_text else ''}")

                if text_cols:
                    print(f"\n   Sample text values:")
                    for col in text_cols[:8]:
                        try:
                            cur.execute(
                                f"SELECT `{col}`, LENGTH(`{col}`) AS L "
                                f"FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != '' "
                                f"ORDER BY L DESC LIMIT 2"
                            )
                            samples = cur.fetchall()
                            if samples:
                                for s in samples:
                                    val = s.get(col) or (s[0] if isinstance(s, tuple) else "?")
                                    ln  = s.get("L") or (s[1] if isinstance(s, tuple) else "?")
                                    print(f"   {col:<40}: ({ln} chars) {str(val)[:80]}")
                            else:
                                print(f"   {col:<40}: (empty)")
                        except Exception as e:
                            print(f"   {col:<40}: ERROR {e}")

                cur.execute(f"SELECT COUNT(*) AS N FROM `{table}`")
                n   = cur.fetchone()
                cnt = n.get("N") if isinstance(n, dict) else (n[0] if n else "?")
                print(f"\n   Rows: {cnt}")

            except Exception as e:
                print(f"\n❌ {table}: {e}")

    conn.close()
    print("\n" + "="*70)
    print("Done scanning.")
    print("="*70 + "\n")

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build full-DB FAISS vector index")
    parser.add_argument("--dry-run",  action="store_true", help="Preview without embedding")
    parser.add_argument("--scan",     action="store_true", help="Inspect tables and columns")
    parser.add_argument("--tables",   type=str, default="", help="Comma-separated table names to index")
    args = parser.parse_args()

    only = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None

    if args.scan:
        scan_columns(only_tables=only)
    else:
        run(dry_run=args.dry_run, only_tables=only)