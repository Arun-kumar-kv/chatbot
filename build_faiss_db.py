#!/usr/bin/env python3
"""
build_faiss_db.py
=================
Standalone script — builds the FAISS vector database for the
Property Management RAG Chatbot.

Reads credentials from .env, pulls text from MySQL,
embeds with OpenAI text-embedding-3-small, saves FAISS index + metadata.

Usage:
    python build_faiss_db.py              # build full index
    python build_faiss_db.py --dry-run    # preview rows without embedding
    python build_faiss_db.py --scan       # inspect DB tables first

Requirements:
    pip install faiss-cpu openai pymysql python-dotenv numpy
"""

import argparse
import logging
import os
import pickle
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── 0. Install check ─────────────────────────────────────────────────────────
def _check_deps():
    missing = []
    for pkg, imp in [("faiss-cpu","faiss"), ("openai","openai"),
                     ("pymysql","pymysql"), ("python-dotenv","dotenv"), ("numpy","numpy")]:
        try:
            __import__(imp)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print(f"   Run: pip install {' '.join(missing)}")
        sys.exit(1)

_check_deps()

import faiss
import numpy as np
import pymysql
import pymysql.cursors
from dotenv import load_dotenv

# ── 1. Load .env ─────────────────────────────────────────────────────────────
def _find_and_load_env() -> Path:
    """Search for .env starting from script directory upward."""
    script_dir = Path(__file__).parent.resolve()
    for directory in [script_dir, script_dir.parent, Path.cwd()]:
        env_file = directory / ".env"
        if env_file.exists():
            load_dotenv(dotenv_path=str(env_file), override=True)
            return env_file
    # Still try default load_dotenv
    load_dotenv()
    return Path(".env")

env_path = _find_and_load_env()

# ── 2. Configuration (from .env) ──────────────────────────────────────────────
# Database
DB_HOST     = os.getenv("SQL_SERVER",   "localhost")
DB_PORT     = int(os.getenv("SQL_PORT", "3306"))
DB_NAME     = os.getenv("SQL_DATABASE", "lease_management_db")
DB_USER     = os.getenv("SQL_USERNAME", "root")
DB_PASS     = os.getenv("SQL_PASSWORD", "")

# OpenAI
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")

# FAISS output paths
FAISS_INDEX_PATH    = os.getenv("FAISS_INDEX_PATH",    "E:/file/faiss.index")
FAISS_METADATA_PATH = os.getenv("FAISS_METADATA_PATH", "E:/file/metadata.pkl")

# Embedding model
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM   = int(os.getenv("EMBEDDING_DIM", "1536"))

# Tuning
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "50"))   # texts per OpenAI API call

# ── 3. Startup banner ─────────────────────────────────────────────────────────
print("=" * 65)
print("  FAISS Vector DB Builder — Property Management RAG")
print("=" * 65)
print(f"  .env file    : {env_path}")
print(f"  Database     : {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"  FAISS index  : {FAISS_INDEX_PATH}")
print(f"  Metadata     : {FAISS_METADATA_PATH}")
print(f"  Embed model  : {EMBEDDING_MODEL}  (dim={EMBEDDING_DIM})")
print(f"  Batch size   : {BATCH_SIZE}")
print("=" * 65)

if not OPENAI_API_KEY:
    print("❌ OPENAI_API_KEY not set in .env — cannot embed.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── 4. Column / table filters ─────────────────────────────────────────────────

# Tables to completely skip (auth, audit, pure-numeric lookup, binary blob)
SKIP_TABLES = {
    "TERP_AUTH_USER", "TERP_AUTH_TOKEN", "TERP_AUTH_SESSION",
    "TERP_USERS", "TERP_USER_ROLES", "TERP_ROLES", "TERP_PERMISSIONS",
    "TERP_ACC_VOUCHER_LINES", "TERP_ACC_VOUCHER_ITEMS",
    "TERP_ACC_GL_ENTRIES", "TERP_ACC_JOURNAL",
    "TERP_LS_PROPERTY_UNIT_STATUS", "TERP_LS_PROPERTY_UNIT_TYPE",
    "TERP_LS_CONTRACT_STATUS", "TERP_LS_INCIDENT_TYPE",
    "TERP_LS_COMPLAINT_CATEGORY", "TERP_LS_PAYMENT_TERM",
    "TERP_SYS_CONFIG", "TERP_SYS_SETTINGS",
    "TERP_AUDIT_LOG", "TERP_NOTIFICATIONS", "TERP_EMAIL_LOG",
}

# Column names to skip even if TEXT type (signatures, blobs, system fields)
SKIP_COLUMNS = {
    "MANAGER_SIGNATURE", "TENANT_SIGNATURE", "MGR_PARTNER_SIGN",
    "DEP_MANAGER_SIGNATURE", "SIGNATURE", "PHOTO", "IMAGE", "LOGO",
    "PASSWORD", "TOKEN", "SECRET", "HASH", "SALT",
    "CREATED_BY", "LAST_UPDATED_BY", "ISSUED_BY", "RESOLVED_BY",
    "ASSIGNED_BY", "VERIFIED_BY", "OFFICE_ID", "SITE_ID",
    "FLAG", "TIME_TAKEN", "REQUEST_TIME", "PDC", "PDC_DATE",
    "COLLECTED", "BUSINESS_HOURS_FROM", "BUSINESS_HOURS_TO",
}

# ── 5. Priority table configs — rich metadata + lookup resolution ─────────────
#
# These tables get special treatment:
#   primary_text_cols  → columns with real description text (tried in order)
#   metadata_cols      → extra fields to store in metadata (dates, IDs, status)
#   name_lookups       → FK bigint columns → resolved to human-readable names
#   where              → optional WHERE filter
#   limit              → max rows to index

PRIORITY_TABLES = {
    "TERP_MAINT_INCIDENTS": {
        "source":   "maintenance_incidents",
        "label":    "Maintenance Incidents",
        "text_cols": [
            "COMPLAINT_DESCRIPTION",   # ← confirmed longtext in your DB
            "RESOLUTION_NOTES",
            "VERIFICATION_NOTES",
            "ASSIGNMENT_NOTES",
            "DELETE_NOTES",
            "ADMIN_RESOLVE_NOTES",
            "MAT_REQUIRED_DESCRIPTION",
            "RETURN_REASON",
        ],
        "meta_cols": [
            "INCIDENT_NO", "INCIDENT_DATE", "RESOLVED_DATE",
            "DUE_DATE", "STATUS", "PRIORITY",
        ],
        "name_lookups": {
            # bigint FK col  : (lookup_table, id_col, name_col)
            "TENANT_NAME"         : ("TERP_LS_TENANTS",            "ID", "NAME"),
            "PROPERTY_NAME"       : ("TERP_LS_PROPERTY",            "ID", "NAME"),
            "INCIDENT_TYPE"       : ("TERP_LS_INCIDENT_TYPE",       "ID", "NAME"),
            "COMPLAINT_CATEGORY"  : ("TERP_LS_COMPLAINT_CATEGORY",  "ID", "NAME"),
        },
        "where": "COMPLAINT_DESCRIPTION IS NOT NULL AND COMPLAINT_DESCRIPTION != ''",
        "limit": 20000,
    },
    "TERP_LS_CONTRACT": {
        "source":   "contract",
        "label":    "Contracts",
        "text_cols": [
            "NOTES", "BUSINESS_TYPE", "LEGAL_DESCRIPTION",
            "CANCEL_COMMENT", "TENANT_COMMENTS", "APPROVAL_COMMENTS",
            "RENEWAL_REQUEST_COMMENT", "RENEWAL_SENT_COMMENTS",
            "AUTO_RENEWAL_COMMENTS",
        ],
        "meta_cols": ["CONTRACT_NUMBER", "START_DATE", "END_DATE", "ACTIVE", "GOVT_REF_NO", "TAWTHEEQ_NO"],
        "name_lookups": {
            "TENANT": ("TERP_LS_TENANTS", "ID", "NAME"),
        },
        "where": "ACTIVE = 1",
        "limit": 20000,
    },
    "TERP_LS_TENANTS": {
        "source":   "tenant",
        "label":    "Tenants",
        "text_cols": ["NAME", "EMAIL", "NOTES", "DESCRIPTION", "REMARKS", "ADDRESS"],
        "meta_cols": ["TYPE", "CREATED_AT"],
        "name_lookups": {},
        "where": "",
        "limit": 10000,
    },
    "TERP_LS_PROPERTY": {
        "source":   "property",
        "label":    "Properties",
        "text_cols": ["NAME", "ADDRESS", "DESCRIPTION", "NOTES", "LOCATION", "REMARKS"],
        "meta_cols": ["TYPE", "STATUS"],
        "name_lookups": {},
        "where": "",
        "limit": 1000,
    },
    "TERP_LS_LEGAL_TENANT_REQUEST": {
        "source":   "legal_request",
        "label":    "Legal Tenant Requests",
        "text_cols": ["DESCRIPTION", "MGMT_COMMENTS", "REQUEST_NO"],
        "meta_cols": ["DATE", "STATUS"],
        "name_lookups": {},
        "where": "",
        "limit": 5000,
    },
    "TERP_LS_TICKET_TENANT": {
        "source":   "ticket",
        "label":    "Move-out Tickets",
        "text_cols": ["REMARKS", "DESCRIPTION", "NOTES", "COMMENT", "FEEDBACK"],
        "meta_cols": ["STATUS", "CREATED_AT"],
        "name_lookups": {},
        "where": "",
        "limit": 10000,
    },
}


# ── 6. Database helpers ───────────────────────────────────────────────────────

def get_conn():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASS, database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
    )

def fetch(conn, sql: str) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() or []

def table_columns(conn, table: str) -> Dict[str, str]:
    """Returns {COLUMN_NAME_UPPER: TYPE_UPPER}"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE `{table}`")
            return {
                r.get("Field","?").upper(): r.get("Type","?").upper()
                for r in (cur.fetchall() or [])
            }
    except Exception:
        return {}

def all_tables(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        return [list(r.values())[0] for r in (cur.fetchall() or [])]

def load_lookup(conn, table: str, id_col: str, name_col: str) -> Dict[str, str]:
    """Load {str(id): name} from a lookup table."""
    try:
        rows = fetch(conn, f"SELECT `{id_col}`, `{name_col}` FROM `{table}` LIMIT 5000")
        return {str(r.get(id_col, "") or ""): str(r.get(name_col, "") or "") for r in rows}
    except Exception as e:
        logger.warning("Lookup %s failed: %s", table, e)
        return {}

def is_text_col(col: str, typ: str) -> bool:
    if col.upper() in SKIP_COLUMNS:
        return False
    if col.upper().endswith(("_SIGNATURE", "_SIGN")):
        return False
    return any(t in typ.upper() for t in ["VARCHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "CHAR"])


# ── 7. OpenAI embedder ────────────────────────────────────────────────────────

# text-embedding-3-small: 8192 token limit
# ~4 chars per token on average → 8192 * 4 = ~32768 chars theoretical max
# We use 6000 chars per text as a safe limit (≈1500 tokens), leaving room for batch overhead
MAX_CHARS_PER_TEXT = 6000

def truncate_text(text: str, max_chars: int = MAX_CHARS_PER_TEXT) -> str:
    """Truncate text to max_chars strictly, preserving word boundaries where possible."""
    text = text.replace("\n", " ").strip()
    if len(text) <= max_chars:
        return text
    # Leave 4 chars for " ..."
    cut = max_chars - 4
    truncated = text[:cut]
    # Try to trim at last word boundary (only if space found reasonably close to end)
    last_space = truncated.rfind(" ")
    if last_space > cut * 0.85:
        truncated = truncated[:last_space]
    return truncated + " ..."

def make_embedder():
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    logger.info("✅ OpenAI client ready — model: %s  (max %d chars/text)", EMBEDDING_MODEL, MAX_CHARS_PER_TEXT)

    def embed_one(text: str) -> np.ndarray:
        """Embed a single text, retrying on failure."""
        text = truncate_text(text)
        for attempt in range(4):
            try:
                resp = client.embeddings.create(input=[text], model=EMBEDDING_MODEL)
                vec  = np.array([resp.data[0].embedding], dtype="float32")
                norm = np.linalg.norm(vec, axis=1, keepdims=True)
                return vec / np.where(norm == 0, 1, norm)
            except Exception as exc:
                wait = 2 ** attempt
                logger.warning("embed_one attempt %d/4: %s — retry in %ds", attempt + 1, exc, wait)
                if attempt < 3:
                    time.sleep(wait)
                else:
                    raise

    def embed(texts: List[str]) -> np.ndarray:
        """
        Embed a batch of texts.
        - Truncates each text to MAX_CHARS_PER_TEXT first
        - If the whole batch fails (still too large), falls back to one-by-one
        """
        # Truncate every text before sending
        safe_texts = [truncate_text(t) for t in texts]

        for attempt in range(3):
            try:
                resp  = client.embeddings.create(input=safe_texts, model=EMBEDDING_MODEL)
                vecs  = np.array([item.embedding for item in resp.data], dtype="float32")
                norms = np.linalg.norm(vecs, axis=1, keepdims=True)
                return vecs / np.where(norms == 0, 1, norms)
            except Exception as exc:
                err_msg = str(exc)
                # Token limit error — fall back to one-by-one
                if "maximum context length" in err_msg or "400" in err_msg:
                    logger.warning("Batch too large (%d texts) — embedding one-by-one", len(safe_texts))
                    results = []
                    for i, t in enumerate(safe_texts):
                        results.append(embed_one(t))
                        if (i + 1) % 10 == 0:
                            logger.info("  One-by-one: %d / %d", i + 1, len(safe_texts))
                    return np.vstack(results)
                # Other error — retry with backoff
                wait = 2 ** attempt
                logger.warning("Embed attempt %d/3: %s — retry in %ds", attempt + 1, exc, wait)
                if attempt < 2:
                    time.sleep(wait)
                else:
                    raise
    return embed


# ── 8. Row → rich text document ───────────────────────────────────────────────

def row_to_doc(
    row:        Dict[str, Any],
    text_cols:  List[str],
    meta_cols:  List[str],
    source:     str,
    lookups:    Dict[str, Dict[str, str]],
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Build one searchable text document + metadata dict from a DB row.
    Returns (document_text, metadata) or (None, {}) if no useful text found.
    """
    parts    = []
    metadata = {"source": source}

    # Resolve FK bigint → human-readable label
    for col, lk in lookups.items():
        raw = str(row.get(col) or "").strip()
        if raw and raw not in ("0", "None", ""):
            label = lk.get(raw, "")
            if label and len(label) > 1:
                parts.append(f"{col.replace('_',' ').title()}: {label}")
                metadata[col.lower()] = label

    # Metadata fields (reference numbers, dates, status)
    for col in meta_cols:
        val = str(row.get(col) or "").strip()
        if val and val.lower() not in ("none", "null", "0", "1"):
            metadata[col.lower()] = val
            if any(kw in col.upper() for kw in ["NUMBER", "NO", "REF", "DATE", "STATUS", "GOVT", "TAWTHEEQ"]):
                parts.append(f"{col.replace('_',' ').title()}: {val}")

    # Primary text content
    has_text = False
    for col in text_cols:
        val = str(row.get(col) or "").strip()
        if val and len(val) > 5 and val.lower() not in ("none", "null"):
            label = col.replace("_", " ").title()
            # Truncate long text fields — keep first 3000 chars per column
            val_trunc = val[:3000] + (" ..." if len(val) > 3000 else "")
            parts.append(f"{label}: {val_trunc}")
            metadata[col.lower()] = val[:500]  # cap metadata storage
            has_text = True

    if not has_text or not parts:
        return None, {}

    doc = "\n".join(parts)
    # Final safety cap on entire document (truncate_text applied again at embed time)
    if len(doc) > MAX_CHARS_PER_TEXT:
        doc = doc[:MAX_CHARS_PER_TEXT] + " ..."

    return doc, metadata


# ── 9. Index a priority table ─────────────────────────────────────────────────

def index_priority(conn, table: str, cfg: Dict, texts: List, metas: List):
    logger.info("── [%s]", cfg["label"])

    col_info = table_columns(conn, table)
    if not col_info:
        logger.warning("  Table %s not found — skipping", table)
        return

    # Find which text columns actually exist in this DB
    text_cols = [c for c in cfg["text_cols"] if c.upper() in col_info]
    meta_cols = [c for c in cfg["meta_cols"] if c.upper() in col_info]
    if not text_cols:
        logger.warning("  No text columns found in %s — skipping", table)
        return
    logger.info("  Text cols  : %s", text_cols)
    logger.info("  Meta cols  : %s", meta_cols)

    # Load lookup tables for FK resolution
    lookups: Dict[str, Dict] = {}
    for col, (lk_tbl, lk_id, lk_name) in cfg.get("name_lookups", {}).items():
        if col.upper() in col_info:
            lookups[col] = load_lookup(conn, lk_tbl, lk_id, lk_name)
            logger.info("  Lookup     : %s → %d entries", col, len(lookups[col]))

    # Build SELECT (only columns we need)
    select_cols = list(dict.fromkeys(
        ["ID"] + text_cols + meta_cols + [c for c in cfg.get("name_lookups", {}) if c.upper() in col_info]
    ))
    select  = ", ".join(f"`{c}`" for c in select_cols)
    where   = f"WHERE {cfg['where']}" if cfg.get("where") else ""
    sql     = f"SELECT {select} FROM `{table}` {where} LIMIT {cfg.get('limit', 10000)}"

    try:
        rows = fetch(conn, sql)
    except Exception as e:
        logger.warning("  Query failed: %s — skipping", e)
        return

    indexed = 0
    for row in rows:
        # Normalise keys to UPPER
        row_up = {k.upper(): v for k, v in row.items()}
        doc, meta = row_to_doc(row_up, text_cols, meta_cols, cfg["source"], lookups)
        if doc and len(doc) >= 10:
            meta["table"]   = table.lower()
            meta["_row_id"] = str(row_up.get("ID", ""))
            texts.append(doc)
            metas.append(meta)
            indexed += 1

    logger.info("  Indexed    : %d / %d rows", indexed, len(rows))


# ── 10. Auto-index any remaining table ───────────────────────────────────────

def index_generic(conn, table: str, texts: List, metas: List):
    col_info = table_columns(conn, table)
    txt_cols = [c for c, t in col_info.items() if is_text_col(c, t)]
    if not txt_cols:
        return

    # Quick sample to check if table has any real text
    try:
        sample = fetch(conn,
            f"SELECT `{txt_cols[0]}` FROM `{table}` "
            f"WHERE `{txt_cols[0]}` IS NOT NULL AND `{txt_cols[0]}` != '' LIMIT 5"
        )
        if not sample:
            return
    except Exception:
        return

    id_col = "ID" if "ID" in col_info else list(col_info.keys())[0]
    cols   = list(dict.fromkeys([id_col] + txt_cols[:12]))
    select = ", ".join(f"`{c}`" for c in cols)

    try:
        rows = fetch(conn, f"SELECT {select} FROM `{table}` LIMIT 3000")
    except Exception as e:
        logger.debug("  Generic skip %s: %s", table, e)
        return

    source  = table.lower().replace("terp_", "").replace("_", " ").strip()
    indexed = 0
    for row in rows:
        row_up = {k.upper(): v for k, v in row.items()}
        doc, meta = row_to_doc(row_up, txt_cols, [], source, {})
        if doc and len(doc) >= 15:
            meta["table"]   = table.lower()
            meta["_row_id"] = str(row_up.get(id_col.upper(), ""))
            texts.append(doc)
            metas.append(meta)
            indexed += 1

    if indexed:
        logger.info("  [AUTO] %-45s %d docs", table, indexed)


# ── 11. Main build ────────────────────────────────────────────────────────────

def build(dry_run: bool = False, only: Optional[List[str]] = None):

    # ── Connect ───────────────────────────────────────────────────────────────
    logger.info("Connecting to MySQL at %s:%d/%s ...", DB_HOST, DB_PORT, DB_NAME)
    try:
        conn = get_conn()
        logger.info("✅ Connected")
    except Exception as e:
        logger.error("❌ DB connection failed: %s", e)
        sys.exit(1)

    embed = make_embedder()

    db_tables = all_tables(conn)
    logger.info("Found %d tables in database\n", len(db_tables))

    all_texts: List[str] = []
    all_metas: List[Dict] = []

    # ── Phase 1 : Priority tables ─────────────────────────────────────────────
    logger.info("═══ Phase 1 — Priority Tables ════════════════════════════")
    for table, cfg in PRIORITY_TABLES.items():
        if only and table not in only:
            continue
        if table in db_tables:
            index_priority(conn, table, cfg, all_texts, all_metas)
        else:
            logger.info("  SKIP %-50s (not in DB)", table)

    p1_count = len(all_texts)
    logger.info("\nPhase 1 complete: %d documents\n", p1_count)

    # ── Phase 2 : Auto-index remaining tables ─────────────────────────────────
    logger.info("═══ Phase 2 — Auto-Indexing Remaining Tables ═════════════")
    skip_set = SKIP_TABLES | set(PRIORITY_TABLES.keys())
    for table in sorted(db_tables):
        if only and table not in only:
            continue
        if table.upper() in {t.upper() for t in skip_set}:
            continue
        try:
            index_generic(conn, table, all_texts, all_metas)
        except Exception as e:
            logger.warning("  Error %s: %s", table, e)

    conn.close()

    total = len(all_texts)
    logger.info("\n  Priority : %d docs", p1_count)
    logger.info("  Other    : %d docs", total - p1_count)
    logger.info("  TOTAL    : %d documents\n", total)

    if total == 0:
        logger.error("No documents collected. Run --scan to inspect your tables.")
        sys.exit(1)

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        logger.info("═══ DRY RUN — Preview (no embedding) ════════════════════")
        counts = Counter(m.get("source","?") for m in all_metas)
        for src, cnt in counts.most_common():
            logger.info("  %-40s  %d docs", src, cnt)
        logger.info("\nSample documents:")
        for i in range(min(5, total)):
            logger.info("\n─── [%d] source=%s\n%s", i, all_metas[i].get("source"), all_texts[i][:300])
        return

    # ── Embed ─────────────────────────────────────────────────────────────────
    logger.info("═══ Embedding with OpenAI %s ══════════════════════", EMBEDDING_MODEL)
    vectors = []
    for start in range(0, total, BATCH_SIZE):
        batch = all_texts[start : start + BATCH_SIZE]
        vecs  = embed(batch)
        vectors.append(vecs)
        done = start + len(batch)
        pct  = done / total * 100
        if start % (BATCH_SIZE * 10) == 0 or done >= total:
            logger.info("  %d / %d  (%.0f%%)", done, total, pct)

    matrix = np.vstack(vectors)
    logger.info("Embedding matrix: %s  dtype=%s\n", matrix.shape, matrix.dtype)

    # ── Build FAISS index ─────────────────────────────────────────────────────
    logger.info("═══ Building FAISS Index ════════════════════════════════")
    idx_path  = Path(FAISS_INDEX_PATH)
    meta_path = Path(FAISS_METADATA_PATH)

    idx_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    index = faiss.IndexFlatL2(EMBEDDING_DIM)
    index.add(matrix)
    logger.info("Vectors in index: %d  dim=%d", index.ntotal, index.d)

    faiss.write_index(index, str(idx_path))
    logger.info("✅ FAISS index saved  → %s", idx_path)

    with open(meta_path, "wb") as f:
        pickle.dump(all_metas, f)
    logger.info("✅ Metadata saved     → %s  (%d entries)", meta_path, len(all_metas))

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("\n═══ INDEX COMPOSITION ════════════════════════════════════")
    counts = Counter(m.get("source","?") for m in all_metas)
    for src, cnt in counts.most_common():
        logger.info("  %-40s  %d", src, cnt)
    logger.info("  %-40s  %d  ← TOTAL", "─────────────────", index.ntotal)
    logger.info("══════════════════════════════════════════════════════════\n")
    logger.info("Done! Run your server — RAG queries will now use the FAISS index.")


# ── 12. Scan mode ─────────────────────────────────────────────────────────────

def scan(only: Optional[List[str]] = None):
    """Print column info + sample values for all tables."""
    print("\n" + "═"*65)
    print("  DB SCAN")
    print("═"*65)
    try:
        conn = get_conn()
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        sys.exit(1)

    tables = only or all_tables(conn)
    with conn.cursor() as cur:
        for table in tables:
            try:
                cur.execute(f"DESCRIBE `{table}`")
                cols = cur.fetchall() or []
                txt_cols = [
                    r.get("Field","?") for r in cols
                    if any(t in str(r.get("Type","")).upper()
                           for t in ["VARCHAR","TEXT","LONGTEXT","CHAR"])
                    and r.get("Field","?").upper() not in SKIP_COLUMNS
                ]

                cur.execute(f"SELECT COUNT(*) AS N FROM `{table}`")
                n = (cur.fetchone() or {}).get("N", "?")

                print(f"\n{'─'*65}")
                print(f"  📋 {table}   ({n} rows,  {len(txt_cols)} text cols)")
                print(f"{'─'*65}")
                for r in cols:
                    col = r.get("Field","?")
                    typ = r.get("Type","?")
                    tag = "← TEXT" if col in txt_cols else ""
                    print(f"  {col:<40} {str(typ):<22} {tag}")

                if txt_cols:
                    print(f"\n  Sample text values:")
                    for col in txt_cols[:6]:
                        try:
                            cur.execute(
                                f"SELECT `{col}`, LENGTH(`{col}`) AS L "
                                f"FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != '' "
                                f"ORDER BY L DESC LIMIT 2"
                            )
                            samples = cur.fetchall() or []
                            if samples:
                                for s in samples:
                                    v  = str(s.get(col, "?"))[:80]
                                    ln = s.get("L", "?")
                                    print(f"  {col:<40}: ({ln} chars) {v}")
                            else:
                                print(f"  {col:<40}: (all empty)")
                        except Exception as e:
                            print(f"  {col:<40}: ERROR {e}")
            except Exception as e:
                print(f"\n❌ {table}: {e}")

    conn.close()
    print("\n" + "═"*65)
    print("  Scan complete.")
    print("═"*65 + "\n")


# ── 13. Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Build FAISS vector DB for Property Management RAG",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python build_faiss_db.py                        # full build
  python build_faiss_db.py --dry-run              # preview without embedding
  python build_faiss_db.py --scan                 # inspect DB tables
  python build_faiss_db.py --tables TERP_MAINT_INCIDENTS,TERP_LS_CONTRACT
        """
    )
    ap.add_argument("--dry-run",  action="store_true", help="Preview rows, skip embedding")
    ap.add_argument("--scan",     action="store_true", help="Inspect DB tables and columns")
    ap.add_argument("--tables",   default="",          help="Comma-separated table names to index")
    args = ap.parse_args()

    only_tables = [t.strip() for t in args.tables.split(",") if t.strip()] or None

    if args.scan:
        scan(only=only_tables)
    else:
        build(dry_run=args.dry_run, only=only_tables)