# #!/usr/bin/env python3
# """
# Schema Manager – Reads actual MySQL schema and builds an LLM-friendly
# representation that shows REAL table names, column names, types, and row counts.

# Key improvements over v1:
#   - get_column_index(): returns {table → set(columns)} for fast validation
#   - validate_sql_columns(): pre-execution SQL sanity check — catches hallucinated
#     tables/columns and system-schema abuse before MySQL is called
#   - Rich schema format with row counts so LLM knows which tables have data
# """

# import json
# import logging
# import re
# from typing import Dict, List, Any, Optional, Set, Tuple

# from langchain_openai import ChatOpenAI
# from langchain_core.messages import SystemMessage, HumanMessage

# from config import LLM_MODEL

# logger = logging.getLogger(__name__)

# # Token budget: ~8 chars/token; 15 000 chars ≈ 1 875 tokens (very safe)
# SCHEMA_CHAR_LIMIT = 15_000

# # System schemas that must never appear in user-generated SQL
# _BLOCKED_SCHEMAS = {
#     "information_schema", "performance_schema", "mysql", "sys",
# }


# class SchemaManager:
#     """
#     Builds and caches a rich schema description from live MySQL metadata.

#     New public methods:
#       get_column_index()      → {table: {col1, col2, ...}}
#       validate_sql_columns()  → (bool, error_str)
#     """

#     def __init__(self, db_manager):
#         self.db = db_manager
#         self._table_blocks: Dict[str, str]   = {}
#         self._table_rowcounts: Dict[str, int] = {}
#         self._column_index: Dict[str, Set[str]] = {}   # NEW: table → column names
#         self._full_schema: str  = ""
#         self._all_tables: List[str] = []
#         self._build_cache()

#     # ── Internal builders ──────────────────────────────────────────────────────

#     def _build_cache(self):
#         """Read actual MySQL schema and build cached rich blocks + column index."""
#         try:
#             tables = self.db.get_all_tables()
#             self._all_tables = tables
#             logger.info("[SchemaManager] Found %d tables: %s", len(tables), tables)

#             blocks       = {}
#             col_index    = {}

#             for t in tables:
#                 schema   = self.db.get_table_schema(t)
#                 rowcount = self._get_row_count(t)
#                 self._table_rowcounts[t] = rowcount
#                 blocks[t]    = self._rich_block(schema, rowcount)
#                 col_index[t] = {c["name"] for c in schema["columns"]}

#             self._table_blocks  = blocks
#             self._column_index  = col_index
#             self._full_schema   = "\n\n".join(blocks.values())

#             total_chars = len(self._full_schema)
#             logger.info(
#                 "[SchemaManager] Schema cached: %d tables, %d chars (~%d tokens)",
#                 len(tables), total_chars, total_chars // 4,
#             )

#             for name, block in blocks.items():
#                 logger.debug("[SchemaManager] Table '%s':\n%s", name, block)

#         except Exception as exc:
#             logger.error("[SchemaManager] Cache build failed: %s", exc, exc_info=True)
#             self._full_schema = "ERROR: Could not read database schema."

#     def _get_row_count(self, table: str) -> int:
#         try:
#             result = self.db.execute_query(f"SELECT COUNT(*) FROM `{table}`")
#             if result.get("success") and result.get("rows"):
#                 return int(result["rows"][0][0])
#         except Exception:
#             pass
#         return -1

#     def _rich_block(self, schema: Dict[str, Any], rowcount: int) -> str:
#         """Format one table into a clear, LLM-readable block."""
#         t      = schema["table_name"]
#         pk_set = set(schema["primary_keys"])
#         fk_map = {fk["column"]: fk for fk in schema["foreign_keys"]}

#         row_info = f"rows: {rowcount}" if rowcount >= 0 else "rows: unknown"
#         lines = [
#             f"TABLE: `{t}`  ({row_info})",
#             "  Columns:",
#         ]

#         for col in schema["columns"]:
#             n     = col["name"]
#             dtype = col["type"].upper()
#             if col.get("max_length"):
#                 dtype += f"({col['max_length']})"

#             flags = []
#             if n in pk_set:
#                 flags.append("PRIMARY KEY")
#             if fk := fk_map.get(n):
#                 flags.append(f"FK → `{fk['references_table']}`.`{fk['references_column']}`")
#             if not col.get("nullable", True):
#                 flags.append("NOT NULL")
#             if col.get("extra") and "auto_increment" in col.get("extra", "").lower():
#                 flags.append("AUTO_INCREMENT")
#             if col.get("default") is not None:
#                 flags.append(f"DEFAULT={col['default']}")

#             flag_str = "  " + ", ".join(flags) if flags else ""
#             lines.append(f"    `{n}`  {dtype}{flag_str}")

#         if schema["foreign_keys"]:
#             lines.append("  Foreign Keys:")
#             for fk in schema["foreign_keys"]:
#                 lines.append(
#                     f"    `{fk['column']}` → `{fk['references_table']}`.`{fk['references_column']}`"
#                 )

#         return "\n".join(lines)

#     # ── Public API ─────────────────────────────────────────────────────────────

#     def get_all_table_names(self) -> List[str]:
#         return list(self._all_tables)

#     def get_table_rowcount(self, table: str) -> int:
#         return self._table_rowcounts.get(table, -1)

#     def get_full_schema(self) -> str:
#         """Full schema, truncated to SCHEMA_CHAR_LIMIT if needed."""
#         if len(self._full_schema) <= SCHEMA_CHAR_LIMIT:
#             return self._full_schema
#         truncated = self._full_schema[:SCHEMA_CHAR_LIMIT]
#         return truncated + "\n\n[SCHEMA TRUNCATED – use only tables shown above]"

#     def get_schema_for_tables(self, table_names: List[str]) -> str:
#         """Schema for a specific list of tables only."""
#         blocks = [self._table_blocks[t] for t in table_names if t in self._table_blocks]
#         return "\n\n".join(blocks) if blocks else self.get_full_schema()

#     def get_schema_for_query(self, user_question: str, max_tables: int = 12) -> str:
#         """
#         Pick the most relevant tables for this question, return their schema.
#         Falls back to full schema if table count is small or selection fails.
#         """
#         all_tables = self.get_all_table_names()

#         if len(all_tables) == 0:
#             return "No tables found in database."

#         if len(all_tables) <= max_tables:
#             schema = self.get_full_schema()
#             logger.info("[SchemaManager] Returning full schema (%d chars)", len(schema))
#             return schema

#         try:
#             selected = self._select_relevant_tables(user_question, all_tables, max_tables)
#             schema   = self.get_schema_for_tables(selected)
#             logger.info("[SchemaManager] Selected tables %s → %d chars", selected, len(schema))
#             return schema
#         except Exception as exc:
#             logger.warning("[SchemaManager] Table selection failed: %s – using full schema", exc)
#             return self.get_full_schema()

#     # ── NEW: Column Index & SQL Validation ────────────────────────────────────

#     def get_column_index(self) -> Dict[str, Set[str]]:
#         """
#         Returns a dict mapping each table name to its set of column names.
#         Used for fast pre-execution SQL validation.

#         Example:
#             {
#                 "properties": {"property_id", "property_name", "address", ...},
#                 "leases":     {"lease_id", "unit_id", "tenant_id", "start_date", ...},
#                 ...
#             }
#         """
#         return dict(self._column_index)

#     def validate_sql_columns(self, sql: str) -> Tuple[bool, str]:
#         """
#         Lightweight pre-execution SQL validation.

#         Checks:
#           1. No system schema access (information_schema, performance_schema, etc.)
#           2. All table names referenced in FROM / JOIN exist in the real schema.

#         Note: Column-level validation is intentionally skipped here because
#         aliases and subqueries make regex-based column extraction unreliable.
#         Table-name validation alone catches the vast majority of hallucinations.

#         Returns:
#             (True, "")            – SQL looks valid
#             (False, error_message) – SQL has a problem; error_message explains what
#         """
#         sql_lower = sql.lower()

#         # Check 1: system schema abuse
#         for schema in _BLOCKED_SCHEMAS:
#             if schema in sql_lower:
#                 return (
#                     False,
#                     f"SQL must not reference system schema '{schema}'. "
#                     f"Use only the application tables listed in the schema."
#                 )

#         # Check 2: table names in FROM and JOIN clauses
#         known_tables_lower = {t.lower() for t in self._all_tables}

#         # Extract table names from: FROM `table`, FROM table, JOIN `table`, JOIN table
#         pattern = r'(?:FROM|JOIN)\s+`?(\w+)`?'
#         used_tables = re.findall(pattern, sql, re.IGNORECASE)

#         for tname in used_tables:
#             tname_lower = tname.lower()
#             # Skip SQL keywords that can follow FROM/JOIN in edge cases
#             if tname_lower in {"select", "where", "on", "as", "set"}:
#                 continue
#             if tname_lower not in known_tables_lower:
#                 return (
#                     False,
#                     f"Table `{tname}` does not exist in the database schema. "
#                     f"Valid tables are: {sorted(self._all_tables)}. "
#                     f"Use ONLY these table names."
#                 )

#         return True, ""

#     # ── LLM-based table selection ──────────────────────────────────────────────

#     def _select_relevant_tables(
#         self, question: str, all_tables: List[str], max_tables: int
#     ) -> List[str]:
#         """
#         Fast LLM call to pick which tables are needed.
#         Only sends table names (not full columns) to keep this call cheap.
#         """
#         table_list = "\n".join(f"- {t}" for t in all_tables)

#         system = (
#             "You are a MySQL database expert. "
#             "Given a user question and a list of table names, "
#             "identify which tables are needed to answer the question.\n\n"
#             "Rules:\n"
#             f"- Return at most {max_tables} table names\n"
#             "- ONLY choose from the provided list\n"
#             "- Include all related/joined tables that would be needed\n"
#             "- Respond ONLY with valid JSON: {\"tables\": [\"table1\", \"table2\"]}"
#         )
#         user = f"Question: {question}\n\nAvailable tables:\n{table_list}"

#         llm      = ChatOpenAI(model=LLM_MODEL, temperature=0, max_tokens=200)
#         response = llm.invoke([SystemMessage(content=system), HumanMessage(content=user)])

#         raw = response.content.strip()
#         if raw.startswith("```"):
#             raw = re.sub(r"```\w*", "", raw).strip().strip("`").strip()

#         parsed   = json.loads(raw)
#         selected = [t for t in parsed.get("tables", []) if t in all_tables]

#         if not selected:
#             logger.warning(
#                 "[SchemaManager] LLM returned no valid tables; falling back to first %d", max_tables
#             )
#             selected = all_tables[:max_tables]

#         return selected

#     # ── Debug & refresh ───────────────────────────────────────────────────────

#     def get_debug_info(self, question: Optional[str] = None) -> Dict[str, Any]:
#         info = {
#             "total_tables":      len(self._all_tables),
#             "tables":            [
#                 {"name": t, "row_count": self._table_rowcounts.get(t, -1),
#                  "columns": sorted(self._column_index.get(t, set()))}
#                 for t in self._all_tables
#             ],
#             "full_schema_chars": len(self._full_schema),
#         }
#         if question:
#             selected_schema = self.get_schema_for_query(question)
#             info["question"]         = question
#             info["schema_for_query"] = selected_schema
#             info["schema_chars"]     = len(selected_schema)
#         return info

#     def refresh(self):
#         """Rebuild cache (call after schema changes)."""
#         logger.info("[SchemaManager] Refreshing cache …")
#         self._build_cache()



#!/usr/bin/env python3
"""
Schema Manager – Reads actual MySQL schema and builds an LLM-friendly
representation that shows REAL table names, column names, types, and row counts.

v6.0 — Disk cache persistence:
  On first run (or after refresh()):
    → Queries MySQL for full schema
    → Saves everything to  schema_cache.pkl  in the current working directory
    → Also writes a human-readable  schema_cache.txt  for inspection

  On restart (cache file exists and is fresh):
    → Loads from disk instantly — zero MySQL queries, zero LLM calls
    → "Fresh" = cache file age < SCHEMA_CACHE_TTL_HOURS (default 24 h)
    → Override with SCHEMA_CACHE_TTL_HOURS=0  to always reload from DB

  Cache file location:
    Default:  ./schema_cache.pkl  (next to your app)
    Override: SCHEMA_CACHE_PATH env var  (e.g. E:/data/schema_cache.pkl)

  Manual refresh:
    POST /schema/refresh   → forces a full MySQL re-read and saves new cache
    or call schema_manager.refresh() in code
"""

import json
import logging
import os
import pickle
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

from config import LLM_MODEL

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

SCHEMA_CHAR_LIMIT = 15_000   # chars sent to LLM per request (~1 875 tokens)

_BLOCKED_SCHEMAS = {
    "information_schema", "performance_schema", "mysql", "sys",
}

# Cache TTL: how many hours before the disk cache is considered stale
# Set to 0 to always reload from MySQL on startup
_SCHEMA_CACHE_TTL_HOURS: float = float(os.getenv("SCHEMA_CACHE_TTL_HOURS", "24"))

# Path to the pickle cache file
_SCHEMA_CACHE_PATH: Path = Path(
    os.getenv("SCHEMA_CACHE_PATH", "schema_cache.pkl")
).resolve()

# Pickle protocol version  (4 = Python 3.4+, safe for Windows paths)
_PICKLE_PROTOCOL = 4


class SchemaManager:
    """
    Builds, caches (memory + disk), and serves database schema to LLM calls.

    Startup behaviour:
      1. Try to load from disk cache (schema_cache.pkl)
         → if fresh (< TTL hours old): use it, skip MySQL entirely
         → if stale or missing: fall through to step 2
      2. Query MySQL for full schema
      3. Save result to disk cache for next restart

    Public methods:
      get_full_schema()           → full schema text (truncated to char limit)
      get_schema_for_query(q)     → relevant subset for a question
      get_schema_for_tables([t])  → schema for named tables
      get_column_index()          → {table: {col1, col2, ...}}
      validate_sql_columns(sql)   → (bool, error_str)
      get_debug_info(q)           → dict with table list, counts, columns
      refresh(force=True)         → force MySQL reload + save new cache
    """

    def __init__(self, db_manager):
        self.db = db_manager

        # In-memory state
        self._table_blocks:    Dict[str, str]      = {}
        self._table_rowcounts: Dict[str, int]      = {}
        self._column_index:    Dict[str, Set[str]] = {}
        self._full_schema:     str                 = ""
        self._all_tables:      List[str]           = []
        self._cache_loaded_at: float               = 0.0   # epoch seconds

        self._initialise()

    # ── Init / disk cache ─────────────────────────────────────────────────────

    def _initialise(self):
        """Try disk cache first; fall back to MySQL if stale/missing."""
        if self._try_load_disk_cache():
            logger.info(
                "[SchemaManager] ✅ Loaded from disk cache: %s  (%d tables)",
                _SCHEMA_CACHE_PATH, len(self._all_tables),
            )
            return

        logger.info("[SchemaManager] No valid disk cache — querying MySQL …")
        self._build_from_mysql()
        self._save_disk_cache()

    def _try_load_disk_cache(self) -> bool:
        """
        Attempt to load schema from disk.
        Returns True if cache was loaded successfully and is still fresh.
        """
        if not _SCHEMA_CACHE_PATH.exists():
            logger.info("[SchemaManager] No cache file at %s", _SCHEMA_CACHE_PATH)
            return False

        # Check age
        if _SCHEMA_CACHE_TTL_HOURS > 0:
            age_hours = (time.time() - _SCHEMA_CACHE_PATH.stat().st_mtime) / 3600
            if age_hours > _SCHEMA_CACHE_TTL_HOURS:
                logger.info(
                    "[SchemaManager] Cache is %.1f h old (TTL=%.0f h) — rebuilding from MySQL",
                    age_hours, _SCHEMA_CACHE_TTL_HOURS,
                )
                return False

        try:
            with open(_SCHEMA_CACHE_PATH, "rb") as f:
                data = pickle.load(f)

            # Validate pickle has all required keys
            required = {"table_blocks", "table_rowcounts", "column_index",
                        "full_schema", "all_tables", "saved_at"}
            missing = required - set(data.keys())
            if missing:
                logger.warning("[SchemaManager] Cache missing keys %s — rebuilding", missing)
                return False

            self._table_blocks    = data["table_blocks"]
            self._table_rowcounts = data["table_rowcounts"]
            self._column_index    = {t: set(cols) for t, cols in data["column_index"].items()}
            self._full_schema     = data["full_schema"]
            self._all_tables      = data["all_tables"]
            self._cache_loaded_at = data["saved_at"]

            saved_ago = (time.time() - self._cache_loaded_at) / 3600
            logger.info(
                "[SchemaManager] Cache loaded (saved %.1f h ago). Tables: %s",
                saved_ago, self._all_tables,
            )
            return True

        except Exception as exc:
            logger.warning("[SchemaManager] Failed to load cache (%s) — rebuilding", exc)
            return False

    def _save_disk_cache(self):
        """Persist current in-memory schema to disk."""
        try:
            # Ensure parent directory exists
            _SCHEMA_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

            data = {
                "table_blocks":    self._table_blocks,
                "table_rowcounts": self._table_rowcounts,
                # Sets are not JSON-serialisable, save as sorted lists
                "column_index":    {t: sorted(cols) for t, cols in self._column_index.items()},
                "full_schema":     self._full_schema,
                "all_tables":      self._all_tables,
                "saved_at":        time.time(),
            }

            # Write atomically: write to .tmp then rename
            tmp_path = _SCHEMA_CACHE_PATH.with_suffix(".pkl.tmp")
            with open(tmp_path, "wb") as f:
                pickle.dump(data, f, protocol=_PICKLE_PROTOCOL)
            tmp_path.replace(_SCHEMA_CACHE_PATH)

            logger.info(
                "[SchemaManager] ✅ Cache saved → %s  (%d tables, %.1f KB)",
                _SCHEMA_CACHE_PATH,
                len(self._all_tables),
                _SCHEMA_CACHE_PATH.stat().st_size / 1024,
            )

            # Also write a human-readable text version alongside the pickle
            txt_path = _SCHEMA_CACHE_PATH.with_suffix(".txt")
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"Schema Cache — saved at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Tables ({len(self._all_tables)}): {self._all_tables}\n\n")
                f.write(self._full_schema)
            logger.info("[SchemaManager] Readable schema → %s", txt_path)

        except Exception as exc:
            logger.error("[SchemaManager] Failed to save cache: %s", exc, exc_info=True)

    # ── MySQL builder ─────────────────────────────────────────────────────────

    def _build_from_mysql(self):
        """Query MySQL and populate all in-memory structures."""
        try:
            tables = self.db.get_all_tables()
            self._all_tables = tables
            logger.info("[SchemaManager] MySQL returned %d tables: %s", len(tables), tables)

            blocks    = {}
            col_index = {}

            for t in tables:
                schema   = self.db.get_table_schema(t)
                rowcount = self._get_row_count(t)
                self._table_rowcounts[t] = rowcount
                blocks[t]    = self._rich_block(schema, rowcount)
                col_index[t] = {c["name"] for c in schema["columns"]}

            self._table_blocks = blocks
            self._column_index = col_index
            self._full_schema  = "\n\n".join(blocks.values())

            logger.info(
                "[SchemaManager] Built from MySQL: %d tables, %d chars (~%d tokens)",
                len(tables), len(self._full_schema), len(self._full_schema) // 4,
            )

        except Exception as exc:
            logger.error("[SchemaManager] MySQL build failed: %s", exc, exc_info=True)
            self._full_schema = "ERROR: Could not read database schema."

    def _get_row_count(self, table: str) -> int:
        try:
            result = self.db.execute_query(f"SELECT COUNT(*) FROM `{table}`")
            if result.get("success") and result.get("rows"):
                return int(result["rows"][0][0])
        except Exception:
            pass
        return -1

    def _rich_block(self, schema: Dict[str, Any], rowcount: int) -> str:
        """Format one table into a clear, LLM-readable text block."""
        t      = schema["table_name"]
        pk_set = set(schema["primary_keys"])
        fk_map = {fk["column"]: fk for fk in schema["foreign_keys"]}

        row_info = f"rows: {rowcount}" if rowcount >= 0 else "rows: unknown"
        lines = [f"TABLE: `{t}`  ({row_info})", "  Columns:"]

        for col in schema["columns"]:
            n     = col["name"]
            dtype = col["type"].upper()
            if col.get("max_length"):
                dtype += f"({col['max_length']})"

            flags = []
            if n in pk_set:
                flags.append("PRIMARY KEY")
            if fk := fk_map.get(n):
                flags.append(f"FK → `{fk['references_table']}`.`{fk['references_column']}`")
            if not col.get("nullable", True):
                flags.append("NOT NULL")
            if col.get("extra") and "auto_increment" in col.get("extra", "").lower():
                flags.append("AUTO_INCREMENT")
            if col.get("default") is not None:
                flags.append(f"DEFAULT={col['default']}")

            flag_str = "  " + ", ".join(flags) if flags else ""
            lines.append(f"    `{n}`  {dtype}{flag_str}")

        if schema["foreign_keys"]:
            lines.append("  Foreign Keys:")
            for fk in schema["foreign_keys"]:
                lines.append(
                    f"    `{fk['column']}` → `{fk['references_table']}`.`{fk['references_column']}`"
                )

        return "\n".join(lines)

    # ── Public schema API ─────────────────────────────────────────────────────

    def get_all_table_names(self) -> List[str]:
        return list(self._all_tables)

    def get_table_rowcount(self, table: str) -> int:
        return self._table_rowcounts.get(table, -1)

    def get_full_schema(self) -> str:
        """Full schema text, hard-truncated at SCHEMA_CHAR_LIMIT."""
        if len(self._full_schema) <= SCHEMA_CHAR_LIMIT:
            return self._full_schema
        return self._full_schema[:SCHEMA_CHAR_LIMIT] + \
               "\n\n[SCHEMA TRUNCATED – use only tables shown above]"

    def get_schema_for_tables(self, table_names: List[str]) -> str:
        blocks = [self._table_blocks[t] for t in table_names if t in self._table_blocks]
        return "\n\n".join(blocks) if blocks else self.get_full_schema()

    def get_schema_for_query(self, user_question: str, max_tables: int = 12) -> str:
        """
        Return the most relevant schema subset for this question.
        If table count ≤ max_tables: return full schema (no LLM call needed).
        If larger: fire a cheap LLM call to pick which tables are relevant.
        """
        all_tables = self.get_all_table_names()

        if not all_tables:
            return "No tables found in database."

        if len(all_tables) <= max_tables:
            schema = self.get_full_schema()
            logger.info("[SchemaManager] Full schema: %d chars", len(schema))
            return schema

        try:
            selected = self._select_relevant_tables(user_question, all_tables, max_tables)
            schema   = self.get_schema_for_tables(selected)
            logger.info("[SchemaManager] Selected %s → %d chars", selected, len(schema))
            return schema
        except Exception as exc:
            logger.warning("[SchemaManager] Table selection failed: %s – full schema", exc)
            return self.get_full_schema()

    # ── Column index & SQL validation ────────────────────────────────────────

    def get_column_index(self) -> Dict[str, Set[str]]:
        """
        Return {table_name: set_of_column_names} from the cached schema.
        Used by agent_nodes._find_contract_ref_column() and _find_unit_status_column().
        """
        return dict(self._column_index)

    def validate_sql_columns(self, sql: str) -> Tuple[bool, str]:
        """
        Lightweight pre-execution SQL sanity check.

        Checks:
          1. No system schema access
          2. Every table in FROM / JOIN exists in our real schema

        Returns (True, "") or (False, human-readable error).
        """
        sql_lower = sql.lower()

        for schema in _BLOCKED_SCHEMAS:
            if schema in sql_lower:
                return (
                    False,
                    f"SQL must not reference system schema '{schema}'. "
                    f"Use only the application tables listed in the schema."
                )

        known_lower = {t.lower() for t in self._all_tables}
        pattern     = r'(?:FROM|JOIN)\s+`?(\w+)`?'
        used        = re.findall(pattern, sql, re.IGNORECASE)

        for tname in used:
            tl = tname.lower()
            if tl in {"select", "where", "on", "as", "set", "lateral"}:
                continue
            if tl not in known_lower:
                return (
                    False,
                    f"Table `{tname}` does not exist in the database schema. "
                    f"Valid tables: {sorted(self._all_tables)}. Use ONLY these names."
                )

        return True, ""

    # ── LLM table selector (only used when >max_tables exist) ────────────────

    def _select_relevant_tables(
        self, question: str, all_tables: List[str], max_tables: int
    ) -> List[str]:
        """Cheap LLM call: sends only table names, returns subset needed."""
        table_list = "\n".join(f"- {t}" for t in all_tables)
        system = (
            "You are a MySQL database expert. "
            "Given a user question and a list of table names, "
            "identify which tables are needed to answer the question.\n\n"
            f"- Return at most {max_tables} table names\n"
            "- ONLY choose from the provided list\n"
            "- Include all related/joined tables needed\n"
            "- Respond ONLY with valid JSON: {\"tables\": [\"table1\", \"table2\"]}"
        )
        user = f"Question: {question}\n\nAvailable tables:\n{table_list}"

        llm      = ChatOpenAI(model=LLM_MODEL, temperature=0, max_tokens=200)
        response = llm.invoke([SystemMessage(content=system), HumanMessage(content=user)])

        raw = response.content.strip()
        if raw.startswith("```"):
            raw = re.sub(r"```\w*", "", raw).strip().strip("`").strip()

        parsed   = json.loads(raw)
        selected = [t for t in parsed.get("tables", []) if t in all_tables]

        if not selected:
            logger.warning("[SchemaManager] LLM picked no valid tables; using first %d", max_tables)
            selected = all_tables[:max_tables]

        return selected

    # ── Cache management ──────────────────────────────────────────────────────

    def refresh(self, force: bool = True):
        """
        Force a full schema reload from MySQL and save a new disk cache.

        Call this:
          - After adding or altering tables
          - Via POST /schema/refresh in the API
          - Any time you suspect the cache is stale
        """
        logger.info("[SchemaManager] Refreshing schema from MySQL …")
        self._build_from_mysql()
        self._save_disk_cache()
        logger.info("[SchemaManager] Refresh complete. Tables: %s", self._all_tables)

    def get_cache_info(self) -> Dict[str, Any]:
        """Return metadata about the current cache state."""
        cache_exists = _SCHEMA_CACHE_PATH.exists()
        cache_age_h  = None
        cache_size_kb = None

        if cache_exists:
            cache_age_h   = round((time.time() - _SCHEMA_CACHE_PATH.stat().st_mtime) / 3600, 2)
            cache_size_kb = round(_SCHEMA_CACHE_PATH.stat().st_size / 1024, 1)

        return {
            "cache_path":      str(_SCHEMA_CACHE_PATH),
            "cache_exists":    cache_exists,
            "cache_age_hours": cache_age_h,
            "cache_size_kb":   cache_size_kb,
            "cache_ttl_hours": _SCHEMA_CACHE_TTL_HOURS,
            "tables_cached":   len(self._all_tables),
            "loaded_at":       time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(self._cache_loaded_at)
            ) if self._cache_loaded_at else "never",
        }

    def delete_cache(self):
        """Delete the disk cache file (forces MySQL reload on next restart)."""
        if _SCHEMA_CACHE_PATH.exists():
            _SCHEMA_CACHE_PATH.unlink()
            logger.info("[SchemaManager] Deleted cache file: %s", _SCHEMA_CACHE_PATH)
        txt = _SCHEMA_CACHE_PATH.with_suffix(".txt")
        if txt.exists():
            txt.unlink()

    # ── Debug ─────────────────────────────────────────────────────────────────

    def get_debug_info(self, question: Optional[str] = None) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "total_tables":      len(self._all_tables),
            "tables": [
                {
                    "name":      t,
                    "row_count": self._table_rowcounts.get(t, -1),
                    "columns":   sorted(self._column_index.get(t, set())),
                }
                for t in self._all_tables
            ],
            "full_schema_chars": len(self._full_schema),
            "cache_info":        self.get_cache_info(),
        }
        if question:
            selected = self.get_schema_for_query(question)
            info["question"]         = question
            info["schema_for_query"] = selected
            info["schema_chars"]     = len(selected)
        return info