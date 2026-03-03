#!/usr/bin/env python3
"""
Database Manager – MySQL 5.7
Thread-safe connection pool using mysql-connector-python.

Key MySQL 5.7 differences vs PostgreSQL:
  - No pgvector  → all semantic search is done through FAISS (Python-side)
  - No ILIKE     → use LIKE (MySQL string comparisons are case-insensitive by default)
  - No levenshtein() built-in → use LIKE / SOUNDEX for fuzzy match
  - Date funcs   → NOW(), DATE_ADD(), DATEDIFF(), DATE_FORMAT()
  - Backtick identifiers instead of double-quotes
  - No RETURNING clause
  - LIMIT syntax: LIMIT n  (same as PG)
  - information_schema works similarly
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple

import mysql.connector
from mysql.connector import pooling

from config import DB_CONFIG, SQL_QUERY_HARD_LIMIT

logger = logging.getLogger(__name__)

# Blocked DML/DDL keywords – only SELECT is allowed
_BLOCKED = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "TRUNCATE", "CREATE", "REPLACE", "CALL", "EXEC",
)

# System schemas that should never be queried
_BLOCKED_SCHEMAS = (
    "information_schema", "performance_schema", "mysql", "sys",
)


class DatabaseManager:
    """Thread-safe MySQL 5.7 connection pool manager."""

    def __init__(self, pool_size: int = 5):
        pool_cfg = {
            "pool_name":  "lease_pool",
            "pool_size":  pool_size,
            "host":       DB_CONFIG["host"],
            "port":       DB_CONFIG["port"],
            "database":   DB_CONFIG["database"],
            "user":       DB_CONFIG["user"],
            "password":   DB_CONFIG["password"],
            "charset":    DB_CONFIG.get("charset", "utf8mb4"),
            "autocommit": DB_CONFIG.get("autocommit", True),
            "use_pure":   True,
        }
        self._pool = mysql.connector.pooling.MySQLConnectionPool(**pool_cfg)
        logger.info("MySQL connection pool created (pool_size=%d).", pool_size)

    # ── Connection helper ───────────────────────────────────────────────────

    @contextmanager
    def _get_connection(self) -> Generator:
        conn = self._pool.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    # ── Schema helpers ──────────────────────────────────────────────────────

    def get_all_tables(self) -> List[str]:
        """Return all user table names in the current database."""
        sql = (
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME;"
        )
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Return column, PK, and FK metadata for one table."""
        with self._get_connection() as conn:
            cur = conn.cursor()

            # Columns
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                       IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION,
                       COLUMN_KEY, EXTRA
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = %s
                ORDER BY ORDINAL_POSITION;
                """,
                (table_name,),
            )
            columns = [
                {
                    "name":       r[0],
                    "type":       r[1],
                    "max_length": r[2],
                    "nullable":   r[3] == "YES",
                    "default":    r[4],
                    "position":   r[5],
                    "key":        r[6],   # PRI / MUL / UNI
                    "extra":      r[7],   # auto_increment etc.
                }
                for r in cur.fetchall()
            ]

            primary_keys = [c["name"] for c in columns if c["key"] == "PRI"]

            # Foreign keys
            cur.execute(
                """
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA          = DATABASE()
                  AND TABLE_NAME            = %s
                  AND REFERENCED_TABLE_NAME IS NOT NULL;
                """,
                (table_name,),
            )
            foreign_keys = [
                {
                    "column":            r[0],
                    "references_table":  r[1],
                    "references_column": r[2],
                }
                for r in cur.fetchall()
            ]

        return {
            "table_name":   table_name,
            "columns":      columns,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
        }

    def get_row_count(self, table_name: str) -> int:
        """Fast approximate row count for a single table."""
        try:
            result = self.execute_query(f"SELECT COUNT(*) FROM `{table_name}`")
            if result.get("success") and result.get("rows"):
                return int(result["rows"][0][0])
        except Exception:
            pass
        return -1

    def build_schema_text(self) -> str:
        """
        Generate a compact, LLM-friendly text description of the full database schema.
        Includes MySQL-specific notes.
        """
        tables = self.get_all_tables()
        lines = [
            "DATABASE SCHEMA  (MySQL 5.7)",
            "=" * 60,
            "SQL DIALECT NOTES:",
            "  • Use LIKE for text search (no ILIKE)",
            "  • Use backtick `` for reserved-word identifiers",
            "  • Date math: DATEDIFF(a,b), DATE_ADD(d, INTERVAL n DAY), NOW(), CURDATE()",
            "  • No RETURNING clause",
            "  • String concat: CONCAT(a, b)",
            "  • Boolean: TINYINT(1) where 1=true / 0=false",
            "=" * 60,
        ]

        for t in tables:
            s = self.get_table_schema(t)
            lines.append(f"\nTABLE: `{t}`")
            lines.append("-" * 40)
            for col in s["columns"]:
                pk_mark = " [PK]"          if col["name"] in s["primary_keys"] else ""
                nn_mark = " NOT NULL"       if not col["nullable"]              else ""
                ai_mark = " AUTO_INCREMENT" if "auto_increment" in col["extra"]  else ""
                lines.append(f"  `{col['name']}`  {col['type']}{pk_mark}{nn_mark}{ai_mark}")
            if s["foreign_keys"]:
                for fk in s["foreign_keys"]:
                    lines.append(
                        f"  FK: `{fk['column']}` → `{fk['references_table']}`.`{fk['references_column']}`"
                    )
        return "\n".join(lines)

    # ── Safe query execution ────────────────────────────────────────────────

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """
        Execute a read-only SELECT query with full security validation.

        Returns:
            {success, rows, column_names, row_count}  or  {success=False, error, ...}
        """
        try:
            stripped  = sql.strip()
            normalized = stripped.upper()

            # Security: block non-SELECT operations
            for bad in _BLOCKED:
                if normalized.startswith(bad) or f" {bad} " in normalized:
                    return {
                        "success":          False,
                        "error":            f"Operation '{bad}' is not permitted. Only SELECT is allowed.",
                        "validation_failed": True,
                        "is_security_issue": True,
                    }

            # Security: block system schema access
            for schema in _BLOCKED_SCHEMAS:
                if schema in normalized:
                    return {
                        "success":          False,
                        "error":            f"Querying system schema '{schema}' is not permitted.",
                        "validation_failed": True,
                        "is_security_issue": True,
                    }

            # Inject LIMIT if absent
            if "LIMIT" not in normalized:
                stripped = stripped.rstrip(";") + f" LIMIT {SQL_QUERY_HARD_LIMIT}"

            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute(stripped, params or ())
                rows      = cur.fetchall()
                col_names = [d[0] for d in cur.description] if cur.description else []

            return {
                "success":      True,
                "rows":         [list(r) for r in rows],
                "column_names": col_names,
                "row_count":    len(rows),
            }

        except mysql.connector.Error as exc:
            logger.error("MySQL error: %s | SQL: %.200s", exc, sql)
            return {"success": False, "error": str(exc), "validation_failed": False}
        except Exception as exc:
            logger.error("Unexpected query error: %s", exc, exc_info=True)
            return {"success": False, "error": str(exc), "validation_failed": False}

    def test_connection(self) -> Tuple[bool, str]:
        """Test the database connection. Returns (ok, message)."""
        try:
            result = self.execute_query("SELECT 1 AS ping")
            if result.get("success"):
                return True, "Connected"
            return False, result.get("error", "Unknown error")
        except Exception as exc:
            return False, str(exc)

    def close(self):
        """MySQL connector pool does not need explicit close; placeholder for compatibility."""
        logger.info("DatabaseManager closed.")
