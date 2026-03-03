#!/usr/bin/env python3
"""
LangGraph Node Functions - MySQL 5.7 + FAISS + SchemaManager

v6.0 — Three new features:
  - Fuzzy correction  : typos in property/tenant/contract names fixed before SQL
  - Scope guard       : out-of-scope questions rejected before any DB query
  - Memory resolution : pronouns / references resolved from conversation history

Circular import fix (v6.1):
  Shared LLM helpers (get_llm, llm_invoke_with_retry, parse_json) live in
  llm_utils.py.  preprocessor.py imports from llm_utils.py directly — no
  longer imports from agent_nodes.py, breaking the old circular dependency.
"""

import json
import logging
import re
from typing import Any, Dict, Optional, Set

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from config import MAX_SQL_RETRIES
from agent_state import AgentState
from llm_utils import get_llm as _get_llm, llm_invoke_with_retry, parse_json as _parse_json
from prompts import (
    ROUTER_SYSTEM_PROMPT,
    create_sql_generation_prompt,
    create_sql_retry_message,
    create_intent_context,
    FINAL_ANSWER_SYSTEM_PROMPT,
    create_final_answer_user_message,
    CONVERSATIONAL_SYSTEM_PROMPT,
)
# preprocessor is imported LAZILY inside preprocess_node to guarantee
# llm_utils is fully initialised before preprocessor loads


logger = logging.getLogger(__name__)

_MAX_HISTORY_MESSAGES = 6
_MAX_SQL_RESULT_LINES = 120
_DB_RAG_TOP_K = 400
_DB_RAG_SCORE_THRESHOLD = 0.35

_HYBRID_CAUSAL_KEYWORDS = {
    "why did", "why has", "why is", "why are", "why was", "why were",
    "what caused", "what cause", "how did", "how does", "how do",
    "correlation between", "correlate", "impact of", "effect of",
    "reason for", "led to", "resulted in", "due to",
}

_CONTRACT_REF_CANDIDATES = [
    "CONTRACT_NO", "CONTRACT_NUMBER", "CONTRACT_NAME", "REF_NO",
    "REFERENCE", "NAME", "CODE", "CONTRACT_REF", "LEASE_NO",
]
_UNIT_STATUS_COL_CANDIDATES = [
    "STATUS", "UNIT_STATUS", "STATUS_ID", "UNIT_STATUS_ID",
]


def _extract_sql_query_from_response(content: str) -> str:
    """
    Best-effort SQL extraction when the LLM does not return strict JSON.

    Accepted patterns:
      1) {"sql_query": "SELECT ..."}
      2) ```sql ... ```
      3) Raw SQL text beginning with SELECT/WITH
    """
    if not content:
        return ""

    raw = content.strip()

    # 1) Strict/near-strict JSON path.
    try:
        parsed = _parse_json(raw)
        sql = str(parsed.get("sql_query", "") or "").strip()
        if sql:
            return sql
    except Exception:
        pass

    # 2) SQL code block path.
    block = re.search(r"```(?:sql)?\s*(.*?)```", raw, flags=re.IGNORECASE | re.DOTALL)
    if block:
        candidate = block.group(1).strip()
        if candidate:
            return candidate

    # 3) Raw SQL fallback path (handles extra prose before/after query).
    m = re.search(r"\b(SELECT|WITH)\b[\s\S]*", raw, flags=re.IGNORECASE)
    if not m:
        return ""

    candidate = m.group(0).strip()
    # Trim common explanatory tail text if present.
    candidate = re.split(
        r"\n\s*(?:Explanation|Reasoning|Notes?)\s*:\s*",
        candidate,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip()
    return candidate


def _truncate_sql_text(text: str) -> str:
    if not text:
        return ""
    lines = text.split("\n")
    if len(lines) <= _MAX_SQL_RESULT_LINES:
        return text
    return "\n".join(lines[:_MAX_SQL_RESULT_LINES]) + \
           f"\n... ({len(lines) - _MAX_SQL_RESULT_LINES} more lines)"


def _has_causal_keywords(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _HYBRID_CAUSAL_KEYWORDS)


def _sanitize_rag_answer_claims(answer: str, retrieved_count: int) -> str:
    """Prevent dataset-wide count claims in qualitative RAG answers."""
    if not answer:
        return answer

    replacement = f"a retrieved set of {retrieved_count} relevant records"
    answer = re.sub(
        r"total\s+of\s+\*\*?\d[\d,]*\s+records\*\*?",
        replacement,
        answer,
        flags=re.IGNORECASE,
    )
    answer = re.sub(
        r"total\s+of\s+\d[\d,]*\s+records",
        replacement,
        answer,
        flags=re.IGNORECASE,
    )
    return answer


# ── Schema column helpers ──────────────────────────────────────────────────────

def _find_contract_ref_column(schema_manager) -> Optional[str]:
    try:
        col_index = schema_manager.get_column_index()
        contract_cols: Set[str] = col_index.get("TERP_LS_CONTRACT", set())
        for candidate in _CONTRACT_REF_CANDIDATES:
            if candidate in contract_cols:
                return candidate
        for col in contract_cols:
            cu = col.upper()
            if any(kw in cu for kw in ["_NO", "NUMBER", "REF", "NAME", "CODE"]):
                if cu not in {"ID", "ACTIVE", "TENANT", "PROPERTY"}:
                    return col
    except Exception as exc:
        logger.warning("[ContractRefCol] %s", exc)
    return None


def _find_unit_status_column(schema_manager) -> Optional[str]:
    try:
        col_index = schema_manager.get_column_index()
        unit_cols: Set[str] = col_index.get("TERP_LS_PROPERTY_UNIT", set())
        for candidate in _UNIT_STATUS_COL_CANDIDATES:
            if candidate in unit_cols:
                return candidate
    except Exception as exc:
        logger.warning("[UnitStatusCol] %s", exc)
    return None


def _build_schema_aware_hints(user_question: str, schema_manager) -> str:
    q      = user_question.lower()
    hints  = []

    is_contract_lookup = (
        any(kw in q for kw in [
            "contract/", "expiry of contract", "find contract",
            "contract expiry", "contract detail", "contract no",
            "contract number", "contract ref",
        ])
        or ("/" in user_question and any(c.isdigit() for c in user_question) and "contract" in q)
    )

    if is_contract_lookup:
        ref_col = _find_contract_ref_column(schema_manager)
        if ref_col:
            hints.append(
                f"SCHEMA FACT: Contract reference strings are in `TERP_LS_CONTRACT`.`{ref_col}`\n"
                f"Use: WHERE c.`{ref_col}` LIKE '%<contract_number>%'\n"
                f"Do NOT use WHERE c.ID = '...' — c.ID is an integer primary key."
            )
        else:
            hints.append(
                "SCHEMA FACT: Contract reference strings are NOT in c.ID (integer PK).\n"
                "Find the text column (CONTRACT_NO, NAME, REF_NO) in the schema and use LIKE."
            )

    is_vacancy = any(kw in q for kw in [
        "vacant", "vacancy", "available unit", "empty unit",
        "unoccupied", "free unit", "total vacant",
    ])

    if is_vacancy:
        status_col = _find_unit_status_column(schema_manager)
        if status_col:
            hints.append(
                f"SCHEMA FACT: Vacancy is determined by TERP_LS_PROPERTY_UNIT_STATUS.\n"
                f"Link column in TERP_LS_PROPERTY_UNIT: `{status_col}`\n"
                f"Correct pattern:\n"
                f"  FROM TERP_LS_PROPERTY_UNIT u\n"
                f"  INNER JOIN TERP_LS_PROPERTY_UNIT_STATUS s ON s.ID = u.`{status_col}`\n"
                f"  INNER JOIN TERP_LS_PROPERTY p ON u.PROPERTY_ID = p.ID\n"
                f"  WHERE s.STATUS = 'Available' AND p.NAME LIKE '%<name>%'\n"
                f"  GROUP BY p.NAME, s.STATUS"
            )

    return ("\n\n--- LIVE SCHEMA FACTS ---\n" + "\n\n".join(hints) + "\n---") if hints else ""


# ── NODE 0: Preprocess ────────────────────────────────────────────────────────

def preprocess_node(
    state: AgentState,
    fuzzy_matcher,
    **_,
) -> AgentState:
    """
    Runs BEFORE routing. Three steps:
      1. FUZZY CORRECTION  – fix typos in property/tenant/contract names
      2. MEMORY RESOLUTION – expand pronouns/references from conversation history
      3. SCOPE GUARD       – reject non-ERP questions immediately

    Lazy-imports preprocessor here (not at module level) to avoid the
    circular import: agent_nodes ↔ preprocessor.
    """
    import preprocessor as _pre   # lazy import — breaks circular dependency

    result = _pre.run(
        user_question        = state["user_question"],
        conversation_history = state.get("messages", []),
        fuzzy_matcher        = fuzzy_matcher,
    )

    if result.is_out_of_scope:
        logger.info("[Preprocess] Out of scope: '%s'", state["user_question"])
        return {
            **state,
            "resolved_question":   state["user_question"],
            "fuzzy_corrections":   result.fuzzy_corrections,
            "is_out_of_scope":     True,
            "strategy":            "out_of_scope",
            "final_answer":        result.refusal_message,
            "success":             True,
            "messages":            state.get("messages", []) + [
                HumanMessage(content=state["user_question"]),
                AIMessage(content=result.refusal_message),
            ],
        }

    if result.resolved_question != state["user_question"]:
        logger.info(
            "[Preprocess] '%s' → '%s'  corrections=%s",
            state["user_question"], result.resolved_question, result.fuzzy_corrections,
        )

    return {
        **state,
        "resolved_question": result.resolved_question,
        "fuzzy_corrections": result.fuzzy_corrections,
        "is_out_of_scope":   False,
    }


# ── Question Type Classification ──────────────────────────────────────────────
#
# THREE types:
#   QUALITATIVE (db_rag)  — needs free-text complaint/feedback content from FAISS
#   QUANTITATIVE (sql)    — needs numbers, counts, lists from structured DB tables
#   HYBRID                — needs both (causal analysis: "why did revenue drop?")
#
# RULE: Only route to db_rag when question is about the CONTENT/NATURE of complaints
#       i.e., "what KIND of complaints" / "what do tenants SAY" / "themes in feedback"
#       NOT for any question that can be answered with numbers from the DB.

# Strictly qualitative — about complaint CONTENT/THEMES, not counts or tenant lists
_QUALITATIVE_COMPLAINT_PHRASES = {
    # Asking about complaint content/themes
    "types of complaint", "type of complaint", "kinds of complaint",
    "kind of complaint", "nature of complaint", "complaint themes",
    "what complaints", "which complaints", "what are the complaints",
    "what kind of complaint", "what types of complaint",
    "frequently reported complaint", "frequently received complaint",
    "most common complaint", "common complaints",
    # Asking about what tenants SAY/REPORT (content)
    "what do tenants report", "what do tenants complain",
    "what issues do tenants", "what problems do tenants",
    "tenant feedback", "tenant concerns", "tenant dissatisfaction",
    "move-out reason", "move out reason", "reason for leaving",
    "ticket remarks", "move-out remark", "remarks from tenant",
    # Legal content
    "nature of legal request", "types of legal request",
}

# These are QUANTITATIVE even if they use qualitative-sounding words
# (they ask for lists/counts/rankings, not text content)
_QUANTITATIVE_OVERRIDES = {
    # Risk / performance questions → SQL
    "high-risk", "high risk", "at-risk", "at risk",
    "low performing", "top tenant", "bottom tenant",
    "highest outstanding", "most overdue", "most dues",
    "risk based", "based on revenue", "based on rent",
    "based on payment", "based on outstanding",
    # List/ranking questions → SQL
    "list of tenant", "list tenants", "show tenants",
    "list of property", "list properties", "show properties",
    "list of contract", "list contracts",
    "top 10", "top 5", "bottom 10", "worst",
    # Vacancy / financial → SQL
    "vacant unit", "vacancy rate", "occupancy",
    "revenue", "income", "outstanding", "dues", "balance",
    "rent", "payment", "overdue", "bounced", "cheque",
    # Count/aggregate → SQL
    "how many", "total number", "count of", "number of",
    "percentage", "rate", "ratio", "average",
}


def _classify_question(question: str) -> str:
    """
    Classify question into: 'db_rag', 'sql_only', or 'llm_decides'

    Returns:
      'db_rag'     → qualitative complaint content question → FAISS RAG
      'sql_only'   → clear quantitative/list/ranking question → SQL
      'llm_decides'→ ambiguous, let the LLM router decide
    """
    q = question.lower()

    # Step 1: Check quantitative overrides FIRST — these are never db_rag
    if any(kw in q for kw in _QUANTITATIVE_OVERRIDES):
        return "sql_only"

    # Step 2: Check strict qualitative complaint phrases
    if any(phrase in q for phrase in _QUALITATIVE_COMPLAINT_PHRASES):
        return "db_rag"

    # Step 3: Loose complaint keywords — only qualify if NO quantitative context
    _LOOSE_COMPLAINT_KW = {
        "complaint", "complaints", "complain",
        "tenant issue", "tenant problem", "tenant complaint",
        "maintenance problem", "maintenance issue",
        "open ticket", "unresolved complaint",
        "legal request", "legal issue",
        "frequently reported", "frequently received",
    }
    if any(kw in q for kw in _LOOSE_COMPLAINT_KW):
        # Extra check: if question has numbers/ranking intent → sql_only
        _numeric_intent = {
            "how many", "total", "count", "number", "most", "least",
            "highest", "lowest", "top", "bottom", "rank", "list",
            "show", "which tenant", "which property",
        }
        if any(kw in q for kw in _numeric_intent):
            return "sql_only"
        return "db_rag"

    return "llm_decides"


# ── NODE 1: Route Query ────────────────────────────────────────────────────────

def route_query_node(state, db_manager, vector_store, schema_manager) -> AgentState:
    llm      = _get_llm(temperature=0, max_tokens=150)
    question = state.get("resolved_question") or state["user_question"]

    # ── Step 1: Classify question type ────────────────────────────────────────
    question_type = _classify_question(question)
    logger.info("[Router] question_type=%s for: %.80s", question_type, question)

    if question_type == "db_rag":
        return {
            **state,
            "strategy":            "db_rag",
            "routing_reasoning":   "Qualitative complaint/feedback question → FAISS RAG synthesis",
            "vector_query":        question,
            "sql_attempt":         0,
            "sql_attempt_history": [],
            "max_sql_retries":     MAX_SQL_RETRIES,
            "need_embedding":      False,
            "embedding_params":    [],
        }

    if question_type == "sql_only":
        logger.info("[Router] Quantitative question → sql_only (bypassing LLM router)")
        return {
            **state,
            "strategy":            "sql_only",
            "routing_reasoning":   "Quantitative/list/ranking question → SQL",
            "vector_query":        None,
            "sql_attempt":         0,
            "sql_attempt_history": [],
            "max_sql_retries":     MAX_SQL_RETRIES,
            "need_embedding":      False,
            "embedding_params":    [],
        }

    # ── Step 2: Force hybrid for causal+numeric questions ─────────────────────
    if _has_causal_keywords(question):
        numeric_kw = {
            "revenue", "income", "rent", "rate", "count", "number", "amount",
            "occupancy", "vacancy", "percentage", "%", "drop", "increase",
            "decrease", "growth", "decline", "trend", "outstanding", "receivable",
        }
        if any(kw in question.lower() for kw in numeric_kw):
            logger.info("[Router] Causal + numeric → forcing 'hybrid'")
            return {
                **state,
                "strategy":            "hybrid",
                "routing_reasoning":   "Causal numeric question → hybrid",
                "vector_query":        question,
                "sql_attempt":         0,
                "sql_attempt_history": [],
                "max_sql_retries":     MAX_SQL_RETRIES,
                "need_embedding":      False,
                "embedding_params":    [],
            }

    # ── Step 3: Let LLM decide for ambiguous questions ────────────────────────
    try:
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=ROUTER_SYSTEM_PROMPT),
            HumanMessage(content=question),
        ], context="router")
        parsed = _parse_json(response.content)
    except Exception as exc:
        logger.warning("[Router] LLM error: %s → sql_only", exc)
        parsed = {"strategy": "sql_only", "reasoning": str(exc), "vector_query": None}

    strategy = parsed.get("strategy", "sql_only")
    logger.info("[Router] LLM strategy=%s", strategy)

    return {
        **state,
        "strategy":            strategy,
        "routing_reasoning":   parsed.get("reasoning", ""),
        "vector_query":        parsed.get("vector_query"),
        "sql_attempt":         0,
        "sql_attempt_history": [],
        "max_sql_retries":     MAX_SQL_RETRIES,
        "need_embedding":      False,
        "embedding_params":    [],
    }




# SQL queries to fetch actual complaint text — COALESCE tries multiple column names
# so it works regardless of your exact schema column naming
_COMPLAINT_TEXT_QUERIES = [
    (
        "Maintenance Incidents",
        """SELECT
                  t.NAME AS TENANT_NAME,
                  p.NAME AS PROPERTY_NAME,
                  it.NAME AS INCIDENT_TYPE,
                  cc.NAME AS COMPLAINT_CATEGORY,
                  mi.INCIDENT_DATE,
                  CASE WHEN mi.RESOLVED_DATE IS NULL THEN 'Open' ELSE 'Resolved' END AS STATUS,
                  COALESCE(
                      mi.COMPLAINT_DESCRIPTION,
                      mi.RESOLUTION_NOTES,
                      mi.VERIFICATION_NOTES,
                      mi.ASSIGNMENT_NOTES,
                      mi.DELETE_NOTES,
                      mi.ADMIN_RESOLVE_NOTES,
                      mi.MAT_REQUIRED_DESCRIPTION,
                      mi.RETURN_REASON
                  ) AS COMPLAINT_TEXT
           FROM TERP_MAINT_INCIDENTS mi
           LEFT JOIN TERP_LS_TENANTS t ON t.ID = mi.TENANT_NAME
           LEFT JOIN TERP_LS_PROPERTY_UNIT pu ON pu.ID = mi.PROPERTY_UNIT
           LEFT JOIN TERP_LS_PROPERTY p ON p.ID = pu.PROPERTY_ID
           LEFT JOIN TERP_LS_INCIDENT_TYPE it ON it.ID = mi.INCIDENT_TYPE
           LEFT JOIN TERP_LS_COMPLAINT_CATEGORY cc ON cc.ID = mi.COMPLAINT_CATEGORY
           WHERE mi.COMPLAINT_DESCRIPTION IS NOT NULL
             AND mi.COMPLAINT_DESCRIPTION != ''
             AND LENGTH(mi.COMPLAINT_DESCRIPTION) > 10
           ORDER BY mi.INCIDENT_DATE DESC
           LIMIT 120""",
    ),
    (
        "Move-out Tickets",
        """SELECT
                  tt.ID,
                  CASE WHEN tt.STATUS = 1 THEN 'Resolved' ELSE 'Open' END AS STATUS,
                  tt.CREATED_AT,
                  COALESCE(tt.REMARKS, tt.DESCRIPTION, tt.NOTES,
                           tt.COMPLAINT, tt.FEEDBACK, tt.COMMENT
                  ) AS COMPLAINT_TEXT
           FROM TERP_LS_TICKET_TENANT tt
           WHERE COALESCE(tt.REMARKS, tt.DESCRIPTION, tt.NOTES,
                          tt.COMPLAINT, tt.FEEDBACK, tt.COMMENT) IS NOT NULL
           ORDER BY tt.ID DESC
           LIMIT 40""",
    ),
    (
        "Legal Tenant Requests",
        """SELECT
                  lr.ID,
                  lr.DATE,
                  COALESCE(lr.DESCRIPTION, lr.MGMT_COMMENTS) AS COMPLAINT_TEXT
           FROM TERP_LS_LEGAL_TENANT_REQUEST lr
           WHERE COALESCE(lr.DESCRIPTION, lr.MGMT_COMMENTS) IS NOT NULL
             AND COALESCE(lr.DESCRIPTION, lr.MGMT_COMMENTS) != ''
           ORDER BY lr.ID DESC
           LIMIT 20""",
    ),
]


_DB_RAG_COUNT_QUERIES = [
    """SELECT COUNT(*) AS CNT
       FROM TERP_MAINT_INCIDENTS mi
       WHERE mi.COMPLAINT_DESCRIPTION IS NOT NULL
         AND mi.COMPLAINT_DESCRIPTION != ''
         AND LENGTH(mi.COMPLAINT_DESCRIPTION) > 10""",
    """SELECT COUNT(*) AS CNT
       FROM TERP_LS_TICKET_TENANT tt
       WHERE COALESCE(tt.REMARKS, tt.DESCRIPTION, tt.NOTES,
                      tt.COMPLAINT, tt.FEEDBACK, tt.COMMENT) IS NOT NULL
         AND COALESCE(tt.REMARKS, tt.DESCRIPTION, tt.NOTES,
                      tt.COMPLAINT, tt.FEEDBACK, tt.COMMENT) != ''""",
    """SELECT COUNT(*) AS CNT
       FROM TERP_LS_LEGAL_TENANT_REQUEST lr
       WHERE COALESCE(lr.DESCRIPTION, lr.MGMT_COMMENTS) IS NOT NULL
         AND COALESCE(lr.DESCRIPTION, lr.MGMT_COMMENTS) != ''""",
]


def _count_db_rag_records(db_manager) -> Optional[int]:
    """Best-effort count of text-bearing records used for qualitative complaint analysis."""
    total = 0
    ok = False

    for sql in _DB_RAG_COUNT_QUERIES:
        try:
            result = db_manager.execute_query(sql.strip())
            if not result.get("success"):
                continue
            rows = result.get("rows") or []
            if not rows:
                continue
            first = rows[0]
            if isinstance(first, dict):
                cnt = first.get("CNT", 0)
            else:
                cnt = first[0] if len(first) > 0 else 0
            total += int(cnt or 0)
            ok = True
        except Exception as exc:
            logger.warning("[DB-RAG] Count query error: %s", exc)

    return total if ok else None


def db_rag_node(state: AgentState, db_manager, **_) -> AgentState:
    """
    Fetches actual complaint/ticket text rows directly from MySQL.
    Formats them as RAG context for the synthesiser.
    Used for qualitative questions like 'what types of complaints are reported'.
    """
    question = state.get("resolved_question") or state["user_question"]
    logger.info("[DB-RAG] Fetching complaint text for: %.80s", question)

    all_chunks = []
    queries_tried = 0

    for source_label, sql in _COMPLAINT_TEXT_QUERIES:
        try:
            result = db_manager.execute_query(sql.strip())
            if not result.get("success"):
                logger.warning("[DB-RAG] %s query failed: %s", source_label, result.get("error"))
                continue

            rows = result.get("rows", [])
            cols = result.get("column_names", [])
            queries_tried += 1

            valid_rows = 0
            for row in rows:
                # Build dict from column names + row values
                if isinstance(row, dict):
                    row_dict = row
                else:
                    row_dict = dict(zip(cols, row))

                text = str(row_dict.get("COMPLAINT_TEXT") or "").strip()
                if not text or len(text) < 8 or text.startswith("Maintenance incident by") and "Unknown" in text:
                    continue  # skip empty / auto-generated fallback rows

                meta_parts = []
                for k in ["TENANT_NAME", "PROPERTY_NAME", "INCIDENT_DATE", "STATUS"]:
                    v = row_dict.get(k)
                    if v and str(v).strip():
                        meta_parts.append(f"{k.replace('_',' ').title()}: {v}")

                meta = "  |  ".join(meta_parts) if meta_parts else ""
                chunk = f"[{source_label}]{f'  {meta}' if meta else ''}\n{text}"
                all_chunks.append(chunk)
                valid_rows += 1

            logger.info("[DB-RAG] %s → %d text rows", source_label, valid_rows)

        except Exception as exc:
            logger.warning("[DB-RAG] %s error: %s", source_label, exc)

    if all_chunks:
        rag_text = (
            f"Retrieved {len(all_chunks)} complaint/ticket records from database:\n"
            f"{'='*60}\n\n" +
            "\n\n---\n".join(all_chunks)
        )
        logger.info("[DB-RAG] Built RAG context: %d chunks, %d chars", len(all_chunks), len(rag_text))
    else:
        # Fallback: no text columns found — use count summary from SQL
        rag_text = ""
        logger.warning("[DB-RAG] No text content found in complaint tables — falling back to SQL counts")

    return {
        **state,
        "vector_results_text": rag_text,
        "vector_results":      [{"text": c, "source": "db_rag"} for c in all_chunks],
        "rag_total_records":   _count_db_rag_records(db_manager),
        "strategy":            "db_rag",   # special strategy for synthesiser
    }


def vector_search_node(state: AgentState, vector_store, db_manager=None, **_) -> AgentState:
    query    = state.get("vector_query") or state.get("resolved_question") or state["user_question"]
    strategy = state.get("strategy", "vector_only")
    is_rag   = strategy == "db_rag"

    # ── FAISS unavailable fallback ────────────────────────────────────────────
    if not vector_store or not vector_store.is_available:
        if is_rag and db_manager:
            # Fall back to direct SQL-based complaint fetching
            logger.info("[VectorSearch] FAISS not available — falling back to SQL-based RAG for db_rag query")
            return db_rag_node(state, db_manager=db_manager)
        return {**state, "vector_results": [], "vector_results_text": "FAISS unavailable."}

    # Qualitative RAG queries should scan the full vector index (user requested all matches).
    threshold = 0.00 if is_rag else None
    top_k     = (vector_store.total_vectors if is_rag else None)

    try:
        if threshold is not None and top_k is not None:
            results = vector_store.search(query, top_k=top_k, score_threshold=threshold)
        else:
            results = vector_store.search(query)
    except Exception as exc:
        logger.error("[VectorSearch] search failed: %s", exc)
        if is_rag and db_manager:
            logger.info("[VectorSearch] Search error — falling back to SQL-based RAG")
            return db_rag_node(state, db_manager=db_manager)
        results = []

    if is_rag and results:
        # Defensive cap and de-duplication for noisy indexes / unexpected backend behaviour.
        deduped = []
        seen = set()
        for r in results:
            key = (r.get("text") or r.get("content") or r.get("description") or "").strip().lower()
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            deduped.append(r)

        if len(deduped) > _DB_RAG_TOP_K:
            logger.warning("[VectorSearch] db_rag returned %d results, trimming to top %d", len(deduped), _DB_RAG_TOP_K)
            deduped = deduped[:_DB_RAG_TOP_K]
        results = deduped

    # ── Build clean RAG context from metadata ─────────────────────────────────
    if results:
        chunks = []
        for r in results:
            # Extract the main readable text — try multiple field names
            text = (
                r.get("text") or
                r.get("content") or
                r.get("description") or
                r.get("remarks") or
                r.get("notes") or
                ""
            ).strip()

            if not text:
                # Last resort: concatenate all non-private string fields
                parts = [
                    f"{k}: {v}" for k, v in r.items()
                    if not k.startswith("_") and isinstance(v, str) and v.strip()
                ]
                text = "  |  ".join(parts)

            if not text:
                continue

            # Build metadata header for context
            score  = r.get("_score", 0)
            table  = r.get("table") or r.get("source") or r.get("type") or ""
            tenant = r.get("tenant_name") or r.get("tenant") or ""
            prop   = r.get("property_name") or r.get("property") or ""

            header_parts = [f"Score: {score:.2f}"]
            if table:  header_parts.append(f"Source: {table}")
            if tenant: header_parts.append(f"Tenant: {tenant}")
            if prop:   header_parts.append(f"Property: {prop}")

            chunks.append(f"[{' | '.join(header_parts)}]\n{text}")

        if chunks:
            if is_rag:
                results_text = (
                    f"Retrieved {len(chunks)} relevant records from knowledge base for: '{query}'\n"
                    f"{'='*60}\n\n" + "\n\n---\n".join(chunks)
                )
            else:
                results_text = (
                    f"Semantic Search Results for: '{query}'\n{'='*50}\n\n"
                    + "\n\n---\n".join(chunks)
                )
        else:
            results_text = "No semantically similar records found in the vector store."
    else:
        results_text = "No semantically similar records found in the vector store."

    logger.info("[VectorSearch] strategy=%s top_k=%s threshold=%s → %d results",
                strategy, top_k, threshold, len(results))
    return {**state, "vector_results": results, "vector_results_text": results_text}


# ── NODE 3: Generate SQL ───────────────────────────────────────────────────────

def generate_sql_node(state: AgentState, db_manager, schema_manager, **_) -> AgentState:
    llm     = _get_llm(max_tokens=800)
    # Use resolved_question (typo-corrected + memory-expanded)
    raw_q   = state["user_question"]
    user_q  = state.get("resolved_question") or raw_q
    attempt = state.get("sql_attempt", 0) + 1
    history = state.get("sql_attempt_history", [])

    schema_text     = schema_manager.get_schema_for_query(user_q)
    all_table_names = schema_manager.get_all_table_names()

    system_prompt = create_sql_generation_prompt(
        database_schema=schema_text,
        table_list=all_table_names,
    )

    schema_aware_hints = _build_schema_aware_hints(user_q, schema_manager)
    intent_hints       = create_intent_context(user_q)

    # ── Conversation memory context ──────────────────────────────────────────
    # Inject last 2 Q&A pairs so the LLM knows what entity was discussed before.
    mem_context = ""
    prior_msgs  = state.get("messages", [])
    if prior_msgs:
        pairs = []
        i = 0
        while i < len(prior_msgs) - 1 and len(pairs) < 2:
            if isinstance(prior_msgs[i], HumanMessage) and isinstance(prior_msgs[i + 1], AIMessage):
                q = prior_msgs[i].content[:120]
                a = prior_msgs[i + 1].content[:200]
                pairs.append(f"Q: {q}\nA: {a}")
                i += 2
            else:
                i += 1
        if pairs:
            mem_context = (
                "\n\n--- CONVERSATION CONTEXT (prior turns) ---\n"
                + "\n\n".join(pairs)
                + "\n--- Use only if current question references prior context ---"
            )

    all_hints = schema_aware_hints + intent_hints + mem_context

    if attempt == 1:
        user_message = user_q + all_hints
        if raw_q != user_q:
            user_message = (
                f"[Note: User typed '{raw_q}'. Corrected to: '{user_q}']\n\n"
                + user_message
            )
    else:
        err_hist = "".join(
            f"\nATTEMPT {i}:\nSQL: {p['sql']}\nError: {p['error']}\n---"
            for i, p in enumerate(history, 1)
        )
        user_message = create_sql_retry_message(user_q, err_hist)

    logger.info(
        "[GenerateSQL] attempt=%d schema_chars=%d raw='%s' resolved='%s'",
        attempt, len(schema_text), raw_q[:60], user_q[:60],
    )

    try:
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message),
        ], context="sql_gen")
    except Exception as exc:
        err_msg = f"LLM call failed after retries: {exc}"
        logger.error("[GenerateSQL] %s", err_msg)
        return {
            **state,
            "sql_attempt":         attempt,
            "sql_query":           None,
            "sql_results":         {"success": False, "error": err_msg},
            "sql_attempt_history": history + [{"sql": "(llm failed)", "error": err_msg}],
            "need_embedding": False, "embedding_params": [], "error": err_msg,
        }

    sql_query = _extract_sql_query_from_response(response.content)
    if not sql_query:
        err_msg = "SQL generation returned no extractable SQL query."
        logger.error("[GenerateSQL] %s | raw: %.200s", err_msg, response.content)
        return {
            **state,
            "sql_attempt":         attempt,
            "sql_query":           None,
            "sql_results":         {"success": False, "error": err_msg},
            "sql_attempt_history": history + [{"sql": "(parse failed)", "error": err_msg}],
            "need_embedding": False, "embedding_params": [], "error": err_msg,
        }

    is_valid, validation_error = schema_manager.validate_sql_columns(sql_query)
    if not is_valid:
        logger.warning("[GenerateSQL] Validation failed: %s", validation_error)
        return {
            **state,
            "sql_attempt":         attempt,
            "sql_query":           sql_query,
            "sql_results":         {"success": False, "error": validation_error},
            "sql_attempt_history": history + [{"sql": sql_query, "error": validation_error}],
            "need_embedding": False, "embedding_params": [], "error": validation_error,
        }

    logger.info("[GenerateSQL] ✅ attempt=%d: %.200s", attempt, sql_query)
    return {
        **state,
        "sql_attempt":      attempt,
        "sql_query":        sql_query,
        "sql_results":      None,
        "need_embedding":   False,
        "embedding_params": [],
        "error":            None,
    }


# ── NODE 4: Execute SQL ────────────────────────────────────────────────────────

def execute_sql_node(state: AgentState, db_manager, **_) -> AgentState:
    sql     = state.get("sql_query", "")
    history = state.get("sql_attempt_history", [])

    if not sql:
        return {
            **state,
            "sql_results":      {"success": False, "error": "No SQL query to execute."},
            "sql_results_text": "No SQL query was generated.",
        }

    result = db_manager.execute_query(sql)
    logger.info("[ExecuteSQL] success=%s rows=%s", result.get("success"), result.get("row_count", "N/A"))

    if not result["success"]:
        error = result.get("error", "Unknown error.")
        return {
            **state,
            "sql_results":         result,
            "sql_results_text":    f"Query failed: {error}",
            "sql_attempt_history": history + [{"sql": sql, "error": error}],
        }

    return {**state, "sql_results": result, "sql_results_text": _format_sql_results(result)}


def _compute_column_sums(rows: list, cols: list) -> Dict[str, Any]:
    """
    For numeric columns that look like counts/totals, compute the actual sum.
    Returns dict of {col_name: sum} for columns where summing makes sense.
    """
    sums = {}
    count_keywords = {
        "total", "count", "vacant", "units", "amount", "balance",
        "outstanding", "due", "incidents", "complaints", "tickets",
        "contracts", "leases", "days", "loss", "revenue", "rent",
    }
    for i, col in enumerate(cols):
        col_lower = col.lower()
        if not any(kw in col_lower for kw in count_keywords):
            continue
        try:
            vals = []
            for row in rows:
                v = row[i] if isinstance(row, (list, tuple)) else row.get(col)
                if v is not None and str(v).strip() not in ("", "NULL"):
                    vals.append(float(str(v).replace(",", "")))
            if vals and len(vals) > 1:  # only sum when multiple rows
                sums[col] = int(sum(vals)) if all(v == int(v) for v in vals) else round(sum(vals), 2)
        except (ValueError, TypeError):
            continue
    return sums


def _format_sql_results(result: Dict[str, Any]) -> str:
    rows      = result.get("rows", [])
    cols      = result.get("column_names", [])
    row_count = result.get("row_count", 0)
    if row_count == 0:
        return "Query executed successfully but returned 0 rows."
    header = " | ".join(cols)
    sep    = "-" * min(len(header) + 10, 120)
    body   = "\n".join(
        " | ".join(str(v)[:80] if v is not None else "NULL" for v in row)
        for row in rows[:50]
    )
    tail = f"\n(showing first 50 of {row_count} rows)" if row_count > 50 else ""

    # Compute and append verified column totals so LLM doesn't need to add
    col_sums = _compute_column_sums(rows, cols)
    totals_note = ""
    if col_sums:
        total_parts = [f"{col}: {val:,}" for col, val in col_sums.items()]
        totals_note = (
            f"\n\n⚠️  VERIFIED TOTALS (pre-computed, use these exactly — do NOT re-add):\n"
            + "\n".join(f"  GRAND TOTAL {p}" for p in total_parts)
        )

    return f"Results ({row_count} rows):\n{header}\n{sep}\n{body}{tail}{totals_note}"


# ── NODE 5: Synthesise Answer ──────────────────────────────────────────────────

# Dedicated RAG prompt for pure vector_only qualitative questions
_RAG_SYSTEM_PROMPT = """You are an expert Property & Lease Management analyst.

You have been given retrieved document chunks from a semantic search over the ERP database.
These chunks contain real records — tenant feedback, maintenance notes, complaints, remarks, 
contract observations, and operational data.

Your job: synthesize these chunks into a clear, insightful, business-friendly answer.

GUIDELINES:
  - Read ALL retrieved chunks carefully before answering.
  - Identify patterns, themes, and categories across the chunks.
  - Lead with the most important finding or direct answer.
  - Group similar themes together (e.g. "Maintenance issues", "Payment disputes", "Move-out reasons").
  - Give specific examples or quotes from the data where relevant (use italics or quotes).
  - Quantify where possible: "3 out of 8 records mention...", "The most common theme is..."
  - End with 1-2 concrete recommendations based on the patterns.
  - Tone: professional, analytical, actionable.
  - Length: 200-400 words. Use headers or bullet points for clarity.

CRITICAL:
  - If chunks are empty or irrelevant, say: "No relevant records were found for this query."
  - NEVER invent data not present in the retrieved chunks.
  - Do NOT say "based on semantic search context" — just answer naturally.
  - Do NOT mention FAISS, vector search, embeddings, or technical infrastructure."""


def synthesise_answer_node(state: AgentState, **_) -> AgentState:
    llm    = _get_llm(max_tokens=1000)
    raw_q  = state["user_question"]
    user_q = state.get("resolved_question") or raw_q
    strategy = state.get("strategy", "sql_only")

    sql_res       = state.get("sql_results") or {}
    sql_success   = sql_res.get("success", False)
    sql_row_count = sql_res.get("row_count", 0) if sql_success else 0

    sql_text = _truncate_sql_text(state.get("sql_results_text") or "")
    vec_limit = 60000 if strategy == "db_rag" else 4000
    vec_text = (state.get("vector_results_text") or "")[:vec_limit]

    # ── RAG path: db_rag or vector_only with actual results ────────────────────
    rag_has_content = (
        vec_text
        and "FAISS unavailable" not in vec_text
        and len(vec_text.strip()) > 80
        and not (
            "No semantically similar" in vec_text
            and len(vec_text.strip()) < 100
        )
    )

    if strategy in ("vector_only", "db_rag") and rag_has_content:
        retrieved_count = len(state.get("vector_results", []))
        rag_user_msg = f"""Question: {user_q}

Total retrieved records: {retrieved_count}
(Use this exact count in your analysis; do not invent a different total.)

Retrieved Knowledge:
{vec_text}

Please synthesize the above retrieved records into a clear, analytical answer."""

        try:
            response = llm_invoke_with_retry(llm, [
                SystemMessage(content=_RAG_SYSTEM_PROMPT),
                HumanMessage(content=rag_user_msg),
            ], context="rag_synthesise")
            answer = _sanitize_rag_answer_claims(response.content.strip(), retrieved_count)
        except Exception as exc:
            logger.error("[Synthesise-RAG] LLM failed: %s", exc)
            answer = f"I retrieved relevant records but could not format the answer due to an API issue.\n\nRaw results:\n{vec_text}"

        logger.info("[Synthesise-RAG] %d chars, %d vector results", len(answer), len(state.get("vector_results", [])))

        corrections = state.get("fuzzy_corrections", [])
        if corrections:
            parts = [f"'{c['original']}' → '{c['corrected']}'" for c in corrections]
            answer += f"\n\n_(Auto-corrected: {', '.join(parts)})_"

        return {
            **state,
            "final_answer": answer,
            "success":      True,
            "messages":     state.get("messages", []) + [
                HumanMessage(content=raw_q),
                AIMessage(content=answer),
            ],
        }

    # ── db_rag with NO vector content: run a SQL count summary instead ──────────
    if strategy == "db_rag" and not rag_has_content:
        # Try to answer qualitatively using SQL aggregates as a fallback
        llm = _get_llm(max_tokens=800)
        fallback_sql_context = (
            "Note: The vector knowledge base is not yet populated. "
            "Answering from SQL aggregate data instead. "
            "If the user asks about complaint content/themes, acknowledge you can only provide counts — "
            "not the full text analysis — until the vector index is built."
        )
        try:
            response = llm_invoke_with_retry(llm, [
                SystemMessage(content=_RAG_SYSTEM_PROMPT),
                HumanMessage(content=(
                    f"Question: {user_q}\n\n"
                    f"{fallback_sql_context}\n\n"
                    f"Based on the database schema, provide the best answer you can. "
                    f"Suggest the user ask 'How many complaints by type?' or "
                    f"'What are the top complaint categories?' for SQL-based counts."
                )),
            ], context="rag_fallback")
            answer = response.content.strip()
        except Exception:
            answer = (
                "I could not retrieve complaint text records at this time.\n\n"
                "You can ask **'How many complaints are there by type?'** or "
                "**'What are the top complaint categories?'** to get complaint counts from the database."
            )

        return {
            **state,
            "final_answer": answer,
            "success":      True,
            "messages":     state.get("messages", []) + [
                HumanMessage(content=raw_q),
                AIMessage(content=answer),
            ],
        }

    # ── SQL / Hybrid path (existing logic) ─────────────────────────────────────
    zero_row_note = ""
    if sql_success and sql_row_count == 0:
        zero_row_note = (
            "SQL returned 0 rows. Do NOT invent data. "
            "Tell the user no matching records were found."
        )
    elif not sql_success and vec_text and "unavailable" not in vec_text.lower():
        zero_row_note = "SQL failed. Answer from semantic context; note exact figures need a DB query."

    corrections = state.get("fuzzy_corrections", [])
    correction_note = ""
    if corrections:
        parts = [f"'{c['original']}' → '{c['corrected']}'" for c in corrections]
        correction_note = f"\n\n_(Auto-corrected: {', '.join(parts)})_"

    user_msg = create_final_answer_user_message(
        user_question=user_q,
        sql_results=sql_text,
        vector_results=vec_text,
        zero_row_note=zero_row_note,
        sql_query=state.get("sql_query"),
        sql_row_count=sql_row_count,
    )

    try:
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=FINAL_ANSWER_SYSTEM_PROMPT),
            HumanMessage(content=user_msg),
        ], context="synthesise")
        answer = response.content.strip() + correction_note
    except Exception as exc:
        logger.error("[Synthesise] LLM failed: %s", exc)
        answer = (
            f"I retrieved the data but could not format the answer due to a temporary API issue.\n\n"
            f"Raw results:\n{sql_text or vec_text or 'No data available.'}"
        ) + correction_note

    logger.info("[Synthesise] %d chars, sql_rows=%d", len(answer), sql_row_count)

    return {
        **state,
        "final_answer": answer,
        "success":      True,
        "messages":     state.get("messages", []) + [
            HumanMessage(content=raw_q),
            AIMessage(content=answer),
        ],
    }


# ── NODE 6: Conversational ─────────────────────────────────────────────────────

def conversational_node(state: AgentState, **_) -> AgentState:
    llm    = _get_llm(max_tokens=800)
    raw_q  = state["user_question"]
    user_q = state.get("resolved_question") or raw_q
    hist   = state.get("messages", [])[-_MAX_HISTORY_MESSAGES:]

    try:
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=CONVERSATIONAL_SYSTEM_PROMPT),
            *hist,
            HumanMessage(content=user_q),
        ], context="conversational")
        answer = response.content.strip()
    except Exception as exc:
        answer = f"I'm temporarily unavailable due to API rate limits. Please try again in a moment. ({exc})"

    return {
        **state,
        "final_answer": answer,
        "success":      True,
        "messages":     state.get("messages", []) + [
            HumanMessage(content=raw_q),
            AIMessage(content=answer),
        ],
    }


# ── NODE 7: Error ──────────────────────────────────────────────────────────────

def error_node(state: AgentState, **_) -> AgentState:
    error   = state.get("error", "An unknown error occurred.")
    history = state.get("sql_attempt_history", [])
    user_q  = state["user_question"]

    answer = (
        f"Unable to retrieve that information due to a security or permission issue.\n\n"
        f"Reason: {error}\n\n"
        f"Please rephrase your question or contact your administrator."
        + (f"\n\n({len(history)} SQL attempt(s) made.)" if history else "")
    )

    return {
        **state,
        "final_answer": answer,
        "success":      False,
        "messages":     state.get("messages", []) + [
            HumanMessage(content=user_q),
            AIMessage(content=answer),
        ],
    }


# ── Edge Condition ─────────────────────────────────────────────────────────────

def should_retry_sql(state: AgentState) -> str:
    res     = state.get("sql_results", {})
    attempt = state.get("sql_attempt", 0)
    max_r   = state.get("max_sql_retries", MAX_SQL_RETRIES)

    if res.get("success"):
        return "synthesise"
    if res.get("is_security_issue"):
        return "error"
    if attempt < max_r:
        logger.info("[ShouldRetry] attempt %d/%d → retry", attempt, max_r)
        return "retry_sql"

    logger.warning("[ShouldRetry] max retries → synthesise fallback")
    return "synthesise"
