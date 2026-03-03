# #!/usr/bin/env python3
# """
# FastAPI Application – Property Management Agentic RAG Chatbot
# MySQL 5.7 + FAISS + SchemaManager (token-safe, schema-verified)

# Key fixes over v1:
#   - Session history key fixed: reads 'messages' not 'messages_updated'
#   - Session history correctly merges old + new messages
#   - /debug/schema now also returns column names per table
#   - /vector-search endpoint uses configurable top_k and threshold
#   - Startup logs include connection test result
# """

# import os
# import uuid
# import logging
# from contextlib import asynccontextmanager
# from typing import Any, Dict, List, Optional

# from fastapi import FastAPI, HTTPException, Query
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel, Field

# from config import API_HOST, API_PORT, CORS_ORIGINS, LOG_LEVEL, LLM_MODEL, VECTOR_DEFAULT_THRESHOLD
# from database import DatabaseManager
# from vector_store import VectorStore
# from schema_manager import SchemaManager
# from fuzzy_matcher import FuzzyMatcher
# from agent_graph import build_agent_graph, run_agent

# logging.basicConfig(
#     level=getattr(logging, LOG_LEVEL),
#     format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
# )
# logger = logging.getLogger(__name__)

# # ── Singletons ──────────────────────────────────────────────────────────────────
# _db_manager:     Optional[DatabaseManager] = None
# _vector_store:   Optional[VectorStore]     = None
# _schema_manager: Optional[SchemaManager]   = None
# _fuzzy_matcher:  Optional[FuzzyMatcher]    = None
# _agent_app                                 = None

# # In-memory session store: {session_id: [LangChain message objects]}
# _session_store: Dict[str, List[Any]] = {}


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     global _db_manager, _vector_store, _schema_manager, _agent_app

#     logger.info("=" * 60)
#     logger.info("Starting Property Management Chatbot …")
#     logger.info("=" * 60)

#     # Database
#     _db_manager = DatabaseManager()
#     ok, msg = _db_manager.test_connection()
#     if ok:
#         tables = _db_manager.get_all_tables()
#         logger.info("✅ MySQL connected – tables: %s", tables)
#     else:
#         logger.error("❌ MySQL connection FAILED: %s", msg)

#     # Vector store
#     _vector_store = VectorStore()
#     logger.info(
#         "✅ FAISS ready – vectors: %d  available: %s",
#         _vector_store.total_vectors, _vector_store.is_available,
#     )

#     # Schema manager — loads from disk cache if fresh, else queries MySQL
#     _schema_manager = SchemaManager(_db_manager)
#     all_tables = _schema_manager.get_all_table_names()
#     cache_info = _schema_manager.get_cache_info()

#     logger.info(
#         "✅ Schema ready – %d tables | source: %s | cache age: %s h | path: %s",
#         len(all_tables),
#         "DISK CACHE" if cache_info["cache_exists"] and cache_info["cache_age_hours"] is not None
#                      else "MYSQL (fresh)",
#         cache_info.get("cache_age_hours", "N/A"),
#         cache_info["cache_path"],
#     )
#     logger.info("Tables: %s", all_tables)

#     # Log column index for debugging
#     col_index = _schema_manager.get_column_index()
#     for t, cols in col_index.items():
#         logger.info("[Schema] Table '%s' columns: %s", t, sorted(cols))

#     logger.info("=" * 60)
#     logger.info("FULL DATABASE SCHEMA:\n%s", _schema_manager.get_full_schema())
#     logger.info("=" * 60)

#     # Fuzzy matcher — loads real property/tenant/contract names from MySQL
#     _fuzzy_matcher = FuzzyMatcher(_db_manager)
#     counts = _fuzzy_matcher.entity_counts()
#     logger.info(
#         "✅ FuzzyMatcher ready – properties: %d, tenants: %d, contracts: %d",
#         counts.get("property", 0), counts.get("tenant", 0), counts.get("contract", 0),
#     )

#     # Agent
#     _agent_app = build_agent_graph(_db_manager, _vector_store, _schema_manager, _fuzzy_matcher)
#     logger.info("✅ LangGraph agent ready")

#     yield

#     _db_manager.close()
#     logger.info("Shutdown complete.")


# # ── FastAPI app ─────────────────────────────────────────────────────────────────

# app = FastAPI(
#     title="Property Management Agentic RAG Chatbot",
#     version="3.0.0",
#     description=(
#         "MySQL 5.7 + FAISS + LangGraph. "
#         "Tri-path architecture: SQL (quantitative), Vector (qualitative), Hybrid (causal). "
#         "See /debug/schema to verify table/column names loaded from your actual database."
#     ),
#     lifespan=lifespan,
# )

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=CORS_ORIGINS,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# # ── Request / Response Models ───────────────────────────────────────────────────

# class ChatRequest(BaseModel):
#     question:   str           = Field(..., min_length=1, max_length=2000)
#     session_id: Optional[str] = None


# class ChatResponse(BaseModel):
#     answer:               str
#     session_id:           str
#     strategy:             Optional[str] = None
#     sql_query:            Optional[str] = None
#     sql_row_count:        Optional[int] = None
#     vector_results_count: Optional[int] = None
#     sql_attempts:         Optional[int] = None
#     success:              bool
#     # Preprocessing info
#     resolved_question:    Optional[str] = None   # after fuzzy correction + memory resolution
#     fuzzy_corrections:    Optional[List[Dict[str, Any]]] = None   # what was auto-corrected
#     is_out_of_scope:      bool = False


# class HealthResponse(BaseModel):
#     status:           str
#     database:         str
#     tables:           List[str]
#     vector_available: bool
#     vector_vectors:   int
#     llm_model:        str


# class ColumnInfo(BaseModel):
#     name:    str
#     columns: List[str]


# class TableInfo(BaseModel):
#     name:      str
#     row_count: int
#     columns:   List[str]


# class SchemaDebugResponse(BaseModel):
#     total_tables:     int
#     tables:           List[TableInfo]
#     full_schema:      str
#     question:         Optional[str] = None
#     schema_for_query: Optional[str] = None


# class SQLValidateRequest(BaseModel):
#     sql: str


# class SQLValidateResponse(BaseModel):
#     is_valid: bool
#     error:    Optional[str] = None


# # ── Endpoints ───────────────────────────────────────────────────────────────────

# @app.get("/health", response_model=HealthResponse, tags=["System"])
# async def health():
#     """Check database connectivity, FAISS status, and schema."""
#     db_status = "connected"
#     tables: List[str] = []
#     try:
#         ok, msg = _db_manager.test_connection()
#         db_status = "connected" if ok else f"error: {msg}"
#         tables = _db_manager.get_all_tables()
#     except Exception as exc:
#         db_status = f"error: {exc}"

#     return HealthResponse(
#         status="ok" if db_status == "connected" else "degraded",
#         database=db_status,
#         tables=tables,
#         vector_available=_vector_store.is_available if _vector_store else False,
#         vector_vectors=_vector_store.total_vectors if _vector_store else 0,
#         llm_model=LLM_MODEL,
#     )


# @app.post("/chat", response_model=ChatResponse, tags=["Chat"])
# async def chat(request: ChatRequest):
#     """
#     Main chat endpoint.

#     The agent will:
#     1. Route the question (SQL / vector / hybrid / conversational)
#     2. For causal questions, automatically route to hybrid (both SQL + FAISS)
#     3. Select relevant tables and generate MySQL 5.7 SQL using only real column names
#     4. Pre-validate SQL before execution (catches hallucinated table/column names)
#     5. Execute with retry on errors; fall back to FAISS if SQL fails
#     6. Return a business-friendly answer
#     """
#     session_id = request.session_id or str(uuid.uuid4())
#     history    = _session_store.get(session_id, [])

#     result = run_agent(
#         app=_agent_app,
#         user_question=request.question,
#         session_id=session_id,
#         conversation_history=history,
#     )

#     # Extract only the NEW messages from this turn (last 2: HumanMessage + AIMessage).
#     # result["messages"] = history_passed_in + [HumanMessage(q), AIMessage(answer)]
#     # We only want to append the new pair to avoid duplicating history.
#     all_msgs  = result.get("messages") or []
#     new_turn  = all_msgs[len(history):]   # only what was added this turn
#     if not new_turn:
#         # Fallback: nothing returned, construct from question + answer
#         from langchain_core.messages import HumanMessage as HM, AIMessage as AM
#         answer = result.get("final_answer", "")
#         if answer:
#             new_turn = [HM(content=request.question), AM(content=answer)]

#     # Keep last 20 messages (10 turns) to stay within token budget
#     _session_store[session_id] = (history + new_turn)[-20:]

#     sql_row_count = None
#     if result.get("sql_results") and result["sql_results"].get("success"):
#         sql_row_count = result["sql_results"].get("row_count")

#     vec_count = len(result.get("vector_results") or [])

#     return ChatResponse(
#         answer=result.get("final_answer", "Sorry, I could not generate an answer."),
#         session_id=session_id,
#         strategy=result.get("strategy"),
#         sql_query=result.get("sql_query"),
#         sql_row_count=sql_row_count,
#         vector_results_count=vec_count if vec_count > 0 else None,
#         sql_attempts=result.get("sql_attempts"),
#         success=result.get("success", False),
#         resolved_question=result.get("resolved_question"),
#         fuzzy_corrections=result.get("fuzzy_corrections") or None,
#         is_out_of_scope=result.get("is_out_of_scope", False),
#     )


# @app.get("/debug/schema", response_model=SchemaDebugResponse, tags=["Debug"])
# async def debug_schema(question: Optional[str] = Query(default=None)):
#     """
#     KEY DEBUG ENDPOINT — shows exact schema text sent to the LLM.

#     Pass ?question=... to see which tables are selected for that query.
#     Also returns column names per table for verification.

#     Example:
#       GET /debug/schema?question=how many properties are in database
#     """
#     debug = _schema_manager.get_debug_info(question)
#     return SchemaDebugResponse(
#         total_tables=debug["total_tables"],
#         tables=[
#             TableInfo(
#                 name=t["name"],
#                 row_count=t["row_count"],
#                 columns=t.get("columns", []),
#             )
#             for t in debug["tables"]
#         ],
#         full_schema=_schema_manager.get_full_schema(),
#         question=question,
#         schema_for_query=debug.get("schema_for_query"),
#     )


# @app.post("/debug/schema", tags=["Debug"])
# async def debug_schema_post(question: str):
#     """POST version of schema debug (for longer questions)."""
#     return _schema_manager.get_debug_info(question)


# @app.post("/debug/validate-sql", response_model=SQLValidateResponse, tags=["Debug"])
# async def validate_sql(request: SQLValidateRequest):
#     """
#     Validate a SQL string against the real schema without executing it.
#     Use this to test whether SQL would pass pre-execution validation.

#     Returns is_valid=True if the SQL references only real tables, or
#     is_valid=False with an error message describing what's wrong.
#     """
#     is_valid, error = _schema_manager.validate_sql_columns(request.sql)
#     return SQLValidateResponse(is_valid=is_valid, error=error if not is_valid else None)


# @app.get("/fuzzy/status", tags=["Fuzzy Matcher"])
# async def fuzzy_status():
#     """Show how many entity names the fuzzy matcher has loaded."""
#     if not _fuzzy_matcher:
#         return {"status": "not_initialized"}
#     counts = _fuzzy_matcher.entity_counts()
#     return {
#         "status":   "ready" if _fuzzy_matcher.is_ready() else "empty",
#         "entities": counts,
#         "min_score": float(os.getenv("FUZZY_MIN_SCORE", "0.55")),
#     }


# @app.post("/fuzzy/refresh", tags=["Fuzzy Matcher"])
# async def fuzzy_refresh():
#     """Reload property/tenant/contract names from MySQL (call after new data is added)."""
#     _fuzzy_matcher.refresh()
#     counts = _fuzzy_matcher.entity_counts()
#     return {"message": "Fuzzy matcher refreshed.", "entities": counts}


# @app.post("/fuzzy/test", tags=["Fuzzy Matcher"])
# async def fuzzy_test(question: str):
#     """
#     Test fuzzy correction on a question without running the full agent.
#     Useful for verifying that typos are being corrected correctly.
#     Example: POST /fuzzy/test?question=how many vaccany in see1
#     """
#     if not _fuzzy_matcher or not _fuzzy_matcher.is_ready():
#         return {"error": "Fuzzy matcher not ready."}
#     corrected, corrections = _fuzzy_matcher.correct_question(question)
#     return {
#         "original":    question,
#         "corrected":   corrected,
#         "corrections": corrections,
#         "changed":     corrected != question,
#     }


# @app.post("/schema/refresh", tags=["Schema"])
# async def refresh_schema():
#     """
#     Force a full schema reload from MySQL and save a new disk cache.
#     Call this after adding or altering tables.
#     The new cache is immediately saved to schema_cache.pkl.
#     """
#     _schema_manager.refresh()
#     return {
#         "message":    "Schema refreshed and saved to disk cache.",
#         "tables":     _schema_manager.get_all_table_names(),
#         "cache_info": _schema_manager.get_cache_info(),
#     }


# @app.get("/schema/cache", tags=["Schema"])
# async def get_cache_info():
#     """
#     Show the current disk cache status:
#     location, age, size, TTL, and how many tables are cached.
#     """
#     return _schema_manager.get_cache_info()


# @app.delete("/schema/cache", tags=["Schema"])
# async def delete_cache():
#     """
#     Delete the disk cache file.
#     On next restart the schema will be reloaded from MySQL.
#     Does NOT affect the currently running in-memory cache.
#     """
#     _schema_manager.delete_cache()
#     return {"message": "Disk cache deleted. MySQL will be queried on next restart."}


# @app.get("/sessions/{session_id}", tags=["Sessions"])
# async def get_session(session_id: str):
#     """Return conversation history for a session."""
#     history = _session_store.get(session_id, [])
#     messages = [
#         {
#             "role":    "user" if getattr(m, "type", "") == "human" else "assistant",
#             "content": m.content,
#         }
#         for m in history
#         if hasattr(m, "content")
#     ]
#     return {
#         "session_id":    session_id,
#         "message_count": len(messages),
#         "messages":      messages,
#     }


# @app.delete("/sessions/{session_id}", tags=["Sessions"])
# async def clear_session(session_id: str):
#     """Clear conversation history for a session."""
#     _session_store.pop(session_id, None)
#     return {"message": f"Session '{session_id}' cleared."}


# @app.get("/sessions", tags=["Sessions"])
# async def list_sessions():
#     """List all active session IDs and their message counts."""
#     return {
#         "sessions": [
#             {"session_id": sid, "message_count": len(msgs)}
#             for sid, msgs in _session_store.items()
#         ]
#     }


# @app.post("/vector-search", tags=["Vector Store"])
# async def vector_search(
#     query:           str   = Query(..., description="Natural language search query"),
#     top_k:           int   = Query(default=5,  ge=1, le=20),
#     score_threshold: float = Query(default=VECTOR_DEFAULT_THRESHOLD, ge=0.0, le=1.0),
# ):
#     """
#     Direct FAISS semantic search (bypasses full agent pipeline).
#     Useful for testing what the vector store returns for a given query.
#     Uses adaptive threshold expansion automatically.
#     """
#     if not _vector_store or not _vector_store.is_available:
#         raise HTTPException(status_code=503, detail="Vector store not available.")

#     results = _vector_store.search(query, top_k=top_k, score_threshold=score_threshold)
#     return {
#         "query":           query,
#         "score_threshold": score_threshold,
#         "results":         results,
#         "count":           len(results),
#     }


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False, workers=1)

#!/usr/bin/env python3
"""
FastAPI Application – Property Management Agentic RAG Chatbot
MySQL 5.7 + FAISS + SchemaManager (token-safe, schema-verified)

Key fixes over v1:
  - Session history key fixed: reads 'messages' not 'messages_updated'
  - Session history correctly merges old + new messages
  - /debug/schema now also returns column names per table
  - /vector-search endpoint uses configurable top_k and threshold
  - Startup logs include connection test result
"""

import os
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config import API_HOST, API_PORT, CORS_ORIGINS, LOG_LEVEL, LLM_MODEL, VECTOR_DEFAULT_THRESHOLD
from database import DatabaseManager
from vector_store import VectorStore
from schema_manager import SchemaManager
from fuzzy_matcher import FuzzyMatcher
from agent_graph import build_agent_graph, run_agent

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)

# ── Singletons ──────────────────────────────────────────────────────────────────
_db_manager:     Optional[DatabaseManager] = None
_vector_store:   Optional[VectorStore]     = None
_schema_manager: Optional[SchemaManager]   = None
_fuzzy_matcher:  Optional[FuzzyMatcher]    = None
_agent_app                                 = None

# In-memory session store: {session_id: [LangChain message objects]}
_session_store: Dict[str, List[Any]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db_manager, _vector_store, _schema_manager, _agent_app

    logger.info("=" * 60)
    logger.info("Starting Property Management Chatbot …")
    logger.info("=" * 60)

    # Database
    _db_manager = DatabaseManager()
    ok, msg = _db_manager.test_connection()
    if ok:
        tables = _db_manager.get_all_tables()
        logger.info("✅ MySQL connected – tables: %s", tables)
    else:
        logger.error("❌ MySQL connection FAILED: %s", msg)

    # Vector store
    _vector_store = VectorStore()
    logger.info(
        "✅ FAISS ready – vectors: %d  available: %s",
        _vector_store.total_vectors, _vector_store.is_available,
    )

    # Schema manager — loads from disk cache if fresh, else queries MySQL
    _schema_manager = SchemaManager(_db_manager)
    all_tables = _schema_manager.get_all_table_names()
    cache_info = _schema_manager.get_cache_info()

    logger.info(
        "✅ Schema ready – %d tables | source: %s | cache age: %s h | path: %s",
        len(all_tables),
        "DISK CACHE" if cache_info["cache_exists"] and cache_info["cache_age_hours"] is not None
                     else "MYSQL (fresh)",
        cache_info.get("cache_age_hours", "N/A"),
        cache_info["cache_path"],
    )
    logger.info("Tables: %s", all_tables)

    # Log column index for debugging
    col_index = _schema_manager.get_column_index()
    for t, cols in col_index.items():
        logger.info("[Schema] Table '%s' columns: %s", t, sorted(cols))

    logger.info("=" * 60)
    logger.info("FULL DATABASE SCHEMA:\n%s", _schema_manager.get_full_schema())
    logger.info("=" * 60)

    # Fuzzy matcher — loads real property/tenant/contract names from MySQL
    _fuzzy_matcher = FuzzyMatcher(_db_manager)
    counts = _fuzzy_matcher.entity_counts()
    logger.info(
        "✅ FuzzyMatcher ready – properties: %d, tenants: %d, contracts: %d",
        counts.get("property", 0), counts.get("tenant", 0), counts.get("contract", 0),
    )

    # Agent
    _agent_app = build_agent_graph(_db_manager, _vector_store, _schema_manager, _fuzzy_matcher)
    logger.info("✅ LangGraph agent ready")

    yield

    _db_manager.close()
    logger.info("Shutdown complete.")


# ── FastAPI app ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Property Management Agentic RAG Chatbot",
    version="3.0.0",
    description=(
        "MySQL 5.7 + FAISS + LangGraph. "
        "Tri-path architecture: SQL (quantitative), Vector (qualitative), Hybrid (causal). "
        "See /debug/schema to verify table/column names loaded from your actual database."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response Models ───────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question:   str           = Field(..., min_length=1, max_length=2000)
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    answer:               str
    session_id:           str
    strategy:             Optional[str] = None
    sql_query:            Optional[str] = None
    sql_row_count:        Optional[int] = None
    vector_results_count: Optional[int] = None
    sql_attempts:         Optional[int] = None
    success:              bool
    # Preprocessing info
    resolved_question:    Optional[str] = None   # after fuzzy correction + memory resolution
    fuzzy_corrections:    Optional[List[Dict[str, Any]]] = None   # what was auto-corrected
    is_out_of_scope:      bool = False


class HealthResponse(BaseModel):
    status:           str
    database:         str
    tables:           List[str]
    vector_available: bool
    vector_vectors:   int
    llm_model:        str


class ColumnInfo(BaseModel):
    name:    str
    columns: List[str]


class TableInfo(BaseModel):
    name:      str
    row_count: int
    columns:   List[str]


class SchemaDebugResponse(BaseModel):
    total_tables:     int
    tables:           List[TableInfo]
    full_schema:      str
    question:         Optional[str] = None
    schema_for_query: Optional[str] = None


class SQLValidateRequest(BaseModel):
    sql: str


class SQLValidateResponse(BaseModel):
    is_valid: bool
    error:    Optional[str] = None


# ── Endpoints ───────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    """Check database connectivity, FAISS status, and schema."""
    db_status = "connected"
    tables: List[str] = []
    try:
        ok, msg = _db_manager.test_connection()
        db_status = "connected" if ok else f"error: {msg}"
        tables = _db_manager.get_all_tables()
    except Exception as exc:
        db_status = f"error: {exc}"

    return HealthResponse(
        status="ok" if db_status == "connected" else "degraded",
        database=db_status,
        tables=tables,
        vector_available=_vector_store.is_available if _vector_store else False,
        vector_vectors=_vector_store.total_vectors if _vector_store else 0,
        llm_model=LLM_MODEL,
    )


@app.post("/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(request: ChatRequest):
    """
    Main chat endpoint.

    The agent will:
    1. Route the question (SQL / vector / hybrid / conversational)
    2. For causal questions, automatically route to hybrid (both SQL + FAISS)
    3. Select relevant tables and generate MySQL 5.7 SQL using only real column names
    4. Pre-validate SQL before execution (catches hallucinated table/column names)
    5. Execute with retry on errors; fall back to FAISS if SQL fails
    6. Return a business-friendly answer
    """
    session_id = request.session_id or str(uuid.uuid4())
    history    = _session_store.get(session_id, [])

    result = run_agent(
        app=_agent_app,
        user_question=request.question,
        session_id=session_id,
        conversation_history=history,
    )

    # Extract only the NEW messages from this turn (last 2: HumanMessage + AIMessage).
    # result["messages"] = history_passed_in + [HumanMessage(q), AIMessage(answer)]
    # We only want to append the new pair to avoid duplicating history.
    all_msgs  = result.get("messages") or []
    new_turn  = all_msgs[len(history):]   # only what was added this turn
    if not new_turn:
        # Fallback: nothing returned, construct from question + answer
        from langchain_core.messages import HumanMessage as HM, AIMessage as AM
        answer = result.get("final_answer", "")
        if answer:
            new_turn = [HM(content=request.question), AM(content=answer)]

    # Keep last 20 messages (10 turns) to stay within token budget
    _session_store[session_id] = (history + new_turn)[-20:]

    sql_row_count = None
    if result.get("sql_results") and result["sql_results"].get("success"):
        sql_row_count = result["sql_results"].get("row_count")

    vec_count = len(result.get("vector_results") or [])

    return ChatResponse(
        answer=result.get("final_answer", "Sorry, I could not generate an answer."),
        session_id=session_id,
        strategy=result.get("strategy"),
        sql_query=result.get("sql_query"),
        sql_row_count=sql_row_count,
        vector_results_count=vec_count if vec_count > 0 else None,
        sql_attempts=result.get("sql_attempts"),
        success=result.get("success", False),
        resolved_question=result.get("resolved_question"),
        fuzzy_corrections=result.get("fuzzy_corrections") or None,
        is_out_of_scope=result.get("is_out_of_scope", False),
    )


@app.get("/debug/schema", response_model=SchemaDebugResponse, tags=["Debug"])
async def debug_schema(question: Optional[str] = Query(default=None)):
    """
    KEY DEBUG ENDPOINT — shows exact schema text sent to the LLM.

    Pass ?question=... to see which tables are selected for that query.
    Also returns column names per table for verification.

    Example:
      GET /debug/schema?question=how many properties are in database
    """
    debug = _schema_manager.get_debug_info(question)
    return SchemaDebugResponse(
        total_tables=debug["total_tables"],
        tables=[
            TableInfo(
                name=t["name"],
                row_count=t["row_count"],
                columns=t.get("columns", []),
            )
            for t in debug["tables"]
        ],
        full_schema=_schema_manager.get_full_schema(),
        question=question,
        schema_for_query=debug.get("schema_for_query"),
    )


@app.post("/debug/schema", tags=["Debug"])
async def debug_schema_post(question: str):
    """POST version of schema debug (for longer questions)."""
    return _schema_manager.get_debug_info(question)


@app.post("/debug/validate-sql", response_model=SQLValidateResponse, tags=["Debug"])
async def validate_sql(request: SQLValidateRequest):
    """
    Validate a SQL string against the real schema without executing it.
    Use this to test whether SQL would pass pre-execution validation.

    Returns is_valid=True if the SQL references only real tables, or
    is_valid=False with an error message describing what's wrong.
    """
    is_valid, error = _schema_manager.validate_sql_columns(request.sql)
    return SQLValidateResponse(is_valid=is_valid, error=error if not is_valid else None)


@app.get("/fuzzy/status", tags=["Fuzzy Matcher"])
async def fuzzy_status():
    """Show how many entity names the fuzzy matcher has loaded."""
    if not _fuzzy_matcher:
        return {"status": "not_initialized"}
    counts = _fuzzy_matcher.entity_counts()
    return {
        "status":   "ready" if _fuzzy_matcher.is_ready() else "empty",
        "entities": counts,
        "min_score": float(os.getenv("FUZZY_MIN_SCORE", "0.55")),
    }


@app.post("/fuzzy/refresh", tags=["Fuzzy Matcher"])
async def fuzzy_refresh():
    """Reload property/tenant/contract names from MySQL (call after new data is added)."""
    _fuzzy_matcher.refresh()
    counts = _fuzzy_matcher.entity_counts()
    return {"message": "Fuzzy matcher refreshed.", "entities": counts}


@app.post("/fuzzy/test", tags=["Fuzzy Matcher"])
async def fuzzy_test(question: str):
    """
    Test fuzzy correction on a question without running the full agent.
    Useful for verifying that typos are being corrected correctly.
    Example: POST /fuzzy/test?question=how many vaccany in see1
    """
    if not _fuzzy_matcher or not _fuzzy_matcher.is_ready():
        return {"error": "Fuzzy matcher not ready."}
    corrected, corrections = _fuzzy_matcher.correct_question(question)
    return {
        "original":    question,
        "corrected":   corrected,
        "corrections": corrections,
        "changed":     corrected != question,
    }


@app.post("/schema/refresh", tags=["Schema"])
async def refresh_schema():
    """
    Force a full schema reload from MySQL and save a new disk cache.
    Call this after adding or altering tables.
    The new cache is immediately saved to schema_cache.pkl.
    """
    _schema_manager.refresh()
    return {
        "message":    "Schema refreshed and saved to disk cache.",
        "tables":     _schema_manager.get_all_table_names(),
        "cache_info": _schema_manager.get_cache_info(),
    }


@app.get("/schema/cache", tags=["Schema"])
async def get_cache_info():
    """
    Show the current disk cache status:
    location, age, size, TTL, and how many tables are cached.
    """
    return _schema_manager.get_cache_info()


@app.delete("/schema/cache", tags=["Schema"])
async def delete_cache():
    """
    Delete the disk cache file.
    On next restart the schema will be reloaded from MySQL.
    Does NOT affect the currently running in-memory cache.
    """
    _schema_manager.delete_cache()
    return {"message": "Disk cache deleted. MySQL will be queried on next restart."}


@app.get("/sessions/{session_id}", tags=["Sessions"])
async def get_session(session_id: str):
    """Return conversation history for a session."""
    history = _session_store.get(session_id, [])
    messages = [
        {
            "role":    "user" if getattr(m, "type", "") == "human" else "assistant",
            "content": m.content,
        }
        for m in history
        if hasattr(m, "content")
    ]
    return {
        "session_id":    session_id,
        "message_count": len(messages),
        "messages":      messages,
    }


@app.delete("/sessions/{session_id}", tags=["Sessions"])
async def clear_session(session_id: str):
    """Clear conversation history for a session."""
    _session_store.pop(session_id, None)
    return {"message": f"Session '{session_id}' cleared."}


@app.get("/sessions", tags=["Sessions"])
async def list_sessions():
    """List all active session IDs and their message counts."""
    return {
        "sessions": [
            {"session_id": sid, "message_count": len(msgs)}
            for sid, msgs in _session_store.items()
        ]
    }


@app.post("/vector-search", tags=["Vector Store"])
async def vector_search(
    query:           str   = Query(..., description="Natural language search query"),
    top_k:           int   = Query(default=5,  ge=1, le=20),
    score_threshold: float = Query(default=VECTOR_DEFAULT_THRESHOLD, ge=0.0, le=1.0),
):
    """
    Direct FAISS semantic search (bypasses full agent pipeline).
    Useful for testing what the vector store returns for a given query.
    Uses adaptive threshold expansion automatically.
    """
    if not _vector_store or not _vector_store.is_available:
        raise HTTPException(status_code=503, detail="Vector store not available.")

    results = _vector_store.search(query, top_k=top_k, score_threshold=score_threshold)
    return {
        "query":           query,
        "score_threshold": score_threshold,
        "results":         results,
        "count":           len(results),
    }


@app.get("/debug/rag-test", tags=["Debug"])
async def debug_rag_test():
    """
    Diagnose the RAG complaint pipeline.
    Runs the exact SQL queries used by db_rag_node and shows:
      - Row count returned
      - Column names from cursor
      - First 3 sample rows
      - Any error messages
    Use this to confirm data is actually flowing before asking qualitative questions.
    """
    if not _db_manager:
        raise HTTPException(status_code=503, detail="DB not initialised")

    from agent_nodes import _COMPLAINT_TEXT_QUERIES

    report = {}

    for label, sql in _COMPLAINT_TEXT_QUERIES:
        entry: Dict[str, Any] = {"sql_preview": sql.strip()[:300] + "..."}
        try:
            result = _db_manager.execute_query(sql.strip())
            entry["success"]      = result.get("success")
            entry["error"]        = result.get("error")
            entry["row_count"]    = result.get("row_count", 0)
            entry["column_names"] = result.get("column_names", [])

            rows = result.get("rows", [])
            cols = result.get("column_names", [])

            # Show first 3 rows parsed the same way db_rag_node does
            samples = []
            valid   = 0
            for row in rows:
                row_dict = row if isinstance(row, dict) else dict(zip(cols, row))
                text = str(row_dict.get("COMPLAINT_TEXT") or "").strip()
                if text and len(text) >= 8:
                    valid += 1
                    if len(samples) < 3:
                        samples.append({
                            "TENANT_NAME":    row_dict.get("TENANT_NAME", ""),
                            "PROPERTY_NAME":  row_dict.get("PROPERTY_NAME", ""),
                            "INCIDENT_TYPE":  row_dict.get("INCIDENT_TYPE", ""),
                            "STATUS":         row_dict.get("STATUS", ""),
                            "COMPLAINT_TEXT": text[:200] + ("..." if len(text) > 200 else ""),
                        })

            entry["valid_text_rows"] = valid
            entry["sample_rows"]     = samples

            # Extra: check if column_names is empty (zip would fail)
            if not cols and rows:
                entry["warning"] = "column_names is EMPTY — zip(cols, row) produces empty dict — COMPLAINT_TEXT will always be missing!"

        except Exception as exc:
            entry["success"] = False
            entry["error"]   = str(exc)

        report[label] = entry

    # Also test a minimal direct query on TERP_MAINT_INCIDENTS
    try:
        simple = _db_manager.execute_query(
            "SELECT COUNT(*) AS TOTAL, "
            "SUM(CASE WHEN COMPLAINT_DESCRIPTION IS NOT NULL AND COMPLAINT_DESCRIPTION != '' THEN 1 ELSE 0 END) AS HAS_TEXT "
            "FROM TERP_MAINT_INCIDENTS"
        )
        rows = simple.get("rows", [])
        cols = simple.get("column_names", [])
        if rows:
            r = rows[0] if isinstance(rows[0], dict) else dict(zip(cols, rows[0]))
            report["__direct_count"] = {
                "total_rows":     r.get("TOTAL", "?"),
                "rows_with_text": r.get("HAS_TEXT", "?"),
            }
    except Exception as e:
        report["__direct_count"] = {"error": str(e)}

    # Check which tables actually exist
    try:
        tables = _db_manager.get_all_tables()
        needed = ["TERP_MAINT_INCIDENTS", "TERP_LS_TENANTS", "TERP_LS_PROPERTY",
                  "TERP_LS_INCIDENT_TYPE", "TERP_LS_COMPLAINT_CATEGORY",
                  "TERP_LS_PROPERTY_UNIT"]
        report["__table_existence"] = {t: (t in tables) for t in needed}
    except Exception as e:
        report["__table_existence"] = {"error": str(e)}

    return report


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False, workers=1)