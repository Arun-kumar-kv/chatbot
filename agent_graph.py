# # # #!/usr/bin/env python3
# # # """
# # # LangGraph Agent Graph – Property Management Agentic RAG Chatbot
# # # MySQL 5.7 + FAISS + SchemaManager
# # # """

# # # import logging
# # # import functools
# # # from typing import Any, Dict, List, Optional

# # # from langgraph.graph import StateGraph, START, END
# # # from langgraph.checkpoint.memory import MemorySaver

# # # from agent_state import AgentState
# # # from agent_nodes import (
# # #     route_query_node,
# # #     vector_search_node,
# # #     generate_sql_node,
# # #     execute_sql_node,
# # #     synthesise_answer_node,
# # #     conversational_node,
# # #     error_node,
# # #     should_retry_sql,
# # # )

# # # logger = logging.getLogger(__name__)


# # # def build_agent_graph(db_manager, vector_store, schema_manager):
# # #     """
# # #     Compile the LangGraph StateGraph.

# # #     Args:
# # #         db_manager:     DatabaseManager  (MySQL 5.7)
# # #         vector_store:   VectorStore      (FAISS)
# # #         schema_manager: SchemaManager    (compact schema, dynamic table selection, SQL validation)

# # #     Graph flow:
# # #         START → route_query
# # #             → conversational → END
# # #             → vector_search → (hybrid → generate_sql | vector_only → synthesise) → END
# # #             → generate_sql → execute_sql → (retry → generate_sql | synthesise | error) → END
# # #     """

# # #     # Bind dependencies via functools.partial so nodes are stateless callables
# # #     _route    = functools.partial(
# # #         route_query_node,
# # #         db_manager=db_manager,
# # #         vector_store=vector_store,
# # #         schema_manager=schema_manager,
# # #     )
# # #     _vector   = functools.partial(vector_search_node,  vector_store=vector_store)
# # #     _gen_sql  = functools.partial(
# # #         generate_sql_node,
# # #         db_manager=db_manager,
# # #         schema_manager=schema_manager,
# # #     )
# # #     _exec_sql = functools.partial(execute_sql_node, db_manager=db_manager)

# # #     graph = StateGraph(AgentState)

# # #     # Register nodes
# # #     graph.add_node("route_query",    _route)
# # #     graph.add_node("vector_search",  _vector)
# # #     graph.add_node("generate_sql",   _gen_sql)
# # #     graph.add_node("execute_sql",    _exec_sql)
# # #     graph.add_node("synthesise",     synthesise_answer_node)
# # #     graph.add_node("conversational", conversational_node)
# # #     graph.add_node("error",          error_node)

# # #     # Entry edge
# # #     graph.add_edge(START, "route_query")

# # #     # After routing
# # #     graph.add_conditional_edges(
# # #         "route_query",
# # #         lambda s: s.get("strategy", "sql_only"),
# # #         {
# # #             "conversational": "conversational",
# # #             "vector_only":    "vector_search",
# # #             "sql_only":       "generate_sql",
# # #             "hybrid":         "vector_search",
# # #         },
# # #     )

# # #     # After vector search:
# # #     #   hybrid     → SQL leg (vector context will be merged in synthesis)
# # #     #   vector_only → synthesise directly
# # #     graph.add_conditional_edges(
# # #         "vector_search",
# # #         lambda s: "generate_sql" if s.get("strategy") == "hybrid" else "synthesise",
# # #         {"generate_sql": "generate_sql", "synthesise": "synthesise"},
# # #     )

# # #     # SQL path
# # #     graph.add_edge("generate_sql", "execute_sql")
# # #     graph.add_conditional_edges(
# # #         "execute_sql",
# # #         should_retry_sql,
# # #         {
# # #             "synthesise": "synthesise",
# # #             "retry_sql":  "generate_sql",
# # #             "error":      "error",
# # #         },
# # #     )

# # #     # Terminal edges
# # #     graph.add_edge("synthesise",     END)
# # #     graph.add_edge("conversational", END)
# # #     graph.add_edge("error",          END)

# # #     memory = MemorySaver()
# # #     app    = graph.compile(checkpointer=memory)
# # #     logger.info("✅ LangGraph agent compiled successfully.")
# # #     return app


# # # def run_agent(
# # #     app,
# # #     user_question: str,
# # #     session_id: str = "default",
# # #     conversation_history: Optional[List[Any]] = None,
# # # ) -> Dict[str, Any]:
# # #     """
# # #     Invoke the agent for one user question and return structured results.

# # #     Args:
# # #         app:                  Compiled LangGraph app from build_agent_graph()
# # #         user_question:        The user's natural language question
# # #         session_id:           Identifies the conversation for memory persistence
# # #         conversation_history: Prior messages from _session_store (may be empty)

# # #     Returns:
# # #         Dict with keys: final_answer, success, strategy, sql_query, sql_results,
# # #                         vector_results, sql_attempts, session_id, error, messages
# # #     """
# # #     initial_state: AgentState = {
# # #         "messages":            conversation_history or [],
# # #         "user_question":       user_question,
# # #         "session_id":          session_id,
# # #         "strategy":            None,
# # #         "routing_reasoning":   None,
# # #         "vector_query":        None,
# # #         "vector_index":        None,
# # #         "vector_results":      None,
# # #         "vector_results_text": None,
# # #         "sql_query":           None,
# # #         "need_embedding":      False,
# # #         "embedding_params":    [],
# # #         "sql_results":         None,
# # #         "sql_results_text":    None,
# # #         "sql_attempt":         0,
# # #         "sql_attempt_history": [],
# # #         "max_sql_retries":     4,
# # #         "final_answer":        None,
# # #         "success":             False,
# # #         "error":               None,
# # #     }

# # #     config = {"configurable": {"thread_id": session_id}}

# # #     try:
# # #         final = app.invoke(initial_state, config=config)
# # #         return {
# # #             "final_answer":   final.get("final_answer", "No answer generated."),
# # #             "success":        final.get("success", False),
# # #             "strategy":       final.get("strategy"),
# # #             "sql_query":      final.get("sql_query"),
# # #             "sql_results":    final.get("sql_results"),
# # #             "vector_results": final.get("vector_results"),
# # #             "sql_attempts":   final.get("sql_attempt", 0),
# # #             "session_id":     session_id,
# # #             "error":          final.get("error"),
# # #             "messages":       final.get("messages", []),   # FIX: expose messages for session store
# # #         }
# # #     except Exception as exc:
# # #         logger.error("Agent graph error: %s", exc, exc_info=True)
# # #         return {
# # #             "final_answer": f"System error: {exc}",
# # #             "success":      False,
# # #             "session_id":   session_id,
# # #             "error":        str(exc),
# # #             "messages":     conversation_history or [],
# # #         }














# # #!/usr/bin/env python3
# # """
# # LangGraph Agent Graph – Property Management Agentic RAG Chatbot
# # MySQL 5.7 + FAISS + SchemaManager

# # v6.0 — New nodes:
# #   preprocess → (fuzzy correction + memory resolution + scope guard)
# #               → out_of_scope (immediate END if rejected)
# #               → route_query → ... existing flow ...

# # Graph flow:
# #   START
# #     → preprocess_node
# #         → out_of_scope → END   (rejected by scope guard)
# #         → route_query
# #             → conversational → END
# #             → vector_search → (hybrid → generate_sql | vector_only → synthesise) → END
# #             → generate_sql → execute_sql
# #                 → synthesise → END
# #                 → retry_sql  → generate_sql
# #                 → error      → END
# # """

# # import functools
# # import logging

# # from langgraph.graph import StateGraph, START, END
# # from langgraph.checkpoint.memory import MemorySaver

# # from agent_state import AgentState
# # from agent_nodes import (
# #     preprocess_node,
# #     route_query_node,
# #     vector_search_node,
# #     generate_sql_node,
# #     execute_sql_node,
# #     synthesise_answer_node,
# #     conversational_node,
# #     error_node,
# #     should_retry_sql,
# # )

# # logger = logging.getLogger(__name__)


# # def build_agent_graph(db_manager, vector_store, schema_manager, fuzzy_matcher=None):
# #     """
# #     Compile the LangGraph StateGraph.

# #     Args:
# #         db_manager:     DatabaseManager  (MySQL 5.7)
# #         vector_store:   VectorStore      (FAISS)
# #         schema_manager: SchemaManager    (schema cache + SQL validation)
# #         fuzzy_matcher:  FuzzyMatcher     (entity name correction, optional)
# #     """

# #     # Bind dependencies
# #     _preprocess = functools.partial(preprocess_node, fuzzy_matcher=fuzzy_matcher)
# #     _route      = functools.partial(
# #         route_query_node,
# #         db_manager=db_manager,
# #         vector_store=vector_store,
# #         schema_manager=schema_manager,
# #     )
# #     _vector    = functools.partial(vector_search_node,  vector_store=vector_store)
# #     _gen_sql   = functools.partial(
# #         generate_sql_node,
# #         db_manager=db_manager,
# #         schema_manager=schema_manager,
# #     )
# #     _exec_sql  = functools.partial(execute_sql_node, db_manager=db_manager)

# #     graph = StateGraph(AgentState)

# #     # Register nodes
# #     graph.add_node("preprocess",     _preprocess)
# #     graph.add_node("route_query",    _route)
# #     graph.add_node("vector_search",  _vector)
# #     graph.add_node("generate_sql",   _gen_sql)
# #     graph.add_node("execute_sql",    _exec_sql)
# #     graph.add_node("synthesise",     synthesise_answer_node)
# #     graph.add_node("conversational", conversational_node)
# #     graph.add_node("error",          error_node)

# #     # Entry
# #     graph.add_edge(START, "preprocess")

# #     # After preprocess: out_of_scope → END, everything else → route_query
# #     def _after_preprocess(state: AgentState) -> str:
# #         if state.get("is_out_of_scope"):
# #             return END
# #         return "route_query"

# #     graph.add_conditional_edges(
# #         "preprocess",
# #         _after_preprocess,
# #         {END: END, "route_query": "route_query"},
# #     )

# #     # After routing
# #     graph.add_conditional_edges(
# #         "route_query",
# #         lambda s: s.get("strategy", "sql_only"),
# #         {
# #             "conversational": "conversational",
# #             "vector_only":    "vector_search",
# #             "sql_only":       "generate_sql",
# #             "hybrid":         "vector_search",
# #             "out_of_scope":   END,   # safety fallback (preprocess handles this normally)
# #         },
# #     )

# #     # After vector search:
# #     #   hybrid     → SQL leg
# #     #   vector_only → synthesise directly
# #     graph.add_conditional_edges(
# #         "vector_search",
# #         lambda s: "generate_sql" if s.get("strategy") == "hybrid" else "synthesise",
# #         {"generate_sql": "generate_sql", "synthesise": "synthesise"},
# #     )

# #     # SQL path
# #     graph.add_edge("generate_sql", "execute_sql")
# #     graph.add_conditional_edges(
# #         "execute_sql",
# #         should_retry_sql,
# #         {
# #             "synthesise": "synthesise",
# #             "retry_sql":  "generate_sql",
# #             "error":      "error",
# #         },
# #     )

# #     # Terminal edges
# #     graph.add_edge("synthesise",     END)
# #     graph.add_edge("conversational", END)
# #     graph.add_edge("error",          END)

# #     memory = MemorySaver()
# #     app    = graph.compile(checkpointer=memory)
# #     logger.info("✅ LangGraph agent compiled (nodes: preprocess → route → sql/vector/hybrid).")
# #     return app


# # def run_agent(
# #     app,
# #     user_question: str,
# #     session_id: str = "default",
# #     conversation_history=None,
# # ):
# #     """
# #     Invoke the agent for one user question.

# #     Returns dict with:
# #       final_answer, success, strategy, sql_query, sql_results,
# #       vector_results, sql_attempts, session_id, error, messages,
# #       resolved_question, fuzzy_corrections
# #     """
# #     initial_state: AgentState = {
# #         "messages":            conversation_history or [],
# #         "user_question":       user_question,
# #         "resolved_question":   user_question,   # will be updated by preprocess_node
# #         "session_id":          session_id,
# #         "fuzzy_corrections":   [],
# #         "conversation_summary": None,
# #         "is_out_of_scope":     False,
# #         "strategy":            None,
# #         "routing_reasoning":   None,
# #         "vector_query":        None,
# #         "vector_index":        None,
# #         "vector_results":      None,
# #         "vector_results_text": None,
# #         "sql_query":           None,
# #         "need_embedding":      False,
# #         "embedding_params":    [],
# #         "sql_results":         None,
# #         "sql_results_text":    None,
# #         "sql_attempt":         0,
# #         "sql_attempt_history": [],
# #         "max_sql_retries":     3,
# #         "final_answer":        None,
# #         "success":             False,
# #         "error":               None,
# #     }

# #     config = {"configurable": {"thread_id": session_id}}

# #     try:
# #         final = app.invoke(initial_state, config=config)
# #         return {
# #             "final_answer":      final.get("final_answer", "No answer generated."),
# #             "success":           final.get("success", False),
# #             "strategy":          final.get("strategy"),
# #             "sql_query":         final.get("sql_query"),
# #             "sql_results":       final.get("sql_results"),
# #             "vector_results":    final.get("vector_results"),
# #             "sql_attempts":      final.get("sql_attempt", 0),
# #             "session_id":        session_id,
# #             "error":             final.get("error"),
# #             "messages":          final.get("messages", []),
# #             "resolved_question": final.get("resolved_question", user_question),
# #             "fuzzy_corrections": final.get("fuzzy_corrections", []),
# #             "is_out_of_scope":   final.get("is_out_of_scope", False),
# #         }
# #     except Exception as exc:
# #         logger.error("Agent graph error: %s", exc, exc_info=True)
# #         return {
# #             "final_answer":      f"System error: {exc}",
# #             "success":           False,
# #             "session_id":        session_id,
# #             "error":             str(exc),
# #             "messages":          conversation_history or [],
# #             "resolved_question": user_question,
# #             "fuzzy_corrections": [],
# #             "is_out_of_scope":   False,
# #         }


# #!/usr/bin/env python3
# #!/usr/bin/env python3
# """
# LangGraph Agent Graph – Property Management Agentic RAG Chatbot
# MySQL 5.7 + FAISS + SchemaManager

# v6.0 — New nodes:
#   preprocess → (fuzzy correction + memory resolution + scope guard)
#               → out_of_scope (immediate END if rejected)
#               → route_query → ... existing flow ...

# Graph flow:
#   START
#     → preprocess_node
#         → out_of_scope → END   (rejected by scope guard)
#         → route_query
#             → conversational → END
#             → vector_search → (hybrid → generate_sql | vector_only → synthesise) → END
#             → generate_sql → execute_sql
#                 → synthesise → END
#                 → retry_sql  → generate_sql
#                 → error      → END
# """

# import functools
# import logging

# from langgraph.graph import StateGraph, START, END
# from langgraph.checkpoint.memory import MemorySaver

# from agent_state import AgentState
# from agent_nodes import (
#     preprocess_node,
#     route_query_node,
#     vector_search_node,
#     db_rag_node,
#     generate_sql_node,
#     execute_sql_node,
#     synthesise_answer_node,
#     conversational_node,
#     error_node,
#     should_retry_sql,
# )

# logger = logging.getLogger(__name__)


# def build_agent_graph(db_manager, vector_store, schema_manager, fuzzy_matcher=None):
#     """
#     Compile the LangGraph StateGraph.

#     Args:
#         db_manager:     DatabaseManager  (MySQL 5.7)
#         vector_store:   VectorStore      (FAISS)
#         schema_manager: SchemaManager    (schema cache + SQL validation)
#         fuzzy_matcher:  FuzzyMatcher     (entity name correction, optional)
#     """

#     # Bind dependencies
#     _preprocess = functools.partial(preprocess_node, fuzzy_matcher=fuzzy_matcher)
#     _route      = functools.partial(
#         route_query_node,
#         db_manager=db_manager,
#         vector_store=vector_store,
#         schema_manager=schema_manager,
#     )
#     _vector    = functools.partial(vector_search_node,  vector_store=vector_store, db_manager=db_manager)
#     _db_rag    = functools.partial(db_rag_node, db_manager=db_manager)
#     _gen_sql   = functools.partial(
#         generate_sql_node,
#         db_manager=db_manager,
#         schema_manager=schema_manager,
#     )
#     _exec_sql  = functools.partial(execute_sql_node, db_manager=db_manager)

#     graph = StateGraph(AgentState)

#     # Register nodes
#     graph.add_node("preprocess",     _preprocess)
#     graph.add_node("route_query",    _route)
#     graph.add_node("vector_search",  _vector)
#     graph.add_node("db_rag",         _db_rag)
#     graph.add_node("generate_sql",   _gen_sql)
#     graph.add_node("execute_sql",    _exec_sql)
#     graph.add_node("synthesise",     synthesise_answer_node)
#     graph.add_node("conversational", conversational_node)
#     graph.add_node("error",          error_node)

#     # Entry
#     graph.add_edge(START, "preprocess")

#     # After preprocess: out_of_scope → END, everything else → route_query
#     def _after_preprocess(state: AgentState) -> str:
#         if state.get("is_out_of_scope"):
#             return END
#         return "route_query"

#     graph.add_conditional_edges(
#         "preprocess",
#         _after_preprocess,
#         {END: END, "route_query": "route_query"},
#     )

#     # After routing
#     graph.add_conditional_edges(
#         "route_query",
#         lambda s: s.get("strategy", "sql_only"),
#         {
#             "conversational": "conversational",
#             "vector_only":    "vector_search",
#             "sql_only":       "generate_sql",
#             "hybrid":         "vector_search",
#             "db_rag":         "vector_search",   # qualitative → FAISS search → RAG synthesise
#             "out_of_scope":   END,
#         },
#     )

#     # After vector search:
#     #   hybrid     → SQL leg
#     #   vector_only → synthesise directly
#     graph.add_conditional_edges(
#         "vector_search",
#         lambda s: "generate_sql" if s.get("strategy") == "hybrid" else "synthesise",
#         {"generate_sql": "generate_sql", "synthesise": "synthesise"},
#     )

#     # SQL path
#     graph.add_edge("generate_sql", "execute_sql")
#     graph.add_conditional_edges(
#         "execute_sql",
#         should_retry_sql,
#         {
#             "synthesise": "synthesise",
#             "retry_sql":  "generate_sql",
#             "error":      "error",
#         },
#     )

#     # Terminal edges
#     graph.add_edge("synthesise",     END)
#     graph.add_edge("conversational", END)
#     graph.add_edge("error",          END)

#     memory = MemorySaver()
#     app    = graph.compile(checkpointer=memory)
#     logger.info("✅ LangGraph agent compiled (nodes: preprocess → route → sql/vector/hybrid).")
#     return app


# def run_agent(
#     app,
#     user_question: str,
#     session_id: str = "default",
#     conversation_history=None,
# ):
#     """
#     Invoke the agent for one user question.

#     Returns dict with:
#       final_answer, success, strategy, sql_query, sql_results,
#       vector_results, sql_attempts, session_id, error, messages,
#       resolved_question, fuzzy_corrections
#     """
#     initial_state: AgentState = {
#         "messages":            conversation_history or [],
#         "user_question":       user_question,
#         "resolved_question":   user_question,   # will be updated by preprocess_node
#         "session_id":          session_id,
#         "fuzzy_corrections":   [],
#         "conversation_summary": None,
#         "is_out_of_scope":     False,
#         "strategy":            None,
#         "routing_reasoning":   None,
#         "vector_query":        None,
#         "vector_index":        None,
#         "vector_results":      None,
#         "vector_results_text": None,
#         "sql_query":           None,
#         "need_embedding":      False,
#         "embedding_params":    [],
#         "sql_results":         None,
#         "sql_results_text":    None,
#         "sql_attempt":         0,
#         "sql_attempt_history": [],
#         "max_sql_retries":     3,
#         "final_answer":        None,
#         "success":             False,
#         "error":               None,
#     }

#     config = {"configurable": {"thread_id": session_id}}

#     try:
#         final = app.invoke(initial_state, config=config)
#         return {
#             "final_answer":      final.get("final_answer", "No answer generated."),
#             "success":           final.get("success", False),
#             "strategy":          final.get("strategy"),
#             "sql_query":         final.get("sql_query"),
#             "sql_results":       final.get("sql_results"),
#             "vector_results":    final.get("vector_results"),
#             "sql_attempts":      final.get("sql_attempt", 0),
#             "session_id":        session_id,
#             "error":             final.get("error"),
#             "messages":          final.get("messages", []),
#             "resolved_question": final.get("resolved_question", user_question),
#             "fuzzy_corrections": final.get("fuzzy_corrections", []),
#             "is_out_of_scope":   final.get("is_out_of_scope", False),
#         }
#     except Exception as exc:
#         logger.error("Agent graph error: %s", exc, exc_info=True)
#         return {
#             "final_answer":      f"System error: {exc}",
#             "success":           False,
#             "session_id":        session_id,
#             "error":             str(exc),
#             "messages":          conversation_history or [],
#             "resolved_question": user_question,
#             "fuzzy_corrections": [],
#             "is_out_of_scope":   False,
#         }


#!/usr/bin/env python3
"""
LangGraph Agent Graph – Property Management Agentic RAG Chatbot
MySQL 5.7 + FAISS + SchemaManager

v6.0 — New nodes:
  preprocess → (fuzzy correction + memory resolution + scope guard)
              → out_of_scope (immediate END if rejected)
              → route_query → ... existing flow ...

Graph flow:
  START
    → preprocess_node
        → out_of_scope → END   (rejected by scope guard)
        → route_query
            → conversational → END
            → vector_search → (hybrid → generate_sql | vector_only → synthesise) → END
            → generate_sql → execute_sql
                → synthesise → END
                → retry_sql  → generate_sql
                → error      → END
"""

import functools
import logging

from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from agent_state import AgentState
from agent_nodes import (
    preprocess_node,
    route_query_node,
    vector_search_node,
    db_rag_node,
    generate_sql_node,
    execute_sql_node,
    synthesise_answer_node,
    conversational_node,
    error_node,
    should_retry_sql,
)

logger = logging.getLogger(__name__)


def build_agent_graph(db_manager, vector_store, schema_manager, fuzzy_matcher=None):
    """
    Compile the LangGraph StateGraph.

    Args:
        db_manager:     DatabaseManager  (MySQL 5.7)
        vector_store:   VectorStore      (FAISS)
        schema_manager: SchemaManager    (schema cache + SQL validation)
        fuzzy_matcher:  FuzzyMatcher     (entity name correction, optional)
    """

    # Bind dependencies
    _preprocess = functools.partial(preprocess_node, fuzzy_matcher=fuzzy_matcher)
    _route      = functools.partial(
        route_query_node,
        db_manager=db_manager,
        vector_store=vector_store,
        schema_manager=schema_manager,
    )
    _vector    = functools.partial(vector_search_node,  vector_store=vector_store, db_manager=db_manager)
    _db_rag    = functools.partial(db_rag_node, db_manager=db_manager)
    _gen_sql   = functools.partial(
        generate_sql_node,
        db_manager=db_manager,
        schema_manager=schema_manager,
    )
    _exec_sql  = functools.partial(execute_sql_node, db_manager=db_manager)

    graph = StateGraph(AgentState)

    # Register nodes
    graph.add_node("preprocess",     _preprocess)
    graph.add_node("route_query",    _route)
    graph.add_node("vector_search",  _vector)
    graph.add_node("db_rag",         _db_rag)
    graph.add_node("generate_sql",   _gen_sql)
    graph.add_node("execute_sql",    _exec_sql)
    _synth = functools.partial(synthesise_answer_node, db_manager=db_manager)
    graph.add_node("synthesise",     _synth)
    graph.add_node("conversational", conversational_node)
    graph.add_node("error",          error_node)

    # Entry
    graph.add_edge(START, "preprocess")

    # After preprocess: out_of_scope → END, everything else → route_query
    def _after_preprocess(state: AgentState) -> str:
        if state.get("is_out_of_scope"):
            return END
        return "route_query"

    graph.add_conditional_edges(
        "preprocess",
        _after_preprocess,
        {END: END, "route_query": "route_query"},
    )

    # After routing
    graph.add_conditional_edges(
        "route_query",
        lambda s: s.get("strategy", "sql_only"),
        {
            "conversational": "conversational",
            "vector_only":    "vector_search",
            "sql_only":       "generate_sql",
            "hybrid":         "vector_search",
            "db_rag":         "vector_search",   # qualitative → FAISS search → RAG synthesise
            "out_of_scope":   END,
        },
    )

    # After vector search:
    #   hybrid     → SQL leg
    #   vector_only → synthesise directly
    graph.add_conditional_edges(
        "vector_search",
        lambda s: "generate_sql" if s.get("strategy") == "hybrid" else "synthesise",
        {"generate_sql": "generate_sql", "synthesise": "synthesise"},
    )

    # SQL path
    graph.add_edge("generate_sql", "execute_sql")
    graph.add_conditional_edges(
        "execute_sql",
        should_retry_sql,
        {
            "synthesise": "synthesise",
            "retry_sql":  "generate_sql",
            "error":      "error",
        },
    )

    # Terminal edges
    graph.add_edge("synthesise",     END)
    graph.add_edge("conversational", END)
    graph.add_edge("error",          END)

    memory = MemorySaver()
    app    = graph.compile(checkpointer=memory)
    logger.info("✅ LangGraph agent compiled (nodes: preprocess → route → sql/vector/hybrid).")
    return app


def run_agent(
    app,
    user_question: str,
    session_id: str = "default",
    conversation_history=None,
):
    """
    Invoke the agent for one user question.

    Returns dict with:
      final_answer, success, strategy, sql_query, sql_results,
      vector_results, sql_attempts, session_id, error, messages,
      resolved_question, fuzzy_corrections
    """
    initial_state: AgentState = {
        "messages":            conversation_history or [],
        "user_question":       user_question,
        "resolved_question":   user_question,   # will be updated by preprocess_node
        "session_id":          session_id,
        "fuzzy_corrections":   [],
        "conversation_summary": None,
        "is_out_of_scope":     False,
        "strategy":            None,
        "routing_reasoning":   None,
        "vector_query":        None,
        "vector_index":        None,
        "vector_results":      None,
        "vector_results_text": None,
        "sql_query":           None,
        "need_embedding":      False,
        "embedding_params":    [],
        "sql_results":         None,
        "sql_results_text":    None,
        "sql_attempt":         0,
        "sql_attempt_history": [],
        "max_sql_retries":     3,
        "final_answer":        None,
        "success":             False,
        "error":               None,
    }

    config = {"configurable": {"thread_id": session_id}}

    try:
        final = app.invoke(initial_state, config=config)
        return {
            "final_answer":      final.get("final_answer", "No answer generated."),
            "success":           final.get("success", False),
            "strategy":          final.get("strategy"),
            "sql_query":         final.get("sql_query"),
            "sql_results":       final.get("sql_results"),
            "vector_results":    final.get("vector_results"),
            "sql_attempts":      final.get("sql_attempt", 0),
            "session_id":        session_id,
            "error":             final.get("error"),
            "messages":          final.get("messages", []),
            "resolved_question": final.get("resolved_question", user_question),
            "fuzzy_corrections": final.get("fuzzy_corrections", []),
            "is_out_of_scope":   final.get("is_out_of_scope", False),
        }
    except Exception as exc:
        logger.error("Agent graph error: %s", exc, exc_info=True)
        return {
            "final_answer":      f"System error: {exc}",
            "success":           False,
            "session_id":        session_id,
            "error":             str(exc),
            "messages":          conversation_history or [],
            "resolved_question": user_question,
            "fuzzy_corrections": [],
            "is_out_of_scope":   False,
        }