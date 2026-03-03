# #!/usr/bin/env python3
# """
# LangGraph State Definition – Property Management Agent
# """

# from typing import Annotated, Any, Dict, List, Literal, Optional
# from typing_extensions import TypedDict
# from langgraph.graph.message import add_messages


# class AgentState(TypedDict):
#     """Shared state passed between all LangGraph nodes."""

#     # ── Conversation history (auto-merged by LangGraph) ───────────────────
#     messages: Annotated[List[Any], add_messages]

#     # ── Current turn inputs ────────────────────────────────────────────────
#     user_question: str
#     session_id:    str

#     # ── Routing decision ──────────────────────────────────────────────────
#     strategy:          Optional[Literal["sql_only", "vector_only", "hybrid", "conversational"]]
#     routing_reasoning: Optional[str]

#     # ── Vector search ─────────────────────────────────────────────────────
#     vector_query:        Optional[str]
#     vector_index:        Optional[str]
#     vector_results:      Optional[List[Dict[str, Any]]]
#     vector_results_text: Optional[str]

#     # ── SQL generation & execution ────────────────────────────────────────
#     sql_query:          Optional[str]
#     need_embedding:     bool
#     embedding_params:   Optional[List[Dict[str, Any]]]
#     sql_results:        Optional[Dict[str, Any]]
#     sql_results_text:   Optional[str]

#     # ── Retry handling ────────────────────────────────────────────────────
#     sql_attempt:         int
#     sql_attempt_history: List[Dict[str, str]]   # [{sql, error}, ...]
#     max_sql_retries:     int

#     # ── Final output ──────────────────────────────────────────────────────
#     final_answer: Optional[str]
#     success:      bool
#     error:        Optional[str]


#!/usr/bin/env python3
"""
LangGraph State Definition – Property Management Agent

v6.0 — New fields:
  resolved_question     : question after fuzzy correction + memory resolution
  fuzzy_corrections     : list of {original, corrected, score, type} dicts
  is_out_of_scope       : True when guard node rejects the question
  conversation_summary  : rolling summary of prior turns (memory)
"""

from typing import Annotated, Any, Dict, List, Literal, Optional
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    """Shared state passed between all LangGraph nodes."""

    # ── Conversation history (auto-merged by LangGraph) ───────────────────
    messages: Annotated[List[Any], add_messages]

    # ── Current turn inputs ────────────────────────────────────────────────
    user_question:    str          # raw, exactly as typed
    resolved_question: str         # after fuzzy correction + memory resolution
    session_id:       str

    # ── Fuzzy matching ────────────────────────────────────────────────────
    fuzzy_corrections: List[Dict[str, Any]]  # [{original, corrected, score, type}]

    # ── Conversation memory ───────────────────────────────────────────────
    conversation_summary: Optional[str]   # rolling plain-text summary of prior turns

    # ── Question guard ────────────────────────────────────────────────────
    is_out_of_scope: bool   # True → guard node already set final_answer

    # ── Routing decision ──────────────────────────────────────────────────
    strategy:          Optional[Literal["sql_only", "vector_only", "hybrid", "conversational", "out_of_scope"]]
    routing_reasoning: Optional[str]

    # ── Vector search ─────────────────────────────────────────────────────
    vector_query:        Optional[str]
    vector_index:        Optional[str]
    vector_results:      Optional[List[Dict[str, Any]]]
    vector_results_text: Optional[str]

    # ── SQL generation & execution ────────────────────────────────────────
    sql_query:          Optional[str]
    need_embedding:     bool
    embedding_params:   Optional[List[Dict[str, Any]]]
    sql_results:        Optional[Dict[str, Any]]
    sql_results_text:   Optional[str]

    # ── Retry handling ────────────────────────────────────────────────────
    sql_attempt:         int
    sql_attempt_history: List[Dict[str, str]]
    max_sql_retries:     int

    # ── Final output ──────────────────────────────────────────────────────
    final_answer: Optional[str]
    success:      bool
    error:        Optional[str]