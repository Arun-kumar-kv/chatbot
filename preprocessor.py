#!/usr/bin/env python3
"""
Preprocessor – runs before routing on every user message.

Three jobs in sequence:
  1. FUZZY CORRECTION   : fix typos in property / tenant / contract names
  2. MEMORY RESOLUTION  : expand pronouns & short references using prior turns
                          e.g. "what about Q3?" → "what is the revenue for Q3 in SEASTONE RESIDENCE 2?"
  3. SCOPE GUARD        : reject questions that are not about the ERP database

All three are fast and cheap:
  - Fuzzy correction is pure Python (difflib, no LLM call)
  - Memory resolution is one tiny LLM call only when pronouns/references are detected
  - Scope guard is one tiny LLM call returning a single JSON field

The main entry point is:
    preprocessor.run(user_question, conversation_history)
    → PreprocessResult(resolved_question, fuzzy_corrections, is_out_of_scope, refusal_message)
"""

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from llm_utils import llm_invoke_with_retry, parse_json as _parse_json, get_llm as _get_llm

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

# Tokens that suggest the question refers to a prior turn
_REFERENCE_SIGNALS = {
    "it", "its", "that", "those", "them", "they", "this", "these",
    "same", "same property", "same tenant", "same contract",
    "the above", "mentioned", "previous", "last one", "first one",
    "what about", "how about", "and", "also",
}

# Minimum words in resolved question before we bother with resolution
_MIN_WORDS_FOR_RESOLUTION = 3


@dataclass
class PreprocessResult:
    resolved_question: str
    fuzzy_corrections: List[Dict[str, Any]] = field(default_factory=list)
    is_out_of_scope:   bool = False
    refusal_message:   str  = ""


# ── Scope guard ────────────────────────────────────────────────────────────────

_SCOPE_GUARD_SYSTEM = """You are a scope filter for a Property & Lease Management ERP chatbot.

The chatbot CAN answer questions about (always in scope):
  - Properties, buildings, units, floors, blocks, towers, residences
  - Leases, contracts, tenants, rental agreements
  - Rent payments, receipts, outstanding dues, arrears, overdue amounts
  - Vacancy, occupancy, available units, unit status
  - Bounced cheques, payment delays, collection rates
  - Contract expiry, renewals, upcoming expirations
  - Revenue, income, financial KPIs related to the above
  - Maintenance (including which properties are maintenance-heavy, maintenance costs,
    maintenance frequency, maintenance impact on vacancy/revenue)
  - Analytical questions about the above (trends, comparisons, risk analysis)
  - General ERP help (what can the system do, how to use it)
  - Follow-up questions, clarifications, or references to prior answers
  - Greetings, thanks, simple conversation

The chatbot CANNOT answer (out of scope):
  - General world knowledge with NO connection to real estate / property management
    Examples: "What is the capital of France?", "Who won the World Cup?"
  - Coding, programming, or technical topics unrelated to this ERP
    Examples: "Explain Python decorators", "How do I use React?"
  - Medical, legal advice unrelated to property management
  - News, sports, entertainment, weather (unless about local factors affecting property)

IMPORTANT RULES:
  - When in doubt → answer {"in_scope": true}. It is better to attempt an answer than to wrongly reject.
  - ANY question containing property/tenant/contract/unit/maintenance/vacancy/rent/lease → ALWAYS in scope
  - Analytical questions about ERP data (comparisons, trends, risk, patterns) → ALWAYS in scope
  - Short or ambiguous questions → {"in_scope": true}
  - Follow-up questions → {"in_scope": true}

Respond ONLY with valid JSON:
{"in_scope": true}  OR  {"in_scope": false}"""

_OUT_OF_SCOPE_REPLY = (
    "I can only answer questions about this Property & Lease Management ERP — "
    "properties, units, tenants, contracts, rent, payments, and related topics.\n\n"
    "Your question appears to be outside that scope. "
    "Please ask something related to the ERP database."
)


def check_scope(question: str, history: List[Any]) -> Tuple[bool, str]:
    """
    Returns (in_scope: bool, refusal_message: str).
    in_scope=True  → proceed normally
    in_scope=False → return refusal_message to user
    """
    q_lower = question.strip().lower()
    words   = q_lower.split()

    # ── Fast-pass: always in scope without LLM call ───────────────────────────

    # 1. Very short messages (greetings, thanks)
    if len(words) <= 3:
        return True, ""

    # 2. Contains any ERP domain keyword → definitely in scope
    _ERP_KEYWORDS = {
        "property", "properties", "unit", "units", "tenant", "tenants",
        "contract", "contracts", "lease", "leases", "rent", "rents",
        "vacancy", "vacant", "occupancy", "occupied", "available",
        "maintenance", "payment", "receipt", "invoice", "charge", "due", "dues",
        "overdue", "outstanding", "arrear", "arrears", "bounced", "cheque",
        "expiry", "expir", "renewal", "renew", "collection", "revenue",
        "income", "building", "floor", "block", "tower", "residence",
        "apartment", "villa", "office", "shop",
        # complaint / ticket topics
        "complaint", "complaints", "ticket", "tickets", "incident", "incidents",
        "legal request", "move-out remark", "reported by tenant", "tenant issue",
        "frequently reported", "open complaint", "resolved complaint",
        # trend / analytics topics
        "move-in", "move-out", "month-over-month", "trend", "seasonal",
        "rental loss", "revenue loss", "renewal rate", "churn", "late payer",
        "re-leased", "re-rented", "turnaround", "discharge",
    }
    if any(kw in q_lower for kw in _ERP_KEYWORDS):
        return True, ""

    # 3. Explicit follow-up signals
    _FOLLOWUP = {"what about", "how about", "and that", "tell me more",
                 "what else", "show me", "same", "also", "what did"}
    if any(sig in q_lower for sig in _FOLLOWUP) and history:
        return True, ""

    # ── LLM call only for genuinely ambiguous questions ───────────────────────
    try:
        llm = _get_llm(temperature=0, max_tokens=20)
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=_SCOPE_GUARD_SYSTEM),
            HumanMessage(content=question),
        ], context="scope_guard")
        parsed   = _parse_json(response.content)
        in_scope = bool(parsed.get("in_scope", True))
        return in_scope, ("" if in_scope else _OUT_OF_SCOPE_REPLY)

    except Exception as exc:
        logger.warning("[ScopeGuard] Error (allowing through): %s", exc)
        return True, ""


# ── Memory resolver ───────────────────────────────────────────────────────────

_MEMORY_SYSTEM = """You are a question rewriter for a Property & Lease Management ERP chatbot.

Your job: rewrite the user's CURRENT QUESTION so it is fully self-contained,
using the CONVERSATION HISTORY to fill in any missing context.

Rules:
  - Replace pronouns (it, they, that, those, its) with the actual entity names from history
  - Replace vague references ("same one", "the above", "that property", "same building") with the real name
  - Expand incomplete follow-ups ("what about TOWER 2?") into a full question using the prior question's intent
  - If the current question is ALREADY fully self-contained → return it EXACTLY as-is, word for word
  - NEVER invent entity names or details not present in the history
  - NEVER change the meaning or add assumptions beyond what history supports
  - NEVER copy text from the Assistant's answers into the rewritten question
  - Return ONLY the rewritten question as plain text — no quotes, no explanation, no preamble

CRITICAL — When to return EXACTLY as-is (do not modify):
  - The question contains specific property names, numbers, or complete context already
  - The question is a statement like "show me X" or "list X" with no pronouns
  - The question is long (>8 words) and mentions specific entities by name
  - When in doubt → return the question exactly as written

Decision guide:
  - Can you answer "what entity / metric / filter is this question about?" from the question alone? → return as-is
  - Do you need the history to understand what entity or context the question refers to? → rewrite

Examples:
  History: User asked about SEASTONE RESIDENCE 2 / Assistant said "15 units available"
  Current: "what about GALAXY TOWER?"
  Output:  "How many vacant units in GALAXY TOWER?"

  History: User: "Show contract CONTRACT/2024/GAL2-207/001" / Assistant: showed details
  Current: "when does it expire?"
  Output:  "When does contract CONTRACT/2024/GAL2-207/001 expire?"

  History: User: "Show tenants in SKYREACH RESIDENCE 5" / Assistant: showed list
  Current: "show their outstanding dues"
  Output:  "Show outstanding dues for tenants in SKYREACH RESIDENCE 5"

  History: (anything)
  Current: "Which properties have the most vacant units?"
  Output:  "Which properties have the most vacant units?"

  History: (anything)
  Current: "How many vacant units are there in all properties?"
  Output:  "How many vacant units are there in all properties?"

  History: Assistant said "There are 14 vacant units..."
  Current: "List all vacant units by property"
  Output:  "List all vacant units by property"
"""


def resolve_with_memory(question: str, history: List[Any]) -> str:
    """
    Rewrite the question to be self-contained using conversation history.
    Returns the original question unchanged if no history, or if LLM fails.
    """
    if not _needs_resolution(question, history):
        return question

    # Build compact history string — last 6 messages (3 turns)
    recent       = history[-6:]
    history_text = ""
    for msg in recent:
        if isinstance(msg, HumanMessage):
            history_text += f"User: {msg.content}\n"
        elif isinstance(msg, AIMessage):
            content = msg.content[:300] + ("..." if len(msg.content) > 300 else "")
            history_text += f"Assistant: {content}\n"

    if not history_text.strip():
        return question

    try:
        llm    = _get_llm(temperature=0, max_tokens=150)
        prompt = (
            f"CONVERSATION HISTORY:\n{history_text.strip()}\n\n"
            f"CURRENT QUESTION: {question}\n\n"
            "Rewrite (or return as-is if already self-contained):"
        )
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=_MEMORY_SYSTEM),
            HumanMessage(content=prompt),
        ], context="memory_resolver")

        resolved = response.content.strip().strip('"').strip("'")

        # Sanity checks
        if not resolved or len(resolved) < 4:
            return question
        if len(resolved) > len(question) * 6:
            return question  # LLM went off-track

        if resolved.lower() != question.lower():
            logger.info("[MemoryResolver] '%s' → '%s'", question, resolved)

        return resolved

    except Exception as exc:
        logger.warning("[MemoryResolver] Error (using original): %s", exc)
        return question


def _needs_resolution(question: str, history: List[Any]) -> bool:
    """
    Decide if we should attempt memory resolution.
    Only resolve if the question genuinely needs prior context.
    """
    if not history:
        return False

    q_lower = question.lower().strip()
    words   = q_lower.split()

    # Always resolve short questions — likely a follow-up
    if len(words) <= 4:
        return True

    # Always resolve if explicit pronoun/reference signals present
    if set(words) & _REFERENCE_SIGNALS:
        return True

    # Always resolve common follow-up openers
    _FOLLOWUP_OPENERS = {
        "what about", "how about", "and for", "and in", "same for",
        "what if", "now show", "also show", "now what", "what else",
    }
    if any(q_lower.startswith(op) for op in _FOLLOWUP_OPENERS):
        return True

    # Skip resolution for long questions (>8 words) that are clearly
    # self-contained: contain no pronouns and mention specific entities
    if len(words) > 8:
        pronoun_signals = {"it", "they", "them", "their", "its", "those",
                           "that", "these", "this", "same", "above", "previous"}
        has_pronoun = bool(set(words) & pronoun_signals)
        if not has_pronoun:
            return False  # Self-contained — skip LLM call entirely

    return True


def resolve_with_memory(question: str, history: List[Any]) -> str:
    """
    Rewrite the question to be self-contained using conversation history.
    Returns the original question unchanged if no history, or if LLM fails.
    """
    if not _needs_resolution(question, history):
        return question

    # Build compact history string — last 6 messages (3 turns)
    recent       = history[-6:]
    history_text = ""
    for msg in recent:
        if isinstance(msg, HumanMessage):
            history_text += f"User: {msg.content}\n"
        elif isinstance(msg, AIMessage):
            content = msg.content[:300] + ("..." if len(msg.content) > 300 else "")
            history_text += f"Assistant: {content}\n"

    if not history_text.strip():
        return question

    try:
        llm    = _get_llm(temperature=0, max_tokens=150)
        prompt = (
            f"CONVERSATION HISTORY:\n{history_text.strip()}\n\n"
            f"CURRENT QUESTION: {question}\n\n"
            "Rewrite (or return as-is if already self-contained):"
        )
        response = llm_invoke_with_retry(llm, [
            SystemMessage(content=_MEMORY_SYSTEM),
            HumanMessage(content=prompt),
        ], context="memory_resolver")

        resolved = response.content.strip().strip('"').strip("'")

        # Sanity checks
        if not resolved or len(resolved) < 4:
            return question
        if len(resolved) > len(question) * 6:
            return question  # LLM went off-track

        if resolved.lower() != question.lower():
            logger.info("[MemoryResolver] '%s' → '%s'", question, resolved)

        return resolved

    except Exception as exc:
        logger.warning("[MemoryResolver] Error (using original): %s", exc)
        return question


# ── Main entry point ──────────────────────────────────────────────────────────

def run(
    user_question:    str,
    conversation_history: List[Any],
    fuzzy_matcher=None,
) -> PreprocessResult:
    """
    Run all three preprocessing steps in order.

    Args:
        user_question:         Raw question from user
        conversation_history:  List of prior LangChain messages (HumanMessage, AIMessage)
        fuzzy_matcher:         FuzzyMatcher instance (or None to skip fuzzy correction)

    Returns:
        PreprocessResult with resolved_question, corrections, scope info
    """
    question = user_question.strip()
    corrections: List[Dict] = []

    # ── Step 1: Fuzzy correction ──────────────────────────────────────────────
    if fuzzy_matcher and fuzzy_matcher.is_ready():
        question, corrections = fuzzy_matcher.correct_question(question)
        if corrections:
            logger.info(
                "[Preprocessor] Fuzzy corrections applied: %s",
                [(c["original"], "→", c["corrected"]) for c in corrections],
            )

    # ── Step 2: Memory resolution ─────────────────────────────────────────────
    resolved = resolve_with_memory(question, conversation_history)

    # ── Step 3: Scope guard ───────────────────────────────────────────────────
    in_scope, refusal = check_scope(resolved, conversation_history)

    return PreprocessResult(
        resolved_question = resolved,
        fuzzy_corrections = corrections,
        is_out_of_scope   = not in_scope,
        refusal_message   = refusal,
    )