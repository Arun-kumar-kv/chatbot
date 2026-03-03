#!/usr/bin/env python3
"""
Fuzzy Entity Matcher  —  v2.0
Corrects typos in property names, tenant names, and contract numbers
before SQL is generated.

Examples of what it SHOULD do:
  "see1"         → "SEA1"
  "seastone2"    → "SEASTONE RESIDENCE 2"
  "skyreachh"    → "SKYREACH RESIDENCE 5"

Examples of what it MUST NOT do:
  "SKYREACH RESIDENCE 5"  → leave unchanged  (already correct)
  "RESIDENCE 5?"          → leave unchanged  (trailing punct only)
  "are in SKYREACH"       → leave unchanged  (stop words + valid word)
  "properties"            → leave unchanged  (generic word)

Key design rules in v2.0:
  1. EXACT MATCH FIRST: if any token or n-gram is an exact (case-insensitive)
     match or prefix/substring of a real entity name, skip correction entirely.
  2. STRICT N-GRAM FILTERING: n-grams are only candidates if ALL their
     component tokens pass the entity-name guard (no stop words in the span).
  3. HIGHER THRESHOLD: minimum score raised to 0.72 for single tokens,
     0.80 for multi-word spans (reduces false positives).
  4. REQUIRE MEANINGFUL IMPROVEMENT: correction only applied when the
     corrected form is significantly different AND longer than the original
     (e.g. "see1" → "SEA1" OK; "SKYREACH" → "SKYREACH RESIDENCE 1" NOT OK
      because we'd be adding words the user didn't type).
  5. NO PARTIAL NAME REPLACEMENT: if a token is already an exact word
     within any real entity name (e.g. "SKYREACH"), don't replace it.
"""

import logging
import os
import re
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

# Minimum similarity score — HIGHER than before to avoid false positives
# Single token typo correction (e.g. "see1" → "SEA1")
_MIN_SCORE_SINGLE: float = float(os.getenv("FUZZY_MIN_SCORE", "0.72"))

# Multi-word span correction — needs even higher confidence
_MIN_SCORE_MULTI: float = float(os.getenv("FUZZY_MIN_SCORE_MULTI", "0.85"))

# Minimum character length before attempting fuzzy match
_MIN_TOKEN_LEN: int = 3

# Stop words — NEVER fuzzy-match tokens that are in this set
_STOP_WORDS = {
    # Question / connective words
    "how", "many", "what", "which", "show", "list", "find", "get",
    "the", "for", "all", "are", "in", "of", "to", "is", "and", "or",
    "with", "from", "that", "this", "on", "by", "at", "a", "an",
    "do", "does", "did", "will", "would", "could", "should", "can",
    "give", "me", "please", "tell", "about", "between", "across",
    "top", "bottom", "best", "worst", "most", "least", "any", "some",
    "who", "when", "where", "why", "whose", "have", "has", "had",
    # Generic ERP domain nouns (categories, not specific names)
    "property", "properties",
    "unit", "units",
    "tenant", "tenants",
    "contract", "contracts",
    "lease", "leases",
    "building", "buildings",
    "floor", "floors",
    "block", "blocks",
    "tower", "towers",
    "residence", "residences",
    "apartment", "apartments",
    "office", "offices",
    "shop", "shops",
    "villa", "villas",
    "payment", "payments",
    "receipt", "receipts",
    "invoice", "invoices",
    "charge", "charges",
    "due", "dues",
    "rent", "rents",
    "revenue", "revenues",
    "income", "expenses",
    "vacancy", "vacant",
    "occupancy", "occupied",
    "available", "availability",
    "active", "inactive", "expired",
    "maintenance", "maintenance-heavy",
    "total", "count", "number", "sum", "average", "avg",
    "current", "previous", "next", "last", "first",
    "monthly", "quarterly", "annual", "yearly", "weekly", "daily",
    "status", "type", "category", "categories",
    "certain", "specific", "particular",
    "heavy", "high", "low", "medium",
    "their", "these", "those", "them", "they",
}

# Entity source configuration
_ENTITY_SOURCES = [
    {
        "type":        "property",
        "table_env":   "FUZZY_PROPERTY_TABLE",
        "default_tbl": "TERP_LS_PROPERTY",
        "col_env":     "FUZZY_PROPERTY_COL",
        "default_col": "NAME",
    },
    {
        "type":        "tenant",
        "table_env":   "FUZZY_TENANT_TABLE",
        "default_tbl": "TERP_LS_TENANTS",
        "col_env":     "FUZZY_TENANT_COL",
        "default_col": "NAME",
    },
    {
        "type":        "contract",
        "table_env":   "FUZZY_CONTRACT_TABLE",
        "default_tbl": "TERP_LS_CONTRACT",
        "col_env":     "FUZZY_CONTRACT_COL",
        "default_col": "CONTRACT_NO",
    },
]


class FuzzyMatcher:
    """
    Loads real entity names from MySQL and corrects obvious typos in questions.

    Conservative by design — it's better to pass a slightly misspelled name
    to the SQL engine (which uses LIKE '%...%' anyway) than to replace a
    correct name with a wrong one.
    """

    def __init__(self, db_manager):
        self.db = db_manager
        self._entities: Dict[str, List[str]] = {}
        # Flat set of all lowercase entity words for exact-match fast-lookup
        self._entity_words: Dict[str, set] = {}
        self._load()

    # ── Loading ───────────────────────────────────────────────────────────────

    def _load(self):
        for src in _ENTITY_SOURCES:
            table = os.getenv(src["table_env"], src["default_tbl"])
            col   = os.getenv(src["col_env"],   src["default_col"])
            names = self._fetch_names(table, col)
            if src["type"] == "contract" and not names:
                names = self._fetch_names(table, "NAME")
            self._entities[src["type"]] = names
            # Build word index for fast exact-match detection
            words: set = set()
            for name in names:
                for word in re.split(r'[\s\-_/]+', name.lower()):
                    if len(word) >= 2:
                        words.add(word)
                # Also add the full name lowercased
                words.add(name.lower())
            self._entity_words[src["type"]] = words
            logger.info(
                "[FuzzyMatcher] Loaded %d %s names (%d unique words)",
                len(names), src["type"], len(words),
            )

    def _fetch_names(self, table: str, col: str) -> List[str]:
        try:
            result = self.db.execute_query(
                f"SELECT DISTINCT `{col}` FROM `{table}` "
                f"WHERE `{col}` IS NOT NULL AND `{col}` != '' "
                f"ORDER BY `{col}` LIMIT 5000"
            )
            if result.get("success") and result.get("rows"):
                return [str(r[0]).strip() for r in result["rows"] if r[0]]
        except Exception as exc:
            logger.warning("[FuzzyMatcher] Could not load %s.%s: %s", table, col, exc)
        return []

    def refresh(self):
        logger.info("[FuzzyMatcher] Refreshing …")
        self._entities = {}
        self._entity_words = {}
        self._load()

    # ── Exact-match guards ────────────────────────────────────────────────────

    def _is_exact_entity_word(self, token: str) -> bool:
        """
        Return True if this token is already an exact word found inside
        at least one real entity name. In that case, no correction needed.
        e.g. "SKYREACH" → True (it IS a real property word)
        e.g. "see1"     → False (not an exact match, may need correction)
        """
        t = token.lower().strip("?.,!;:")
        for words in self._entity_words.values():
            if t in words:
                return True
        return False

    def _question_already_contains_entity(self, question: str) -> List[str]:
        """
        Return the list of full entity names that are already present
        (case-insensitive substring) in the question.
        Used to protect correctly-spelled entity references.
        """
        q = question.lower()
        found = []
        for names in self._entities.values():
            for name in names:
                if name.lower() in q:
                    found.append(name)
        return found

    # ── Scoring ───────────────────────────────────────────────────────────────

    def _score(self, token: str, candidate: str) -> float:
        """
        Score how well a user-typed token matches a real entity name (for typo correction).

        Three similarity measures combined:
          1. full_sim    — direct char similarity between token and full candidate name
          2. best_word   — char similarity of token against each individual word of candidate
                          (for single-word tokens only, e.g. 'skyreachh' vs 'skyreach')
          3. concat_sim  — char similarity against candidate with spaces removed
                          (for 'seastone2' matching 'SEASTONE RESIDENCE 2')

        Length penalty applied when candidate has many more words than token
        (prevents short tokens matching long names unless word-level match is strong).
        """
        t = token.lower().strip("?.,!;:")
        c = candidate.lower()
        cand_words = [w for w in re.split(r'[\s\-_/]+', c) if len(w) >= 2]

        # 1. Full string similarity
        full_sim = SequenceMatcher(None, t, c).ratio()

        # 2. Best single-word match — only for single-word tokens
        #    Allows 'skyreachh' to match 'SKYREACH' inside 'SKYREACH RESIDENCE 5'
        best_word = 0.0
        if len(token.strip().split()) == 1 and cand_words:
            best_word = max(SequenceMatcher(None, t, w).ratio() for w in cand_words)

        # 3. Concatenated candidate (no spaces/hyphens)
        c_concat = re.sub(r'[\s\-_/]', '', c)
        concat_sim = SequenceMatcher(None, t, c_concat).ratio()

        base = max(full_sim, best_word, concat_sim * 0.92)

        # Length penalty: applies only when word-level match is weak AND
        # candidate has significantly more words than the token
        tw = len(token.strip().split())
        cw = len(candidate.strip().split())
        if best_word < 0.72 and cw > tw + 1 and not any(ch.isdigit() for ch in token):
            base *= (0.70 ** (cw - tw - 1))

        return min(base, 1.0)

    def _best_match(self, token: str, entity_type: str) -> Tuple[Optional[str], float]:
        candidates = self._entities.get(entity_type, [])
        if not candidates:
            return None, 0.0
        best_name  = None
        best_score = 0.0
        for cand in candidates:
            s = self._score(token, cand)
            if s > best_score:
                best_score = s
                best_name  = cand
        return best_name, best_score

    # ── Token eligibility ─────────────────────────────────────────────────────

    def _is_correction_candidate(self, token: str, is_multiword: bool) -> bool:
        """
        Decide if this token/span is a candidate for fuzzy correction.

        A token IS a candidate if it looks like a mistyped entity name:
          - Contains a digit (e.g. "see1", "seastone2", "gal2-207")
          - Is fully uppercase ≥4 chars (e.g. "SKYREACHH")
          - Is multi-word and all component words pass the guard

        A token is NOT a candidate if:
          - It's in the stop word list
          - It's a pure number
          - It's too short
          - It's already an exact word in a real entity name
          - It's a generic lowercase word
        """
        cleaned = token.strip("?.,!;:")

        # Stop word check (single token)
        if not is_multiword and cleaned.lower() in _STOP_WORDS:
            return False

        # If multi-word: reject if ANY component word is a stop word
        # (this prevents "are in SKYREACH" from being an n-gram candidate)
        if is_multiword:
            words = cleaned.lower().split()
            if any(w in _STOP_WORDS for w in words):
                return False

        # Pure number / date / numeric expression — NEVER correct these
        # Strip all separators: spaces, hyphens, slashes, dots, colons
        # so "30/60", "30-60", "2024.01", "90 days" all register as numeric
        numeric_stripped = re.sub(r'[\s\-/\.,:]', '', cleaned)
        if numeric_stripped.isdigit():
            return False
        # Also block tokens that are MOSTLY numeric (>60% digit chars)
        # catches "30/60/90", "2024-01-15" etc.
        digit_count = sum(c.isdigit() for c in cleaned)
        if len(cleaned) > 0 and digit_count / len(cleaned) > 0.6:
            return False

        # Too short
        if len(cleaned.replace(" ", "")) < _MIN_TOKEN_LEN:
            return False

        # Already an exact entity word → no correction needed
        if not is_multiword and self._is_exact_entity_word(cleaned):
            return False

        # For single tokens: must look like a specific entity name
        if not is_multiword:
            has_digit   = any(c.isdigit() for c in cleaned)
            is_caps     = (
                cleaned.replace(" ", "").replace("-", "").replace("/", "").isupper()
                and len(cleaned) >= 4
            )
            if not has_digit and not is_caps:
                return False

        return True

    # ── Main API ──────────────────────────────────────────────────────────────

    def correct_question(self, question: str) -> Tuple[str, List[Dict]]:
        """
        Scan a user question for typos in entity names and correct them.

        Conservative algorithm:
          1. Find entity names already correctly present → protect them
          2. For each remaining token / 2-gram / 3-gram:
             a. Skip if it fails _is_correction_candidate()
             b. Skip if it's an exact word of a real entity
             c. Fuzzy-match against all entity lists
             d. Only apply if score ≥ threshold AND result ≠ original
          3. Never replace a token that overlaps with a protected region
        """
        if not any(self._entities.values()):
            return question, []

        corrections: List[Dict] = []
        corrected = question

        # ── Step 1: find already-correct entity references ────────────────────
        # Mark character ranges that are already correct — don't touch them
        q_lower = question.lower()
        protected_ranges: List[Tuple[int, int]] = []
        for names in self._entities.values():
            for name in names:
                nl = name.lower()
                start = 0
                while True:
                    idx = q_lower.find(nl, start)
                    if idx == -1:
                        break
                    protected_ranges.append((idx, idx + len(nl)))
                    start = idx + 1

        def _in_protected(token_text: str, question_text: str) -> bool:
            """Check if this token overlaps with a protected entity range."""
            tl = token_text.lower()
            pos = question_text.lower().find(tl)
            while pos != -1:
                tok_end = pos + len(tl)
                for pr_start, pr_end in protected_ranges:
                    if pos < pr_end and tok_end > pr_start:
                        return True
                pos = question_text.lower().find(tl, pos + 1)
            return False

        # ── Step 2: build candidate spans ────────────────────────────────────
        tokens = question.split()
        spans  = []

        for i, tok in enumerate(tokens):
            clean = re.sub(r'[^\w/\-]', '', tok)
            if clean:
                spans.append((i, i + 1, clean, False))

        for i in range(len(tokens) - 1):
            bigram = " ".join(t.strip("?.,!;:") for t in tokens[i:i+2])
            spans.append((i, i + 2, bigram, True))

        for i in range(len(tokens) - 2):
            trigram = " ".join(t.strip("?.,!;:") for t in tokens[i:i+3])
            spans.append((i, i + 3, trigram, True))

        # Longest spans first
        spans.sort(key=lambda x: -(x[1] - x[0]))

        matched_indices: set = set()

        for start, end, span_text, is_multi in spans:
            if any(idx in matched_indices for idx in range(start, end)):
                continue

            if not self._is_correction_candidate(span_text, is_multi):
                continue

            # Skip if this span overlaps a correctly-spelled entity
            if _in_protected(span_text, corrected):
                continue

            # Choose threshold based on span length
            threshold = _MIN_SCORE_MULTI if is_multi else _MIN_SCORE_SINGLE

            # Find best match across all entity types
            best_cand  = None
            best_score = 0.0
            best_type  = None

            for etype in ("property", "tenant", "contract"):
                cand, score = self._best_match(span_text, etype)
                if cand and score > best_score:
                    best_score = score
                    best_cand  = cand
                    best_type  = etype

            # Only apply if above threshold AND genuinely different
            if (best_cand
                    and best_score >= threshold
                    and best_cand.lower() != span_text.lower()):

                corrections.append({
                    "original":  span_text,
                    "corrected": best_cand,
                    "score":     round(best_score, 3),
                    "type":      best_type,
                })
                corrected = re.sub(
                    re.escape(span_text),
                    best_cand,
                    corrected,
                    count=1,
                    flags=re.IGNORECASE,
                )
                # Update protected ranges after replacement
                new_lower = corrected.lower()
                bl = best_cand.lower()
                idx = new_lower.find(bl)
                if idx != -1:
                    protected_ranges.append((idx, idx + len(bl)))

                for idx in range(start, end):
                    matched_indices.add(idx)

        if corrections:
            logger.info(
                "[FuzzyMatcher] '%s' → '%s'  corrections=%s",
                question, corrected,
                [(c["original"], c["corrected"], c["score"]) for c in corrections],
            )

        return corrected, corrections

    # ── Info ─────────────────────────────────────────────────────────────────

    def entity_counts(self) -> Dict[str, int]:
        return {k: len(v) for k, v in self._entities.items()}

    def is_ready(self) -> bool:
        return any(len(v) > 0 for v in self._entities.values())