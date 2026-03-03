#!/usr/bin/env python3
"""
llm_utils.py – Shared LLM helpers used by both agent_nodes and preprocessor.

Extracted here to break the circular import:
  agent_nodes → preprocessor → agent_nodes  (was circular)
  agent_nodes → llm_utils    ← preprocessor (clean)
"""

import logging
import re
import time
from typing import Any

import openai
from langchain_openai import ChatOpenAI

from config import LLM_MODEL, LLM_TEMPERATURE, LLM_MAX_TOKENS

logger = logging.getLogger(__name__)

_RATE_LIMIT_MAX_RETRIES  = 6
_RATE_LIMIT_INITIAL_WAIT = 1.0
_RATE_LIMIT_MAX_WAIT     = 32.0


def get_llm(temperature: float = None, max_tokens: int = None) -> ChatOpenAI:
    return ChatOpenAI(
        model=LLM_MODEL,
        temperature=temperature if temperature is not None else LLM_TEMPERATURE,
        max_tokens=max_tokens if max_tokens is not None else LLM_MAX_TOKENS,
    )


def llm_invoke_with_retry(llm: ChatOpenAI, messages: list, context: str = "") -> Any:
    """
    Invoke the LLM with exponential-backoff retry on 429 / 5xx errors.
    Waits: 1s → 2s → 4s → 8s → 16s → 32s (up to 6 retries).
    """
    wait     = _RATE_LIMIT_INITIAL_WAIT
    last_exc = None

    for attempt in range(1, _RATE_LIMIT_MAX_RETRIES + 1):
        try:
            return llm.invoke(messages)

        except openai.RateLimitError as exc:
            last_exc = exc
            suggested = None
            try:
                m = re.search(r'try again in (\d+)ms', str(exc), re.IGNORECASE)
                if m:
                    suggested = max(int(m.group(1)) / 1000.0 + 0.2, wait)
            except Exception:
                pass
            sleep_for = suggested if suggested else min(wait, _RATE_LIMIT_MAX_WAIT)
            logger.warning(
                "[LLM:%s] 429 rate limit (attempt %d/%d). Waiting %.1fs …",
                context, attempt, _RATE_LIMIT_MAX_RETRIES, sleep_for,
            )
            time.sleep(sleep_for)
            wait = min(wait * 2, _RATE_LIMIT_MAX_WAIT)

        except openai.APIStatusError as exc:
            if exc.status_code in (500, 502, 503):
                last_exc = exc
                sleep_for = min(wait, _RATE_LIMIT_MAX_WAIT)
                logger.warning(
                    "[LLM:%s] API %d (attempt %d/%d). Waiting %.1fs …",
                    context, exc.status_code, attempt, _RATE_LIMIT_MAX_RETRIES, sleep_for,
                )
                time.sleep(sleep_for)
                wait = min(wait * 2, _RATE_LIMIT_MAX_WAIT)
            else:
                raise

        except Exception:
            raise

    logger.error("[LLM:%s] All %d retries failed.", context, _RATE_LIMIT_MAX_RETRIES)
    raise last_exc


def parse_json(content: str) -> dict:
    """Strip markdown fences and parse JSON."""
    raw = content.strip()
    if raw.startswith("```"):
        raw = re.sub(r"```\w*", "", raw).strip().strip("`").strip()
    return __import__("json").loads(raw)