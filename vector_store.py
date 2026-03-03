# # #!/usr/bin/env python3
# # """
# # Vector Store Manager
# # Loads a single FAISS .index file and a .pkl metadata file.

# # Expected metadata format (pickle):
# #   A Python list of dicts, one dict per vector row.
# #   Example entry:
# #   {
# #       "id": 42,
# #       "table": "leases",
# #       "text": "Lease for unit 3B – tenant John Doe – expires 2025-12-31",
# #       "unit_id": 10,
# #       "property_id": 3,
# #       ...
# #   }

# # Config (from .env):
# #   FAISS_INDEX_PATH        = E:/file/faiss.index
# #   FAISS_METADATA_PATH     = E:/file/metadata.pkl
# #   VECTOR_DEFAULT_THRESHOLD = 0.65   (adaptive start)
# #   VECTOR_EXPAND_THRESHOLD  = 0.45   (fallback if < MIN_RESULTS found)
# #   VECTOR_MIN_RESULTS       = 3
# # """

# # import logging
# # import pickle
# # from pathlib import Path
# # from typing import Any, Dict, List, Optional

# # import faiss
# # import numpy as np
# # from sentence_transformers import SentenceTransformer

# # from config import (
# #     FAISS_INDEX_PATH,
# #     FAISS_METADATA_PATH,
# #     EMBEDDING_MODEL,
# #     EMBEDDING_DIM,
# #     VECTOR_TOP_K,
# #     VECTOR_DEFAULT_THRESHOLD,
# #     VECTOR_EXPAND_THRESHOLD,
# #     VECTOR_MIN_RESULTS,
# # )

# # logger = logging.getLogger(__name__)


# # class VectorStore:
# #     """
# #     Wraps a pre-built FAISS index + pickle metadata file.

# #     The index MUST have been built with sentence-transformers/all-MiniLM-L6-v2
# #     and unit-normalised vectors (so that L2 distance == cosine distance).

# #     Key improvements over v1:
# #       - Adaptive threshold expansion (spec §3.4):
# #           Start at VECTOR_DEFAULT_THRESHOLD (0.65).
# #           If fewer than VECTOR_MIN_RESULTS results, expand to VECTOR_EXPAND_THRESHOLD (0.45).
# #       - Explicit domain-based threshold adjustment.
# #       - Better metadata error handling.
# #     """

# #     def __init__(self):
# #         logger.info("Loading embedding model: %s", EMBEDDING_MODEL)
# #         self.encoder = SentenceTransformer(EMBEDDING_MODEL)

# #         self.index: Optional[faiss.Index] = None
# #         self.metadata: List[Dict[str, Any]] = []
# #         self._available: bool = False

# #         self._load()

# #     # ── Loading ──────────────────────────────────────────────────────────────

# #     def _load(self):
# #         idx_path  = Path(FAISS_INDEX_PATH)
# #         meta_path = Path(FAISS_METADATA_PATH)

# #         # Load FAISS index
# #         if not idx_path.exists():
# #             logger.error(
# #                 "FAISS index not found: %s – Semantic search disabled.", idx_path
# #             )
# #             return

# #         try:
# #             self.index = faiss.read_index(str(idx_path))
# #             logger.info(
# #                 "✅ FAISS index loaded: %s  (%d vectors, dim=%d)",
# #                 idx_path, self.index.ntotal, self.index.d,
# #             )
# #         except Exception as exc:
# #             logger.error("Failed to load FAISS index: %s", exc)
# #             return

# #         if self.index.d != EMBEDDING_DIM:
# #             logger.warning(
# #                 "Index dimension (%d) ≠ EMBEDDING_DIM config (%d). "
# #                 "Verify EMBEDDING_MODEL and EMBEDDING_DIM settings.",
# #                 self.index.d, EMBEDDING_DIM,
# #             )

# #         # Load metadata (pickle)
# #         if not meta_path.exists():
# #             logger.warning(
# #                 "Metadata file not found: %s – Results will lack text/field info.", meta_path
# #             )
# #             self.metadata = [{"_id": i} for i in range(self.index.ntotal)]
# #         else:
# #             try:
# #                 with open(meta_path, "rb") as f:
# #                     raw = pickle.load(f)

# #                 if isinstance(raw, list):
# #                     self.metadata = raw
# #                 elif isinstance(raw, dict):
# #                     self.metadata = [raw[k] for k in sorted(raw.keys())]
# #                 else:
# #                     raise ValueError(f"Unsupported metadata format: {type(raw)}")

# #                 logger.info(
# #                     "✅ Metadata loaded: %s  (%d entries)", meta_path, len(self.metadata)
# #                 )

# #                 if len(self.metadata) != self.index.ntotal:
# #                     logger.warning(
# #                         "Metadata count (%d) ≠ index vectors (%d). "
# #                         "Some results may lack metadata.",
# #                         len(self.metadata), self.index.ntotal,
# #                     )
# #             except Exception as exc:
# #                 logger.error("Failed to load metadata: %s", exc)
# #                 self.metadata = [{"_id": i} for i in range(self.index.ntotal)]

# #         self._available = True

# #     # ── Encoding ─────────────────────────────────────────────────────────────

# #     def encode(self, text: str) -> np.ndarray:
# #         """Encode a text string to a unit-normalised float32 vector. Returns shape (1, dim)."""
# #         vec = self.encoder.encode([text], normalize_embeddings=True)
# #         return vec.astype("float32")

# #     # ── Internal raw search ───────────────────────────────────────────────────

# #     def _raw_search(
# #         self,
# #         query: str,
# #         top_k: int,
# #         score_threshold: float,
# #     ) -> List[Dict[str, Any]]:
# #         """Execute a single FAISS search pass at a fixed threshold."""
# #         query_vec = self.encode(query)
# #         k = min(top_k, self.index.ntotal)
# #         distances, ids = self.index.search(query_vec, k)

# #         results: List[Dict[str, Any]] = []
# #         for rank, (dist, vec_id) in enumerate(zip(distances[0], ids[0])):
# #             if vec_id == -1:
# #                 continue

# #             # For unit-normalised vectors: cosine_similarity = 1 - (L2² / 2)
# #             score = float(max(0.0, 1.0 - dist / 2.0))

# #             if score < score_threshold:
# #                 continue

# #             entry = dict(self.metadata[vec_id]) if vec_id < len(self.metadata) else {"_id": int(vec_id)}
# #             entry["_score"] = round(score, 4)
# #             entry["_rank"]  = rank + 1
# #             results.append(entry)

# #         return results

# #     # ── Public search ─────────────────────────────────────────────────────────

# #     def search(
# #         self,
# #         query: str,
# #         top_k: int = VECTOR_TOP_K,
# #         score_threshold: float = VECTOR_DEFAULT_THRESHOLD,
# #     ) -> List[Dict[str, Any]]:
# #         """
# #         Search the FAISS index with adaptive threshold expansion (spec §3.4).

# #         Algorithm:
# #           1. Search at `score_threshold` (default 0.65).
# #           2. If fewer than VECTOR_MIN_RESULTS found AND threshold > EXPAND_THRESHOLD,
# #              expand to VECTOR_EXPAND_THRESHOLD (0.45) and re-search.
# #           3. Return best results sorted by score descending.

# #         Args:
# #             query:           Natural language query.
# #             top_k:           Max results to return.
# #             score_threshold: Initial similarity threshold (0–1).

# #         Returns:
# #             List of result dicts with _score and _rank fields.
# #         """
# #         if not self._available or self.index is None:
# #             logger.warning("VectorStore not available – returning empty results.")
# #             return []

# #         results = self._raw_search(query, top_k, score_threshold)

# #         # Adaptive expansion: if too few results, lower the bar
# #         if len(results) < VECTOR_MIN_RESULTS and score_threshold > VECTOR_EXPAND_THRESHOLD:
# #             logger.info(
# #                 "[VectorStore] Only %d results at threshold %.2f – expanding to %.2f",
# #                 len(results), score_threshold, VECTOR_EXPAND_THRESHOLD,
# #             )
# #             expanded = self._raw_search(query, top_k, VECTOR_EXPAND_THRESHOLD)
# #             if len(expanded) > len(results):
# #                 results = expanded
# #                 logger.info("[VectorStore] Expanded to %d results", len(results))

# #         # Sort by score descending and re-rank
# #         results.sort(key=lambda r: r["_score"], reverse=True)
# #         for i, r in enumerate(results):
# #             r["_rank"] = i + 1

# #         logger.info("[VectorStore] Returning %d results for query: %.80s", len(results), query)
# #         return results

# #     def search_formatted(
# #         self,
# #         query: str,
# #         top_k: int = VECTOR_TOP_K,
# #         score_threshold: float = VECTOR_DEFAULT_THRESHOLD,
# #     ) -> str:
# #         """Return search results as a readable multi-line string for LLM context."""
# #         results = self.search(query, top_k=top_k, score_threshold=score_threshold)

# #         if not results:
# #             return "No semantically similar records found in the vector store."

# #         lines = [f"Semantic Search Results for: '{query}'", "=" * 50]
# #         for r in results:
# #             lines.append(f"\n[Rank {r['_rank']}  |  Similarity Score: {r['_score']}]")
# #             for k, v in r.items():
# #                 if not k.startswith("_"):
# #                     lines.append(f"  {k}: {v}")

# #         return "\n".join(lines)

# #     # ── Info helpers ──────────────────────────────────────────────────────────

# #     @property
# #     def is_available(self) -> bool:
# #         return self._available

# #     @property
# #     def total_vectors(self) -> int:
# #         return self.index.ntotal if self.index else 0

# #     def list_indices(self) -> List[str]:
# #         return ["faiss_main"] if self._available else []

# #     def index_size(self, name: str = "faiss_main") -> int:
# #         return self.total_vectors


# #!/usr/bin/env python3
# """
# Vector Store Manager
# Loads a single FAISS .index file and a .pkl metadata file.

# Expected metadata format (pickle):
#   A Python list of dicts, one dict per vector row.
#   Example entry:
#   {
#       "id": 42,
#       "table": "leases",
#       "text": "Lease for unit 3B – tenant John Doe – expires 2025-12-31",
#       "unit_id": 10,
#       "property_id": 3,
#       ...
#   }

# Config (from .env):
#   FAISS_INDEX_PATH        = E:/file/faiss.index
#   FAISS_METADATA_PATH     = E:/file/metadata.pkl
#   VECTOR_DEFAULT_THRESHOLD = 0.65   (adaptive start)
#   VECTOR_EXPAND_THRESHOLD  = 0.45   (fallback if < MIN_RESULTS found)
#   VECTOR_MIN_RESULTS       = 3
# """

# import logging
# import pickle
# from pathlib import Path
# from typing import Any, Dict, List, Optional

# import faiss
# import numpy as np
# from sentence_transformers import SentenceTransformer

# from config import (
#     FAISS_INDEX_PATH,
#     FAISS_METADATA_PATH,
#     EMBEDDING_MODEL,
#     EMBEDDING_DIM,
#     VECTOR_TOP_K,
#     VECTOR_DEFAULT_THRESHOLD,
#     VECTOR_EXPAND_THRESHOLD,
#     VECTOR_MIN_RESULTS,
# )

# logger = logging.getLogger(__name__)


# class VectorStore:
#     """
#     Wraps a pre-built FAISS index + pickle metadata file.

#     The index MUST have been built with sentence-transformers/all-MiniLM-L6-v2
#     and unit-normalised vectors (so that L2 distance == cosine distance).

#     Key improvements over v1:
#       - Adaptive threshold expansion (spec §3.4):
#           Start at VECTOR_DEFAULT_THRESHOLD (0.65).
#           If fewer than VECTOR_MIN_RESULTS results, expand to VECTOR_EXPAND_THRESHOLD (0.45).
#       - Explicit domain-based threshold adjustment.
#       - Better metadata error handling.
#     """

#     def __init__(self):
#         logger.info("Loading embedding model: %s", EMBEDDING_MODEL)
#         self.encoder = SentenceTransformer(EMBEDDING_MODEL)

#         self.index: Optional[faiss.Index] = None
#         self.metadata: List[Dict[str, Any]] = []
#         self._available: bool = False

#         self._load()

#     # ── Loading ──────────────────────────────────────────────────────────────

#     def _load(self):
#         idx_path  = Path(FAISS_INDEX_PATH)
#         meta_path = Path(FAISS_METADATA_PATH)

#         # Load FAISS index
#         if not idx_path.exists():
#             logger.error(
#                 "FAISS index not found: %s – Semantic search disabled.", idx_path
#             )
#             return

#         try:
#             self.index = faiss.read_index(str(idx_path))
#             logger.info(
#                 "✅ FAISS index loaded: %s  (%d vectors, dim=%d)",
#                 idx_path, self.index.ntotal, self.index.d,
#             )
#         except Exception as exc:
#             logger.error("Failed to load FAISS index: %s", exc)
#             return

#         if self.index.d != EMBEDDING_DIM:
#             logger.warning(
#                 "Index dimension (%d) ≠ EMBEDDING_DIM config (%d). "
#                 "Verify EMBEDDING_MODEL and EMBEDDING_DIM settings.",
#                 self.index.d, EMBEDDING_DIM,
#             )

#         # Load metadata (pickle)
#         if not meta_path.exists():
#             logger.warning(
#                 "Metadata file not found: %s – Results will lack text/field info.", meta_path
#             )
#             self.metadata = [{"_id": i} for i in range(self.index.ntotal)]
#         else:
#             try:
#                 with open(meta_path, "rb") as f:
#                     raw = pickle.load(f)

#                 if isinstance(raw, list):
#                     self.metadata = raw
#                 elif isinstance(raw, dict):
#                     self.metadata = [raw[k] for k in sorted(raw.keys())]
#                 else:
#                     raise ValueError(f"Unsupported metadata format: {type(raw)}")

#                 logger.info(
#                     "✅ Metadata loaded: %s  (%d entries)", meta_path, len(self.metadata)
#                 )

#                 if len(self.metadata) != self.index.ntotal:
#                     logger.warning(
#                         "Metadata count (%d) ≠ index vectors (%d). "
#                         "Some results may lack metadata.",
#                         len(self.metadata), self.index.ntotal,
#                     )
#             except Exception as exc:
#                 logger.error("Failed to load metadata: %s", exc)
#                 self.metadata = [{"_id": i} for i in range(self.index.ntotal)]

#         self._available = True

#     # ── Encoding ─────────────────────────────────────────────────────────────

#     def encode(self, text: str) -> np.ndarray:
#         """Encode a text string to a unit-normalised float32 vector. Returns shape (1, dim)."""
#         vec = self.encoder.encode([text], normalize_embeddings=True)
#         return vec.astype("float32")

#     # ── Internal raw search ───────────────────────────────────────────────────

#     def _raw_search(
#         self,
#         query: str,
#         top_k: int,
#         score_threshold: float,
#     ) -> List[Dict[str, Any]]:
#         """Execute a single FAISS search pass at a fixed threshold."""
#         query_vec = self.encode(query)
#         k = min(top_k, self.index.ntotal)
#         distances, ids = self.index.search(query_vec, k)

#         results: List[Dict[str, Any]] = []
#         for rank, (dist, vec_id) in enumerate(zip(distances[0], ids[0])):
#             if vec_id == -1:
#                 continue

#             # For unit-normalised vectors: cosine_similarity = 1 - (L2² / 2)
#             score = float(max(0.0, 1.0 - dist / 2.0))

#             if score < score_threshold:
#                 continue

#             entry = dict(self.metadata[vec_id]) if vec_id < len(self.metadata) else {"_id": int(vec_id)}
#             entry["_score"] = round(score, 4)
#             entry["_rank"]  = rank + 1
#             results.append(entry)

#         return results

#     # ── Public search ─────────────────────────────────────────────────────────

#     def search(
#         self,
#         query: str,
#         top_k: int = VECTOR_TOP_K,
#         score_threshold: float = VECTOR_DEFAULT_THRESHOLD,
#     ) -> List[Dict[str, Any]]:
#         """
#         Search the FAISS index with adaptive threshold expansion.

#         Algorithm:
#           1. Search at `score_threshold`.
#           2. If fewer than VECTOR_MIN_RESULTS found AND threshold > EXPAND_THRESHOLD,
#              drop to VECTOR_EXPAND_THRESHOLD and re-search.
#           3. Return best results sorted by score descending.
#         """
#         if not self._available or self.index is None:
#             logger.warning("VectorStore not available – returning empty results.")
#             return []

#         results = self._raw_search(query, top_k, score_threshold)

#         # Adaptive expansion: if too few results, lower the bar
#         if len(results) < VECTOR_MIN_RESULTS and score_threshold > VECTOR_EXPAND_THRESHOLD:
#             logger.info(
#                 "[VectorStore] Only %d results at threshold %.2f – expanding to %.2f",
#                 len(results), score_threshold, VECTOR_EXPAND_THRESHOLD,
#             )
#             expanded = self._raw_search(query, top_k, VECTOR_EXPAND_THRESHOLD)
#             if len(expanded) > len(results):
#                 results = expanded
#                 logger.info("[VectorStore] Expanded to %d results", len(results))

#         # For very low thresholds (RAG mode), expand even further if still empty
#         if len(results) < VECTOR_MIN_RESULTS and score_threshold <= VECTOR_EXPAND_THRESHOLD:
#             logger.info(
#                 "[VectorStore] RAG mode: threshold %.2f still only %d results – "
#                 "trying floor threshold 0.10",
#                 score_threshold, len(results),
#             )
#             floor = self._raw_search(query, top_k, 0.10)
#             if len(floor) > len(results):
#                 results = floor
#                 logger.info("[VectorStore] Floor expanded to %d results", len(results))

#         # Sort by score descending and re-rank
#         results.sort(key=lambda r: r["_score"], reverse=True)
#         for i, r in enumerate(results):
#             r["_rank"] = i + 1

#         logger.info("[VectorStore] Returning %d results for query: %.80s", len(results), query)
#         return results

#     def search_formatted(
#         self,
#         query: str,
#         top_k: int = VECTOR_TOP_K,
#         score_threshold: float = VECTOR_DEFAULT_THRESHOLD,
#     ) -> str:
#         """Return search results as a readable multi-line string for LLM context."""
#         results = self.search(query, top_k=top_k, score_threshold=score_threshold)

#         if not results:
#             return "No semantically similar records found in the vector store."

#         lines = [f"Semantic Search Results for: '{query}'", "=" * 50]
#         for r in results:
#             lines.append(f"\n[Rank {r['_rank']}  |  Similarity Score: {r['_score']}]")
#             for k, v in r.items():
#                 if not k.startswith("_"):
#                     lines.append(f"  {k}: {v}")

#         return "\n".join(lines)

#     # ── Info helpers ──────────────────────────────────────────────────────────

#     @property
#     def is_available(self) -> bool:
#         return self._available

#     @property
#     def total_vectors(self) -> int:
#         return self.index.ntotal if self.index else 0

#     def list_indices(self) -> List[str]:
#         return ["faiss_main"] if self._available else []

#     def index_size(self, name: str = "faiss_main") -> int:
#         return self.total_vectors

#!/usr/bin/env python3
"""
Vector Store Manager — OpenAI Embeddings + FAISS
=================================================
Uses OpenAI text-embedding-3-small (1536 dims) for encoding queries.
MUST use the same model that was used to build the FAISS index.

Config (.env):
  EMBEDDING_MODEL           = text-embedding-3-small   (default)
  EMBEDDING_DIM             = 1536
  OPENAI_API_KEY            = sk-...
  FAISS_INDEX_PATH          = E:/file/faiss.index
  FAISS_METADATA_PATH       = E:/file/metadata.pkl
  VECTOR_DEFAULT_THRESHOLD  = 0.65
  VECTOR_EXPAND_THRESHOLD   = 0.45
  VECTOR_MIN_RESULTS        = 3
"""

import logging
import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import faiss
import numpy as np

from config import (
    OPENAI_API_KEY,
    FAISS_INDEX_PATH,
    FAISS_METADATA_PATH,
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    VECTOR_TOP_K,
    VECTOR_DEFAULT_THRESHOLD,
    VECTOR_EXPAND_THRESHOLD,
    VECTOR_MIN_RESULTS,
)

logger = logging.getLogger(__name__)


class OpenAIEmbedder:
    """Thin wrapper around OpenAI Embeddings API with retry and L2 normalisation."""

    def __init__(self, model: str, api_key: str = ""):
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("Run: pip install openai")
        self.model  = model
        self.client = OpenAI(api_key=api_key or OPENAI_API_KEY)
        logger.info("OpenAI embedder ready: %s", model)

    def embed_batch(self, texts: List[str], retries: int = 3) -> np.ndarray:
        """Embed list of texts. Returns L2-normalised float32 (N, dim)."""
        texts = [t.replace("\n", " ").strip() or " " for t in texts]
        for attempt in range(retries):
            try:
                response = self.client.embeddings.create(input=texts, model=self.model)
                vecs  = np.array([item.embedding for item in response.data], dtype="float32")
                norms = np.linalg.norm(vecs, axis=1, keepdims=True)
                norms = np.where(norms == 0, 1, norms)
                return vecs / norms
            except Exception as exc:
                wait = 2 ** attempt
                logger.warning("Embed attempt %d/%d failed: %s (retry in %ds)", attempt+1, retries, exc, wait)
                if attempt < retries - 1:
                    time.sleep(wait)
                else:
                    raise

    def embed_one(self, text: str) -> np.ndarray:
        """Embed single string. Returns shape (1, dim)."""
        return self.embed_batch([text])


class VectorStore:
    """
    FAISS vector store using OpenAI embeddings.
    Adaptive threshold expansion: 0.65 → 0.45 → 0.10 if too few results.
    """

    def __init__(self):
        self.embedder: Optional[OpenAIEmbedder] = None
        self.index:    Optional[faiss.Index]    = None
        self.metadata: List[Dict[str, Any]]     = []
        self._available: bool = False
        self._load()

    def _load(self):
        try:
            self.embedder = OpenAIEmbedder(model=EMBEDDING_MODEL)
        except Exception as exc:
            logger.error("OpenAI embedder failed: %s – search disabled.", exc)
            return

        idx_path  = Path(FAISS_INDEX_PATH)
        meta_path = Path(FAISS_METADATA_PATH)

        if not idx_path.exists():
            logger.warning("FAISS index not found: %s  Run: python build_vector_index.py", idx_path)
            return

        try:
            self.index = faiss.read_index(str(idx_path))
            logger.info("✅ FAISS index: %s  (%d vectors, dim=%d)", idx_path, self.index.ntotal, self.index.d)
        except Exception as exc:
            logger.error("Failed to load FAISS index: %s", exc)
            return

        if self.index.d != EMBEDDING_DIM:
            logger.warning(
                "Index dim (%d) != EMBEDDING_DIM (%d). "
                "Rebuild with: python build_vector_index.py",
                self.index.d, EMBEDDING_DIM,
            )

        if not meta_path.exists():
            logger.warning("Metadata not found: %s", meta_path)
            self.metadata = [{"_id": i} for i in range(self.index.ntotal)]
        else:
            try:
                with open(meta_path, "rb") as f:
                    raw = pickle.load(f)
                self.metadata = raw if isinstance(raw, list) else [raw[k] for k in sorted(raw)]
                logger.info("✅ Metadata: %s  (%d entries)", meta_path, len(self.metadata))
                if len(self.metadata) != self.index.ntotal:
                    logger.warning("Metadata count (%d) != vectors (%d)", len(self.metadata), self.index.ntotal)
            except Exception as exc:
                logger.error("Failed to load metadata: %s", exc)
                self.metadata = [{"_id": i} for i in range(self.index.ntotal)]

        self._available = True

    def _raw_search(self, query: str, top_k: int, score_threshold: float) -> List[Dict[str, Any]]:
        query_vec = self.embedder.embed_one(query)
        k = min(top_k, self.index.ntotal)
        distances, ids = self.index.search(query_vec, k)

        results = []
        for rank, (dist, vec_id) in enumerate(zip(distances[0], ids[0])):
            if vec_id == -1:
                continue
            score = float(max(0.0, 1.0 - dist / 2.0))
            if score < score_threshold:
                continue
            entry = dict(self.metadata[vec_id]) if vec_id < len(self.metadata) else {"_id": int(vec_id)}
            entry["_score"] = round(score, 4)
            entry["_rank"]  = rank + 1
            results.append(entry)
        return results

    def search(
        self,
        query: str,
        top_k: int = VECTOR_TOP_K,
        score_threshold: float = VECTOR_DEFAULT_THRESHOLD,
    ) -> List[Dict[str, Any]]:
        """Search with adaptive threshold expansion."""
        if not self._available or self.index is None:
            return []

        results = self._raw_search(query, top_k, score_threshold)

        if len(results) < VECTOR_MIN_RESULTS and score_threshold > VECTOR_EXPAND_THRESHOLD:
            logger.info("[VectorStore] %d results @ %.2f → expanding to %.2f", len(results), score_threshold, VECTOR_EXPAND_THRESHOLD)
            expanded = self._raw_search(query, top_k, VECTOR_EXPAND_THRESHOLD)
            if len(expanded) > len(results):
                results = expanded

        if len(results) < VECTOR_MIN_RESULTS:
            floor = self._raw_search(query, top_k, 0.10)
            if len(floor) > len(results):
                results = floor

        results.sort(key=lambda r: r["_score"], reverse=True)
        for i, r in enumerate(results):
            r["_rank"] = i + 1

        logger.info("[VectorStore] %d results for: %.80s", len(results), query)
        return results

    def search_formatted(self, query: str, top_k: int = VECTOR_TOP_K, score_threshold: float = VECTOR_DEFAULT_THRESHOLD) -> str:
        results = self.search(query, top_k=top_k, score_threshold=score_threshold)
        if not results:
            return "No semantically similar records found in the vector store."
        lines = [f"Semantic Search Results for: '{query}'", "=" * 50]
        for r in results:
            lines.append(f"\n[Rank {r['_rank']}  Score: {r['_score']}]")
            for k, v in r.items():
                if not k.startswith("_"):
                    lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    @property
    def is_available(self) -> bool:
        return self._available

    @property
    def total_vectors(self) -> int:
        return self.index.ntotal if self.index else 0

    def list_indices(self) -> List[str]:
        return ["faiss_main"] if self._available else []

    def index_size(self, name: str = "faiss_main") -> int:
        return self.total_vectors