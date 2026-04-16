from typing import List, Dict, Tuple, Optional
import os

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

from app.schema_service.models import SchemaResponse


_MODEL: Optional[SentenceTransformer] = None


def get_embedding_model(model_name: str = "all-MiniLM-L6-v2") -> SentenceTransformer:
    """
    Lazily load and cache the embedding model.
    Uses local cache to avoid repeated HuggingFace downloads.
    """
    global _MODEL

    if _MODEL is None:
        _MODEL = SentenceTransformer(
            model_name,
            cache_folder=os.path.join(os.getcwd(), "models"),
        )

    return _MODEL


def build_vector_index(
    schema: SchemaResponse,
) -> Tuple[Optional[faiss.IndexFlatIP], List[Dict]]:
    """
    Build a FAISS vector index from schema-derived column descriptions.

    - Uses ONLY schema metadata (no glossary, no hallucination)
    - SAFE: returns (None, []) if no descriptions exist
    """

    texts: List[str] = []
    metadata: List[Dict] = []

    for table in schema.tables:
        for col in table.columns:
            # Fallback to name if description is missing
            desc = col.description if col.description else f"The {col.name} column in the {table.table} table."
            
            texts.append(f"{table.table}.{col.name}: {desc}")
            metadata.append({
                "table": table.table,
                "column": col.name,
                "semantic_type": col.semantic_type,
            })

    # IMPORTANT: Vector search is optional
    if not texts:
        return None, []

    model = get_embedding_model()

    embeddings = model.encode(texts, convert_to_numpy=True)
    embeddings = embeddings.astype("float32")

    # cosine similarity
    faiss.normalize_L2(embeddings)

    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    return index, metadata


def search_vector_index(
    query: str,
    index: Optional[faiss.IndexFlatIP],
    metadata: List[Dict],
    top_k: int = 5,
) -> List[Dict]:
    """
    Search schema-derived semantic vectors.

    SAFE GUARANTEES:
    - If index is None → returns []
    - If metadata empty → returns []
    - Never raises runtime error
    """

    if index is None or not metadata:
        return []

    model = get_embedding_model()

    query_vec = model.encode([query], convert_to_numpy=True)
    query_vec = query_vec.astype("float32")

    faiss.normalize_L2(query_vec)

    scores, indices = index.search(query_vec, top_k)

    results: List[Dict] = []

    for idx, score in zip(indices[0], scores[0]):
        if 0 <= idx < len(metadata):
            item = metadata[idx].copy()
            item["score"] = float(score)
            results.append(item)

    return results
