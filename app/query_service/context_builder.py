from typing import Dict, List, Set
from app.schema_service.models import SchemaResponse, TableMeta, ColumnMeta

def tokenize(text: str) -> Set[str]:
    """
    Normalize text into lowercase tokens and handle basic pluralization.
    """
    tokens = set(text.lower().replace("_", " ").split())
    stemmed = set()
    for t in tokens:
        stemmed.add(t)
        if t.endswith("ies") and len(t) > 3:
            stemmed.add(t[:-3] + "y")
        elif t.endswith("s") and len(t) > 1:
            stemmed.add(t[:-1])
    return stemmed

def column_score(column: ColumnMeta, question_tokens: Set[str], vector_hits: List[Dict] = None) -> int:
    """
    Heuristic relevance score for a column.
    """
    score = 0
    name_lower = column.name.lower()
    col_tokens = tokenize(column.name)

    # 1. Lexical relevance
    if col_tokens & question_tokens:
        score += 3

    # 2. Vector relevance
    if vector_hits:
        for hit in vector_hits:
            if hit.get("column") == column.name and hit.get("score"):
                score += int(hit["score"] * 10)

    descriptive_keywords = {"name", "title", "label", "description", "desc"}
    if any(k in name_lower for k in descriptive_keywords):
        score += 1

    if column.semantic_type == "metric":
        score += 2
    elif column.semantic_type == "time":
        score += 2
    elif column.semantic_type == "foreign_key":
        score += 1

    return score

def table_score(table: TableMeta, question_tokens: Set[str], vector_hits: List[Dict] = None) -> int:
    """
    Heuristic relevance score for a table.
    """
    score = 0
    table_tokens = tokenize(table.table)

    if table_tokens & question_tokens:
        score += 3

    if vector_hits:
        table_hits = [h for h in vector_hits if h.get("table") == table.table]
        if table_hits:
            max_vec_score = max(h.get("score", 0) for h in table_hits)
            score += int(max_vec_score * 8)

    if any(c.semantic_type == "metric" for c in table.columns):
        score += 1

    return score

def expand_foreign_key_tables(initial_table_names: Set[str], schema: SchemaResponse, max_depth: int = 2) -> Set[str]:
    """
    Find all tables related to the selected tables through foreign keys,
    including multi-hop relationships up to max_depth.
    """
    expanded = set(initial_table_names)
    table_meta_map = {t.table: t for t in schema.tables}

    for _ in range(max_depth):
        new_tables = set()
        for t_name in expanded:
            table_meta = table_meta_map.get(t_name)
            if not table_meta: continue

            # Forward FKs
            for col in table_meta.columns:
                if col.foreign_key:
                    target_table = col.foreign_key.split('.')[0]
                    if target_table not in expanded:
                        new_tables.add(target_table)

            # Reverse FKs
            for other_name, other_meta in table_meta_map.items():
                if other_name in expanded: continue
                for other_col in other_meta.columns:
                    if other_col.foreign_key and other_col.foreign_key.split('.')[0] == t_name:
                        new_tables.add(other_name)
                        break
        
        if not new_tables: break
        expanded.update(new_tables)

    return expanded

def build_context(
    question: str,
    schema: SchemaResponse,
    vector_candidates: List[Dict] = None,
    history: List[Dict] = None,
    max_tables: int = 6,
    max_columns_per_table: int = 10,
) -> Dict:
    """
    Build a minimal, schema-safe context for NL → SQL generation.
    Supports hybrid retrieval (lexical + vector + multi-hop FK).
    Now includes conversational memory support by expanding tokens from history.
    """
    # 0. Expand question tokens with history for conversational context
    contextual_text = question
    if history:
        # Take tokens from last 2 user messages for context persistence
        recent_user_queries = [h['user'] for h in history[-2:]]
        contextual_text += " " + " ".join(recent_user_queries)
    
    question_tokens = tokenize(contextual_text)

    # 1. Score and select initial tables
    scored_tables = []
    for table in schema.tables:
        score = table_score(table, question_tokens, vector_hits=vector_candidates)
        scored_tables.append((score, table.table))

    scored_tables.sort(key=lambda x: x[0], reverse=True)
    initial_names = {name for score, name in scored_tables[:max_tables] if score > 0}

    # 2. Expand via FKs (Multi-hop)
    all_table_names = expand_foreign_key_tables(initial_names, schema)
    selected_table_metas = [t for t in schema.tables if t.table in all_table_names]

    context_tables: List[str] = []
    context_columns: Dict[str, List[str]] = {}
    context_joins: List[str] = []

    # 3. Select columns per table
    for table in selected_table_metas:
        scored_columns = []
        for col in table.columns:
            score = column_score(col, question_tokens, vector_hits=vector_candidates)
            if score >= 0: # Include even 0-score if necessary
                scored_columns.append((score, col))

        scored_columns.sort(key=lambda x: x[0], reverse=True)
        chosen_cols = [col.name for _, col in scored_columns[:max_columns_per_table]]

        # Ensure PK
        for col in table.columns:
            if col.is_primary_key and col.name not in chosen_cols:
                chosen_cols.insert(0, col.name)
        
        # Ensure at least one descriptive column is included (human-readable)
        descriptive_keywords = {"name", "title", "label", "description", "desc"}
        has_descriptive = any(any(k in c.lower() for k in descriptive_keywords) for c in chosen_cols)
        
        if not has_descriptive:
            for col in table.columns:
                if any(k in col.name.lower() for k in descriptive_keywords):
                    chosen_cols.append(col.name)
                    break # Just one is usually enough for context

        # Ensure FK columns are included if target table is in context
        for col in table.columns:
            if col.foreign_key:
                target_table = col.foreign_key.split('.')[0]
                if target_table in all_table_names and col.name not in chosen_cols:
                    chosen_cols.append(col.name)

        context_tables.append(table.table)
        context_columns[table.table] = chosen_cols

    # 4. Infer joins
    for t_meta in selected_table_metas:
        for col in t_meta.columns:
            if col.foreign_key:
                target_table = col.foreign_key.split(".")[0]
                if target_table in context_tables and t_meta.table in context_tables:
                    context_joins.append(f"{t_meta.table}.{col.name} -> {col.foreign_key}")

    return {
        "tables": context_tables,
        "columns": context_columns,
        "joins": list(set(context_joins)),
    }
