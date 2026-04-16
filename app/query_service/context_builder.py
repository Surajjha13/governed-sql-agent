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

def build_schema_graph(schema: SchemaResponse) -> Dict[str, List[Dict]]:
    """Build bidirectional adjacency graph from FK metadata."""
    graph = {}
    for table in schema.tables:
        graph.setdefault(table.table, [])
        for col in table.columns:
            if col.foreign_key:
                # FK format: "table.col" or "schema.table.col"
                parts = col.foreign_key.split(".")
                if len(parts) >= 2:
                    target_table = parts[-2]
                    target_col = parts[-1]
                    
                    # Forward edge
                    graph[table.table].append({
                        "table": target_table,
                        "from_table": table.table,
                        "from_col": col.name,
                        "to_col": target_col,
                        "type": "fk"
                    })
                    # Reverse edge
                    graph.setdefault(target_table, [])
                    graph[target_table].append({
                        "table": table.table,
                        "from_table": target_table,
                        "from_col": target_col,
                        "to_col": col.name,
                        "type": "rev_fk"
                    })
    return graph

def find_join_path(graph: Dict[str, List[Dict]], start_table: str, end_table: str) -> List[Dict]:
    """BFS to find shortest join path between two tables."""
    if start_table == end_table:
        return []
        
    from collections import deque
    queue = deque([(start_table, [])])
    visited = {start_table}
    
    while queue:
        current, path = queue.popleft()
        for edge in graph.get(current, []):
            nxt = edge["table"]
            # To avoid adding the target table over and over, check nxt
            if nxt == end_table:
                return path + [edge]
            if nxt not in visited:
                visited.add(nxt)
                queue.append((nxt, path + [edge]))
    return []

def get_tables_in_paths(graph: Dict[str, List[Dict]], initial_tables: Set[str]) -> tuple[Set[str], List[Dict]]:
    """Find a connected set of tables holding the initial ones using shortest paths, then expand 2 hops."""
    if not initial_tables:
        return set(), []
        
    tables_list = list(initial_tables)
    connected_tables = {tables_list[0]}
    all_path_edges = []
    
    # Track signatures to avoid duplicate edge objects
    edge_signatures = set()
    
    def add_edge_if_new(edge):
        # We uniquely identify an edge by its table and column pairs
        sig1 = (edge["from_table"], edge["from_col"], edge["table"], edge["to_col"])
        sig2 = (edge["table"], edge["to_col"], edge["from_table"], edge["from_col"])
        if sig1 not in edge_signatures:
            edge_signatures.add(sig1)
            edge_signatures.add(sig2)
            all_path_edges.append(edge)

    # 1. Connect initial tables using shortest paths
    for i in range(1, len(tables_list)):
        target = tables_list[i]
        if target in connected_tables:
            continue
            
        best_path = None
        for start in connected_tables:
            path = find_join_path(graph, start, target)
            if path and (best_path is None or len(path) < len(best_path)):
                best_path = path
                
        if best_path:
            for edge in best_path:
                add_edge_if_new(edge)
                connected_tables.add(edge["table"])
        else:
            connected_tables.add(target)
            
    # 2. Expand outwards by 2 hops to provide neighboring context (to catch implicitly referenced tables like 'rented')
    expanded_tables = set(connected_tables)
    from collections import deque
    queue = deque([(t, 0) for t in connected_tables])
    
    while queue:
        current, depth = queue.popleft()
        if depth >= 2:  # max_depth = 2
            continue
            
        for edge in graph.get(current, []):
            nxt = edge["table"]
            add_edge_if_new(edge)
            
            if nxt not in expanded_tables:
                expanded_tables.add(nxt)
                queue.append((nxt, depth + 1))
                
    return expanded_tables, all_path_edges

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

    # 2. Build graph and find paths connecting initial tables
    graph = build_schema_graph(schema)
    all_table_names, path_edges = get_tables_in_paths(graph, initial_names)
    
    selected_table_metas = [t for t in schema.tables if t.table in all_table_names]

    context_tables: List[str] = []
    context_columns: Dict[str, List[str]] = {}
    context_joins: List[str] = []
    
    # Extract structural join paths format
    for edge in path_edges:
        # Avoid duplicate join statements
        join_str = f"{edge['from_table']} -> {edge['table']} (ON {edge['from_table']}.{edge['from_col']} = {edge['table']}.{edge['to_col']})"
        reverse_join_str = f"{edge['table']} -> {edge['from_table']} (ON {edge['table']}.{edge['to_col']} = {edge['from_table']}.{edge['from_col']})"
        if join_str not in context_joins and reverse_join_str not in context_joins:
            context_joins.append(join_str)

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

    # 4. We already inferred joining paths from the graph.
    # Optional: we can add any direct 1-hop FKs between the final set of tables 
    # to give the LLM full visibility into relationships within the context bubble.
    for t_meta in selected_table_metas:
        for col in t_meta.columns:
            if col.foreign_key:
                parts = col.foreign_key.split(".")
                if len(parts) >= 2:
                    target_table = parts[-2]
                    target_col = parts[-1]
                    if target_table in context_tables and t_meta.table in context_tables:
                        join_str = f"{t_meta.table} -> {target_table} (ON {t_meta.table}.{col.name} = {target_table}.{target_col})"
                        reverse_join_str = f"{target_table} -> {t_meta.table} (ON {target_table}.{target_col} = {t_meta.table}.{col.name})"
                        if join_str not in context_joins and reverse_join_str not in context_joins:
                            context_joins.append(join_str)

    return {
        "tables": context_tables,
        "columns": context_columns,
        "joins": list(set(context_joins)),
    }
