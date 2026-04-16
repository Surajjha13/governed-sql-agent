import re
from typing import Dict, Any, List
from app.schema_service.models import SchemaResponse

# --- TEMPLATES ---

SQL_GENERATION_TEMPLATE = """You are a highly secure, read-only SQL expert for PostgreSQL.
Convert user questions into standard SQL for the provided schema.

CONVERSATION HISTORY:
{history}

DATABASE SCHEMA:
{schema}

JOIN PATH:
{join_path}

BUSINESS METRICS & VECTOR CONTEXT:
{metrics}
{vector_context}

CRITICAL RULES:
1. USE EXACT TABLE NAMES from the schema. Do not pluralize or guess.
2. SELECT ONLY. Forbidden: INSERT, UPDATE, DELETE, DROP, ALTER, GRANT.
3. QUOTING: Double-quote ALL table and column names (e.g., "Users"."UserID").
4. LIMITS: Do NOT use LIMIT/TOP unless the user asks for a specific count (e.g., "Top 5").
5. READABILITY: When querying entities (users, products), you MUST join to get human-readable names; never return raw IDs alone.
6. NO SELECT *: Specify columns individually. For "everything" requests, pick 5-7 descriptive columns.
7. AGGREGATES: For "how many", "most common", etc., use GROUP BY, COUNT(*), and ORDER BY.
8. If the request is analytically ambiguous but the schema supports a reasonable interpretation, make the best safe assumption and generate SQL anyway. For example, phrases like "suddenly increased", "spike", or "dropped sharply" should be interpreted using available date/time and amount columns, such as comparing recent periods or ranking period-over-period change.
9. Only refuse with "I cannot answer this question based on the available data" when the schema truly lacks the fields needed to answer even approximately.

OUTPUT FORMAT:
### Reasoning
- Tables: <tables>
- Logic: <brief explanation>
### SQL
```sql
<query>
```

Question: {question}
Response:"""

MYSQL_SQL_GENERATION_TEMPLATE = """You are a highly secure, read-only SQL expert for MySQL.
Convert user questions into standard SQL for the provided schema.

CONVERSATION HISTORY:
{history}

DATABASE SCHEMA:
{schema}

JOIN PATH:
{join_path}

BUSINESS METRICS & VECTOR CONTEXT:
{metrics}
{vector_context}

CRITICAL RULES:
1. USE EXACT TABLE NAMES from the schema.
2. SELECT ONLY. Forbidden: INSERT, UPDATE, DELETE, DROP, ALTER, GRANT.
3. QUOTING: Use backticks (`) for ALL table and column names (e.g., `Users`.`UserID`).
4. LIMITS: Do NOT use LIMIT unless the user asks for a specific count.
5. READABILITY: Join to get human-readable names; never return raw IDs alone.
6. NO SELECT *: Specify columns individually.
7. AGGREGATES: For "most common", etc., use GROUP BY, COUNT(*), and ORDER BY.
8. If the request is analytically ambiguous but the schema supports a reasonable interpretation, make the best safe assumption and generate SQL anyway. For example, phrases like "suddenly increased", "spike", or "dropped sharply" should be interpreted using available date/time and amount columns, such as comparing recent periods or ranking period-over-period change.
9. Only refuse with "I cannot answer this question based on the available data" when the schema truly lacks the fields needed to answer even approximately.

OUTPUT FORMAT:
### Reasoning
- Tables: <tables>
- Logic: <explanation>
### SQL
```sql
<query>
```

Question: {question}
Response:"""


SUMMARY_TEMPLATE = """You are a sharp business analyst delivering an executive-level insight.
The user asked: "{question}"
Total records matched: {total_records}
Preview data (up to 10 rows):
{data}

Respond with a SHORT, BUSINESS-FOCUSED insight following these rules:
- **Lead with the "so what"**: Open with the most important business takeaway, not a description of what the data contains.
- **Be specific**: **Bold** every key number, name, date, or percentage that matters.
- **Surface the top finding**: Call out the clear winner, outlier, trend, or risk in 1 sentence.
- **Actionable where possible**: If the data implies an action or decision, state it in plain language.
- **Max 60 words total.** No fluff, no row-count commentary, no mention of "preview" or "records returned".
- Do NOT start with "The data shows", "Based on the results", or similar filler phrases."""

EXPLAIN_TEMPLATE = """You are a technical translator.
The user asked: "{question}"
And the system structurally produced this exact SQL to solve it:
```sql
{sql}
```

Task:
Explain to a non-technical user EXACTLY how this SQL calculates their answer in 1 to 2 plain English sentences.
Do NOT use technical words like "JOIN", "GROUP BY", "CTE", "AS", or "LEFT OUTER".
Start directly without any introductory conversational filler like "This query calculates...". Focus purely on the data logic (e.g., "To find this, we checked all inventory items, filtered out the broken ones, and added up their total cost.")"""


def build_prompt(
    question: str,
    context: Dict[str, Any],
    full_schema: SchemaResponse,
    history: List[Dict[str, str]] = None,
    vector_candidates: List[Dict] = None,
    intent_pattern: str = "Standard logic applies.",
    engine: str = "postgres"
) -> str:
    """
    Generate a high-quality prompt for the LLM using retrieved schema context, 
    chat history, vector candidates, and business metrics.
    """
    
    # 1. Format history
    history_str = "No previous context."
    if history:
        history_lines = []
        for msg in history[-3:]: # Keep last 3 turns
            history_lines.append(f"User: {msg['user']}")
            history_lines.append(f"Assistant: {msg['assistant']}")
        history_str = "\n".join(history_lines)

    # 2. Format schema based on context
    is_mysql = engine.lower() == "mysql"
    q = "`" if is_mysql else "\""
    
    selected_tables = [t for t in full_schema.tables if t.table in context["tables"]]
    
    schema_lines = []
    for table in selected_tables:
        schema_lines.append(f"Table: {q}{table.table}{q}")
        context_cols = context["columns"].get(table.table, [])
        
        schema_lines.append("Columns:")
        for col in table.columns:
            if col.name in context_cols:
                pk_mark = " (PK)" if col.is_primary_key else ""
                fk_mark = f" (FK -> {col.foreign_key})" if col.foreign_key else ""
                desc = f" - {col.description}" if col.description else ""
                schema_lines.append(f"  - {q}{col.name}{q} ({col.data_type}){pk_mark}{fk_mark}{desc}")
        schema_lines.append("")

    schema_str = "\n".join(schema_lines)
    
    join_path_str = "No specific join path identified."
    if context.get("joins"):
        join_path_str = "\n".join(f"- {join}" for join in context["joins"])

    # 3. Format Metrics
    metrics_lines = []
    for m in full_schema.metrics:
        metrics_lines.append(f"- {m.name}: {m.formula} ({m.description or 'No desc'})")
    
    if not metrics_lines:
        metrics_lines.append("No predefined business metrics.")
    
    metrics_str = "\n".join(metrics_lines)

    # 4. Format Vector Context
    vector_lines = []
    if vector_candidates:
        for hit in vector_candidates[:5]:
            if hit.get("column"):
                vector_lines.append(f"- Term relates to: {hit['table']}.{hit['column']}")
            else:
                vector_lines.append(f"- Term relates to table: {hit['table']}")
    
    vector_str = "\n".join(vector_lines) if vector_lines else "No semantic hints available."

    template = MYSQL_SQL_GENERATION_TEMPLATE if is_mysql else SQL_GENERATION_TEMPLATE

    return template.format(
        history=history_str,
        schema=schema_str,
        join_path=join_path_str,
        metrics=metrics_str,
        vector_context=vector_str,
        intent_pattern=intent_pattern,
        question=_scrub_pii(question)
    )


def _scrub_pii(text: str) -> str:
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}[-.]?\d{2}[-.]?\d{4}\b', '[REDACTED_SSN]', text)
    text = re.sub(r'\b(?:\+?1[-. ]?)?\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})\b', '[REDACTED_PHONE]', text)
    return text


def build_summary_prompt(question: str, data: Any, total_count: int = None) -> str:
    """
    Build a prompt for summarizing the database results.
    """
    # Truncate results if too large to avoid token limits
    data_preview = str(data)[:2000] if data else "No data returned."
    data_preview = _scrub_pii(data_preview)
    
    total_str = f"{total_count}" if total_count is not None else "some"
    
    return SUMMARY_TEMPLATE.format(
        question=_scrub_pii(question),
        total_records=total_str,
        data=data_preview
    )


def build_summary_prompt_compact(question: str, data: Any, total_count: int = None) -> str:
    """
    Build a smaller, compatibility-friendly summary prompt for providers/models
    that are sensitive to prompt size or formatting.
    """
    data_preview = str(data)[:1000] if data else "No data returned."
    data_preview = _scrub_pii(data_preview)
    total_str = f" ({total_count} total records)" if total_count is not None else ""
    return (
        f'Question: "{_scrub_pii(question)}"\n'
        f"Data{total_str}: {data_preview}\n\n"
        "In under 60 words, deliver the key BUSINESS INSIGHT from this data. "
        "Lead with the most important finding. Bold key numbers. "
        "No fluff, no mention of rows or previews."
    )


def build_explain_prompt(question: str, sql: str) -> str:
    """
    Build a prompt forcing the LLM to explain the SQL query in plain English.
    """
    return EXPLAIN_TEMPLATE.format(
        question=_scrub_pii(question),
        sql=sql.strip()
    )


INTENT_ANALYSIS_TEMPLATE = """You are a data visualization expert.
Analyze the following user question and suggest the most appropriate visualization intent.

Question: "{question}"

Your output MUST be a valid JSON object with the following fields:
- "intent": One of "trend_analysis", "comparison", "composition", "correlation", "distribution", "metric", or "detail".
- "suggested_chart_types": A list of suggested chart types (e.g., ["line", "area"], ["bar"], ["pie"], ["scatter"], ["histogram"], ["kpi"], ["table"]).
- "confidence": A float between 0 and 1 representing your confidence in this recommendation.
- "reason": A brief explanation for your recommendation.

RULES:
1. If the question asks for a single number, count, or total (e.g., "how many", "total number of"), use intent "metric" and suggested_chart_types ["kpi"].
2. If the question involves time-based trends, use "trend_analysis" and ["line", "area"].
3. If the question compares categories, use "comparison" and ["bar"].
4. If the question asks for a breakdown or share, use "composition" and ["pie"].
5. If the question involves relationship between variables, use "correlation" and ["scatter"].
6. If the question asks for distribution or frequency, use "distribution" and ["histogram"].
7. If none fit or it's a general request for data, use "detail" and ["table"].

Return ONLY the JSON object.
"""

def build_intent_prompt(question: str) -> str:
    """
    Build a prompt for analyzing the user's visualization intent.
    """
    return INTENT_ANALYSIS_TEMPLATE.format(question=_scrub_pii(question))
