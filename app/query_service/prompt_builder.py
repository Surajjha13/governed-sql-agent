
from typing import Dict, Any, List
from app.schema_service.models import SchemaResponse

# --- TEMPLATES ---

SQL_GENERATION_TEMPLATE = """You are a highly secure, read-only SQL expert.
Your task is to convert the user's natural language question into a standard SQL query for the provided schema.

CONVERSATION HISTORY:
{history}

DATABASE SCHEMA:
{schema}

BUSINESS METRICS (Use these canonical definitions if applicable):
{metrics}

SEMANTIC SEARCH HITS (Relevant columns/terms found via vector search):
{vector_context}

RULES:
1. Generate ONLY the raw SQL wrapped in Markdown code blocks (```sql ... ```).
2. LIMIT the results to 10 rows automatically if not specified.
3. ALLOWED: SELECT.
4. FORBIDDEN: INSERT, UPDATE, DELETE, DROP, ALTER, GRANT.
5. If the user asks for something not in the schema, answer "I cannot answer this question based on the available data".
6. CRITICAL: Postgres is case-sensitive. You MUST double-quote ALL table names and column names exactly as they appear in the schema (e.g. "Users"."UserID", not Users.UserID).
7. FORBIDDEN: Any request for "all" columns, "everything", or "select *". If the user asks for all data or uses a "select *" pattern, you MUST NOT expand it. Instead, you MUST answer: "For security and performance reasons, please specify the columns you need individually rather than requesting 'all' or '*'."
8. PERFORMANCE: 
   - Use window functions (ROW_NUMBER(), RANK()) for "Top N per group" or "running totals".
   - Use CTEs (WITH) for complex logic reuse or pre-aggregation.
   - Use subqueries to push down filters before joins when it reduces row counts.
   - Prefer indexed keys for joins and filters.
9. FORBIDDEN: Select * and select all. If the user asks for these, explain that it is for security and performance reasons.
10. FORBIDDEN: Session-level or management commands such as CONNECT, SET, USE, or BEGIN. The database connection is already established. Do not attempt to specify the database names or search paths.
11. READABILITY: When the user asks about entities (e.g. categories, products, customers), you MUST prioritize selecting and grouping by descriptive "Name" columns (e.g. "CategoryName", "ProductName") rather than just ID columns. Join the necessary tables to retrieve these descriptive attributes.
12. AGGREGATIONS: For questions like "most common", "least common", "frequency", "distribution", "count by", or "how many per", you MUST generate an aggregate query using explicit columns, GROUP BY, COUNT(*), and ORDER BY the aggregate. Never use SELECT * for these questions.

Question: {question}

SQL Query:"""

SUMMARY_TEMPLATE = """You are a highly efficient Data Analyst. 
The user asked: "{question}"
The database returned this data:
{data}

Provide a concise, high-impact summary of the results:
- **Accuracy First**: Directly address the user's question with specific numbers from the data.
- **Bolding**: You MUST **bold** all specific numbers, percentages, dates, and the most important insights.
- **Conciseness**: Keep the response to 2-3 short, impactful paragraphs. Max 150 words.
- **No Fluff**: Avoid generic intro phrases.
- **Insightful**: Point out the single most important trend or "winner" clearly."""


def build_prompt(
    question: str,
    context: Dict[str, Any],
    full_schema: SchemaResponse,
    history: List[Dict[str, str]] = None,
    vector_candidates: List[Dict] = None
) -> str:
    """
    Generate a high-quality prompt for the LLM using retrieved schema context, 
    chat history, vector candidates, and business metrics.
    """
    
    # 1. Format history
    history_str = "No previous context."
    if history:
        history_lines = []
        for msg in history[-5:]: # Keep last 5 turns
            history_lines.append(f"User: {msg['user']}")
            history_lines.append(f"Assistant: {msg['assistant']}")
        history_str = "\n".join(history_lines)

    # 2. Format schema based on context
    selected_tables = [t for t in full_schema.tables if t.table in context["tables"]]
    
    schema_lines = []
    for table in selected_tables:
        schema_lines.append(f"Table: \"{table.table}\"")
        context_cols = context["columns"].get(table.table, [])
        
        schema_lines.append("Columns:")
        for col in table.columns:
            if col.name in context_cols:
                pk_mark = " (PK)" if col.is_primary_key else ""
                fk_mark = f" (FK -> {col.foreign_key})" if col.foreign_key else ""
                desc = f" - {col.description}" if col.description else ""
                schema_lines.append(f"  - \"{col.name}\" ({col.data_type}){pk_mark}{fk_mark}{desc}")
        schema_lines.append("")

    if context.get("joins"):
        schema_lines.append("Suggested Joins:")
        for join in context["joins"]:
            schema_lines.append(f"- {join}")
        schema_lines.append("")

    schema_str = "\n".join(schema_lines)

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

    return SQL_GENERATION_TEMPLATE.format(
        history=history_str,
        schema=schema_str,
        metrics=metrics_str,
        vector_context=vector_str,
        question=question
    )


def build_summary_prompt(question: str, data: Any) -> str:
    """
    Build a prompt for summarizing the database results.
    """
    # Truncate results if too large to avoid token limits
    data_preview = str(data)[:2000] if data else "No data returned."
    
    return SUMMARY_TEMPLATE.format(
        question=question,
        data=data_preview
    )


def build_summary_prompt_compact(question: str, data: Any) -> str:
    """
    Build a smaller, compatibility-friendly summary prompt for providers/models
    that are sensitive to prompt size or formatting.
    """
    data_preview = str(data)[:1000] if data else "No data returned."
    return (
        f'Question: "{question}"\n'
        f"Data: {data_preview}\n\n"
        "Summarize the result in 2-3 short factual sentences. "
        "Mention the top finding and include important numbers."
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
    return INTENT_ANALYSIS_TEMPLATE.format(question=question)
