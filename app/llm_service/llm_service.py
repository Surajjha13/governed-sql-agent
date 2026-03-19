import os
import re
import time
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

from app.schema_service.models import SchemaResponse
from app.query_service.prompt_builder import build_prompt
from app.llm_service.optimizer import optimize_sql

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# API Configuration
# System API Keys (from .env)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

# Default Models
DEFAULT_MODELS = {
    "groq": "llama-3.3-70b-versatile",
    "openai": "gpt-4o",
    "gemini": "gemini-1.5-pro",
    "anthropic": "claude-3-5-sonnet-20241022",
    "deepseek": "deepseek-chat"
}

STAR_VALIDATION_MESSAGE = (
    "For security and performance reasons, please specify the columns you need individually "
    "rather than requesting 'all' or '*'."
)


import app.app_state as app_state
from app.auth.user_manager import user_manager
from app.llm_service.exceptions import LLMRateLimitError, is_rate_limit

# Dynamic Model Cache
MODELS_CACHE = {} # {provider: (timestamp, [models])}
CACHE_EXPIRY = 3600 # 1 hour

class LLMError(Exception):
    """Custom exception for LLM-related errors."""
    pass

async def get_model_recommendations(provider: str, current_model: str, api_key: Optional[str] = None, base_url: Optional[str] = None) -> List[str]:
    """Provides alternative models when current one is rate-limited, fetching from API if possible."""
    provider = provider.lower()
    
    # Try to get dynamic models if API key is provided
    available_models = []
    if api_key:
        now = time.time()
        cache_hit = MODELS_CACHE.get(provider)
        
        if cache_hit and (now - cache_hit[0] < CACHE_EXPIRY):
            available_models = cache_hit[1]
        else:
            try:
                from app.llm_service.llm_adapters import get_adapter
                adapter = get_adapter(provider)
                available_models = await adapter.list_models(api_key, base_url)
                if available_models:
                    MODELS_CACHE[provider] = (now, available_models)
                    logger.info(f"Dynamically fetched {len(available_models)} models for {provider}")
            except Exception as e:
                logger.warning(f"Failed to fetch dynamic models for {provider}: {e}")

    # Fallback to hardcoded lists if dynamic fetching fails or isn't possible
    if not available_models:
        if provider == 'gemini':
            available_models = ['gemini-2.0-flash-exp', 'gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-1.0-pro']
        elif provider == 'groq':
            available_models = ['llama-3.3-70b-versatile', 'llama-3.1-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768', 'deepseek-r1-distill-llama-70b']
        elif provider == 'openai':
            available_models = ['gpt-4o', 'gpt-4o-mini', 'o1-mini', 'o1-preview']
        elif provider == 'anthropic':
            available_models = ['claude-3-5-sonnet-latest', 'claude-3-5-haiku-latest', 'claude-3-opus-latest']
        elif provider == 'deepseek':
            available_models = ['deepseek-chat', 'deepseek-reasoner']
        else:
            return []
    
    # Filter out current model and return top 3
    return [m for m in available_models if m != current_model][:3]

def _get_system_fallback_key(provider: str) -> Optional[str]:
    """Retrieves the system-wide default key for a given provider."""
    return {
        "groq": GROQ_API_KEY,
        "openai": OPENAI_API_KEY,
        "gemini": GEMINI_API_KEY,
        "anthropic": ANTHROPIC_API_KEY,
        "deepseek": DEEPSEEK_API_KEY
    }.get(provider.lower())

def _get_session_config(session_id: str) -> Dict[str, Any]:
    """Retrieves LLM configuration for the specific session with system fallback."""
    # 1. Check if it's a Solo session (stored in app_state)
    # FIX: Frontend saves as sess_... but query sends solo_user_sess_...
    lookup_sid = session_id
    if session_id.startswith("solo_user_") and session_id not in app_state.solo_session_llm_cache:
        # Try finding the raw session suffix
        short_sid = session_id[len("solo_user_"):]
        if short_sid in app_state.solo_session_llm_cache:
            lookup_sid = short_sid

    if lookup_sid in app_state.solo_session_llm_cache:
        solo_cfg = app_state.solo_session_llm_cache[lookup_sid]
        active = solo_cfg.get("active_provider", "groq")
        prov_cfg = solo_cfg.get("providers", {}).get(active, {})
        
        # Priority: 1. Solo session key (BYOK), 2. System ENV key
        api_key = prov_cfg.get("api_key") or _get_system_fallback_key(active)
        
        # Final fallback to Groq if the chosen provider has no key at all
        if not api_key and active != "groq":
            fallback_key = _get_system_fallback_key("groq")
            if fallback_key:
                logger.info(f"Solo session: Chosen provider {active} has no configuration; falling back to system Groq.")
                return {
                    "api_key": fallback_key,
                    "model": DEFAULT_MODELS["groq"],
                    "provider": "groq",
                    "base_url": None
                }

        return {
            "api_key": api_key,
            "model": prov_cfg.get("model") or DEFAULT_MODELS.get(active, "llama-3.3-70b-versatile"),
            "provider": active,
            "base_url": prov_cfg.get("base_url")
        }

    # 2. Regular User: Check their saved config in DB
    # We first need to know who the user is. Robust split to handle 'solo_user_...'
    if session_id.startswith("solo_user_"):
        username = "solo_user"
    else:
        username = session_id.split('_')[0] if "_" in session_id else "admin"
    
    # If it's a known solo user that somehow didn't have a cache entry, use system defaults directly
    if username == "solo_user":
        return {
            "api_key": GROQ_API_KEY, # Default fallback
            "model": DEFAULT_MODELS["groq"],
            "provider": "groq",
            "base_url": None
        }

    cfg = user_manager.get_llm_config(username)
    active = cfg.get("active_provider", "groq")
    prov_cfg = cfg.get("providers", {}).get(active, {})
    
    # Priority: 1. User saved key, 2. System ENV key for that specific provider
    api_key = prov_cfg.get("api_key") or _get_system_fallback_key(active)
    
    # Final fallback to Groq if the preferred provider has no key at all
    if not api_key and active != "groq":
        fallback_key = _get_system_fallback_key("groq")
        if fallback_key:
            logger.info(f"User {username}: Preferred provider {active} has no configuration; falling back to system Groq.")
            return {
                "api_key": fallback_key,
                "model": DEFAULT_MODELS["groq"],
                "provider": "groq",
                "base_url": None
            }

    return {
        "api_key": api_key,
        "model": prov_cfg.get("model") or DEFAULT_MODELS.get(active, "llama-3.3-70b-versatile"),
        "provider": active,
        "base_url": prov_cfg.get("base_url")
    }


def _classify_safe_retry_mode(question: str) -> Optional[str]:
    q = (question or "").lower()
    pattern_groups = {
        "aggregate": [
            "most common",
            "least common",
            "frequency",
            "distribution",
            "count by",
            "how many per",
            "how many by",
            "group by",
            "breakdown by",
            "per category",
            "per status",
            "for each",
        ],
        "ranking": [
            "top ",
            "bottom ",
            "highest",
            "lowest",
            "ranking",
            "rank ",
            "best",
            "worst",
        ],
        "trend": [
            "trend",
            "over time",
            "by month",
            "by year",
            "by day",
            "monthly",
            "yearly",
            "daily",
            "week over week",
            "month over month",
            "growth",
            "change over time",
        ],
        "metric": [
            "how many",
            "total ",
            "count ",
            "number of",
            "sum of",
            "average",
            "avg ",
            "mean ",
            "median",
        ],
    }

    for mode, patterns in pattern_groups.items():
        if any(pattern in q for pattern in patterns):
            return mode
    return None


def _build_safe_retry_instruction(mode: str) -> str:
    common_rules = (
        "- Use explicit columns only.\n"
        "- Never use SELECT *.\n"
        "- Quote table and column names exactly for Postgres.\n"
    )

    mode_specific = {
        "aggregate": (
            "- The question is asking for an aggregate or frequency-style answer.\n"
            "- Prefer COUNT(*) with GROUP BY on the relevant descriptive column.\n"
            "- Order by the aggregate descending when the question asks for 'most common' or ranking.\n"
        ),
        "ranking": (
            "- The question is asking for a ranking-style answer.\n"
            "- Select the descriptive label plus the metric used for ranking.\n"
            "- Use ORDER BY on the metric and apply LIMIT only if the user asked for top/bottom N.\n"
        ),
        "trend": (
            "- The question is asking for a trend over time.\n"
            "- Group by the correct time bucket and aggregate the requested metric.\n"
            "- Order results chronologically by the time column or derived time bucket.\n"
        ),
        "metric": (
            "- The question is asking for a metric or summary value.\n"
            "- Return only the required aggregate columns, such as COUNT(*), SUM(...), AVG(...), or MAX(...).\n"
            "- Do not include raw detail rows unless the user explicitly asked for them.\n"
        ),
    }

    return common_rules + mode_specific.get(mode, "")


def _format_context_for_star_repair(context: Dict, schema: SchemaResponse) -> str:
    table_map = {table.table: table for table in schema.tables}
    lines = []
    for table_name in context.get("tables", []):
        table = table_map.get(table_name)
        if not table:
            continue
        chosen_columns = context.get("columns", {}).get(table_name, [])
        column_descriptions = []
        for column in table.columns:
            if column.name in chosen_columns:
                column_descriptions.append(
                    f"\"{column.name}\" ({column.data_type}, semantic={column.semantic_type})"
                )
        if column_descriptions:
            lines.append(f'Table "{table_name}": ' + ", ".join(column_descriptions))
    joins = context.get("joins", [])
    if joins:
        lines.append("Suggested joins: " + "; ".join(joins))
    return "\n".join(lines) if lines else "No focused context available."


async def _repair_star_validation_sql(
    adapter,
    api_key: str,
    model_name: str,
    base_url: Optional[str],
    question: str,
    context: Dict,
    schema: SchemaResponse,
    retry_mode: str,
) -> Optional[str]:
    repair_prompt = f"""The previous SQL drafts incorrectly used SELECT * and were rejected.
Rewrite the query safely for the user's question.

Question: {question}
Detected analytical intent: {retry_mode}

Focused schema context:
{_format_context_for_star_repair(context, schema)}

Requirements:
{_build_safe_retry_instruction(retry_mode)}- Return one valid Postgres SELECT query in a markdown sql block.
- Use only tables and columns that appear in the focused schema context.
- If the question is about counts, most common values, trends, rankings, or totals, produce aggregated SQL rather than raw detail rows.
"""

    messages = [
        {
            "role": "system",
            "content": (
                "You are a SQL repair expert. Produce one safe, explicit-column Postgres SELECT query. "
                "Never use SELECT *."
            )
        },
        {
            "role": "user",
            "content": repair_prompt
        }
    ]

    content = await adapter.chat_completion(messages, api_key, model_name, base_url=base_url)
    if not content:
        return None
    return _extract_sql(content.strip())


async def generate_sql(
    question: str,
    context: Dict,
    schema: SchemaResponse,
    history: List[Dict] = None,
    vector_candidates: List[Dict] = None,
    session_id: str = "default"
) -> str:
    """
    Generate SQL query using session-specific or global LLM config.
    """
    
    cfg = _get_session_config(session_id)
    api_key = cfg["api_key"]
    model_name = cfg["model"]
    provider = cfg["provider"]
    base_url = cfg.get("base_url")

    # Check API key
    if not api_key:
        logger.error(f"No API key found for session {session_id}")
        raise LLMError("LLM API key not configured for this session.")
    
    # Build prompt
    prompt = build_prompt(question, context, schema, history, vector_candidates)
    logger.info(f"Generated prompt for LLM (length: {len(prompt)} chars)")
    logger.debug(f"Prompt content:\n{prompt}")
    
    attempts = [
        {
            "label": "primary",
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert SQL generator. Your output MUST be a single raw SQL query wrapped in markdown code blocks. Do not include any other text or explanations."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }
    ]

    retry_mode = _classify_safe_retry_mode(question)
    if retry_mode:
        retry_prompt = (
            f"{prompt}\n\n"
            "RETRY INSTRUCTION:\n"
            f"{_build_safe_retry_instruction(retry_mode)}"
        )
        attempts.append({
            "label": f"{retry_mode}_retry",
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an expert SQL generator. Return only raw SQL in markdown. "
                        "Use explicit columns, follow the user's analytical intent, and never use SELECT *."
                    )
                },
                {
                    "role": "user",
                    "content": retry_prompt
                }
            ]
        })

    # Attempt SQL generation
    try:
        from app.llm_service.llm_adapters import get_adapter
        adapter = get_adapter(provider)
        
        logger.info(f"Attempting SQL generation for session {session_id} using {provider}/{model_name}")
        last_validation_error = None
        for attempt in attempts:
            content = await adapter.chat_completion(
                attempt["messages"],
                api_key,
                model_name,
                base_url=base_url
            )

            if not content:
                logger.warning(f"{model_name} returned empty response on attempt '{attempt['label']}'")
                continue

            raw_content = content.strip()
            sql = _extract_sql(raw_content)
            sql = optimize_sql(sql, schema)

            # Check if LLM refused based on schema or policy
            if "I cannot answer" in sql or "you are not allowed to use this" in sql:
                logger.info(f"LLM correctly refused to generate SQL using {model_name}")
                return sql

            is_valid, error = is_valid_sql(sql)
            if is_valid:
                logger.info(
                    f"Successfully generated and validated SQL using {model_name} "
                    f"on attempt '{attempt['label']}'"
                )
                logger.debug(f"Generated SQL:\n{sql}")
                return sql

            last_validation_error = error
            logger.warning(
                f"Generated SQL failed validation on attempt '{attempt['label']}': {error}"
            )

            if error != STAR_VALIDATION_MESSAGE:
                raise LLMError(f"Validation Error: {error}")

        if retry_mode and last_validation_error == STAR_VALIDATION_MESSAGE:
            repaired_sql = await _repair_star_validation_sql(
                adapter=adapter,
                api_key=api_key,
                model_name=model_name,
                base_url=base_url,
                question=question,
                context=context,
                schema=schema,
                retry_mode=retry_mode,
            )
            if repaired_sql:
                repaired_sql = optimize_sql(repaired_sql, schema)
                is_valid, error = is_valid_sql(repaired_sql)
                if is_valid:
                    logger.info(
                        f"Successfully repaired SELECT * validation failure using {model_name} "
                        f"for retry mode '{retry_mode}'"
                    )
                    logger.debug(f"Repaired SQL:\n{repaired_sql}")
                    return repaired_sql
                logger.warning(f"Star-validation repair attempt failed validation: {error}")
                last_validation_error = error

        if last_validation_error:
            raise LLMError(f"Validation Error: {last_validation_error}")
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"SQL generation failed for session {session_id}: {error_msg}")
        
        if is_rate_limit(e):
            recs = await get_model_recommendations(provider, model_name, api_key, base_url)
            raise LLMRateLimitError(
                f"Rate limit reached: {error_msg}",
                recommendations=recs,
                provider=provider,
                model=model_name
            )
            
        raise LLMError(f"LLM Error: {error_msg}")


def _extract_sql(text: str) -> str:
    """
    Extract SQL from LLM response. 
    Handles:
    1. Markdown code blocks (takes the LAST one if multiple)
    2. Raw SQL text
    3. Leading/trailing conversational text
    """
    # Regex to find all markdown code blocks (sql or plain)
    code_blocks = re.findall(r"```(?:sql)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    
    if code_blocks:
        # Take the last one (models often correct themselves)
        sql = code_blocks[-1].strip()
    else:
        # Fallback: strip common phrases and whitespace
        sql = text.strip()
        # Remove common "Conversational" prefixes if the model included them outside blocks
        sql = re.sub(r"^(?:SQL Query|Generated SQL|Final SQL|Query|Reasoning):\s*", "", sql, flags=re.IGNORECASE)
        
        # If there's still a lot of text, try to find the start of a SELECT
        if len(sql.split()) > 50: # Arbitrary threshold for "too much text"
            select_match = re.search(r"\bSELECT\b.*", sql, re.DOTALL | re.IGNORECASE)
            if select_match:
                sql = select_match.group(0)

    # Basic cleanup: remove trailing semicolon if present to normalize
    sql = sql.rstrip(";").strip()
    
    return sql


import sqlglot
from sqlglot import exp, parse_one

async def repair_sql(
    question: str,
    error: str,
    prior_sql: str,
    context: Dict,
    schema: SchemaResponse,
    history: List[Dict] = None,
    session_id: str = "default"
) -> str:
    """
    Call LLM to repair SQL that failed execution using session config.
    """
    cfg = _get_session_config(session_id)
    if not cfg["api_key"]: return prior_sql
    
    from app.llm_service.llm_adapters import get_adapter
    adapter = get_adapter(cfg["provider"])
    
    repair_prompt = f"""The following SQL query was generated for the question: "{question}"
Query: {prior_sql}
Error: {error}

Please fix the SQL query to resolve the error. 
- Ensure you follow all schema rules.
- Return ONLY the corrected raw SQL in a markdown block.
- Double-quote all table and column names for Postgres.
- CASE SENSITIVITY CHECK: If the error mentions a column "does not exist", it is likely a capitalization issue. Check the schema and use the exact casing (e.g., "ProductID" vs "ProductId").
- Do not use reasoning or explanations.
"""

    try:
        messages = [
            {
                "role": "system",
                "content": "You are a SQL repair expert. Fix the provided SQL based on the error message. Output ONLY the raw SQL."
            },
            {
                "role": "user",
                "content": repair_prompt
            }
        ]
        
        content = await adapter.chat_completion(messages, cfg["api_key"], cfg["model"])
        if content:
            raw_content = content.strip()
            return _extract_sql(raw_content)
    except Exception as e:
        logger.error(f"SQL repair failed: {e}")
    
    return prior_sql # Return original if repair fails


def is_valid_sql(sql: str) -> tuple[bool, Optional[str]]:
    """
    Perform security and structural validation using sqlglot AST parsing.
    """
    if not sql:
        return False, "Empty SQL query"

    try:
        # 1. Parse SQL
        parsed = parse_one(sql, read="postgres")
        
        # 2. Must be a SELECT statement
        if not isinstance(parsed, exp.Select):
            return False, "Only SELECT operations are permitted."

        # 3. Check for forbidden actions/nodes
        # sqlglot makes it easy to find specific node types
        forbidden_nodes = (
            exp.Drop, exp.Delete, exp.Update, exp.Insert, exp.Create, 
            exp.Alter, exp.TruncateTable, exp.Command
        )
        for node in parsed.find_all(forbidden_nodes):
            return False, f"Unauthorized SQL operation detected: {type(node).__name__}"

        # 4. Block SELECT * and SELECT ALL query patterns
        if any(parsed.find_all(exp.Star)):
            return False, "For security and performance reasons, please specify the columns you need individually rather than requesting 'all' or '*'."

        # 5. Check for multiple statements
        # parse_one will usually handle one, but let's be safe
        if ";" in sql.strip().rstrip(";"):
            return False, "Multiple SQL statements are not permitted."

        return True, None

    except sqlglot.errors.ParseError as e:
        return False, f"SQL syntax error: {str(e)}"
    except Exception as e:
        return False, f"Validation error: {str(e)}"


async def extract_structured_memory(question: str, history: List[Dict] = None) -> Dict:
    """
    Extract structured entities, filters, and time ranges from the question.
    """
    # Use Groq for this internal task specifically if available, else fallback
    api_key = GROQ_API_KEY
    if not api_key: return {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}
    
    from app.llm_service.llm_adapters import GroqAdapter
    adapter = GroqAdapter()
    
    extraction_prompt = f"""Extract structured information from this user question: "{question}"
Recent History: {history[-3:] if history else "None"}

Return JSON only:
{{
  "tables": [],
  "metrics": [],
  "dimensions": [],
  "time_range": null,
  "filters": []
}}
"""

    try:
        messages = [
            {"role": "system", "content": "You are a data extraction expert. Return ONLY valid JSON."},
            {"role": "user", "content": extraction_prompt}
        ]
        
        content = await adapter.chat_completion(messages, api_key, DEFAULT_MODELS["groq"])
        if content:
            import json
            return json.loads(content)
    except Exception as e:
        logger.error(f"Structured memory extraction failed: {e}")
    
    return {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}


async def generate_summary(
    question: str,
    data: Any,
    session_id: str = "default"
) -> str:
    """
    Generate a natural language summary of the database results using session config.
    """
    from app.query_service.prompt_builder import build_summary_prompt, build_summary_prompt_compact

    cfg = _get_session_config(session_id)
    if not cfg["api_key"]:
        return "Insight summary unavailable (API key not configured)."
    
    from app.llm_service.llm_adapters import get_adapter
    adapter = get_adapter(cfg["provider"])
    
    attempts = [
        {
            "label": "primary",
            "model": cfg["model"],
            "prompt": build_summary_prompt(question, data),
            "system": "You are a professional Data Analyst. Your summaries are always concise, factual, and highlight key metrics with bolding. Do not use conversational filler."
        }
    ]

    fallback_model = DEFAULT_MODELS.get(cfg["provider"])
    if fallback_model and fallback_model != cfg["model"]:
        attempts.append({
            "label": "provider-default-fallback",
            "model": fallback_model,
            "prompt": build_summary_prompt_compact(question, data),
            "system": "You are a concise data analyst. Return a short factual summary with the key numbers only."
        })
    else:
        attempts.append({
            "label": "compact-retry",
            "model": cfg["model"],
            "prompt": build_summary_prompt_compact(question, data),
            "system": "You are a concise data analyst. Return a short factual summary with the key numbers only."
        })

    last_error = None
    for attempt in attempts:
        try:
            logger.info(
                f"Attempting summary generation for session {session_id} using "
                f"{cfg['provider']}/{attempt['model']} ({attempt['label']})"
            )

            messages = [
                {"role": "system", "content": attempt["system"]},
                {"role": "user", "content": attempt["prompt"]}
            ]

            content = await adapter.chat_completion(
                messages,
                cfg["api_key"],
                attempt["model"],
                base_url=cfg.get("base_url")
            )
            if content and content.strip():
                return content.strip()
        except Exception as e:
            last_error = e
            logger.warning(
                f"Summary generation attempt '{attempt['label']}' failed for session "
                f"{session_id}: {e}"
            )
            if is_rate_limit(e):
                recs = await get_model_recommendations(
                    cfg["provider"],
                    attempt["model"],
                    cfg["api_key"],
                    cfg.get("base_url")
                )
                raise LLMRateLimitError(
                    f"Rate limit reached: {str(e)}",
                    recommendations=recs,
                    provider=cfg["provider"],
                    model=attempt["model"]
                )

    if last_error:
        logger.warning(f"All summary generation attempts failed for session {session_id}: {last_error}")

    return "Failed to generate insight summary."


async def analyze_visualization_intent(question: str, session_id: str = "default") -> Dict[str, Any]:
    """
    Analyze the user's intent to suggest appropriate visualizations using session-specific LLM.
    """
    try:
        cfg = _get_session_config(session_id)
        if not cfg["api_key"]:
            logger.warning(f"No API key available for intent analysis in session {session_id}, using fallback")
            return _fallback_intent_analysis(question)

        from app.llm_service.llm_adapters import get_adapter
        adapter = get_adapter(cfg["provider"])
        
        from app.query_service.prompt_builder import build_intent_prompt
        intent_prompt = build_intent_prompt(question)

        messages = [
            {"role": "system", "content": "You are a data visualization expert. Analyze queries and suggest appropriate chart types. Return ONLY valid JSON."},
            {"role": "user", "content": intent_prompt}
        ]
        
        content = await adapter.chat_completion(messages, cfg["api_key"], cfg["model"], base_url=cfg.get("base_url"))
        if content:
            import json
            result = json.loads(content)
            logger.info(f"Intent analysis for session {session_id}: {result.get('intent')} ({result.get('confidence', 0):.0%} confidence)")
            return result
            
    except Exception as e:
        logger.warning(f"LLM intent analysis failed for session {session_id}: {e}, using fallback")
    
    return _fallback_intent_analysis(question)


def _fallback_intent_analysis(question: str) -> Dict[str, Any]:
    """Fallback rule-based intent analysis when LLM is unavailable."""
    q_lower = question.lower()
    
    # Trend keywords
    if any(kw in q_lower for kw in ['trend', 'over time', 'monthly', 'daily', 'yearly', 'growth', 'change']):
        return {
            "intent": "trend_analysis",
            "keywords": ["trend", "time"],
            "suggested_chart_types": ["line", "area"],
            "confidence": 0.8
        }
    
    # Distribution keywords
    if any(kw in q_lower for kw in ['distribution', 'histogram', 'frequency', 'range', 'spread']):
        return {
            "intent": "distribution",
            "keywords": ["distribution"],
            "suggested_chart_types": ["histogram", "box"],
            "confidence": 0.8
        }
    
    # Comparison keywords
    if any(kw in q_lower for kw in ['compare', 'top', 'bottom', 'highest', 'lowest', 'ranking', 'by category']):
        return {
            "intent": "comparison",
            "keywords": ["compare"],
            "suggested_chart_types": ["bar", "column"],
            "confidence": 0.75
        }
    
    # Composition keywords
    if any(kw in q_lower for kw in ['breakdown', 'composition', 'percentage', 'share', 'proportion']):
        return {
            "intent": "composition",
            "keywords": ["breakdown"],
            "suggested_chart_types": ["pie", "bar"],
            "confidence": 0.75
        }
    
    # Correlation keywords
    if any(kw in q_lower for kw in ['relationship', 'correlation', 'vs', 'versus', 'against']):
        return {
            "intent": "correlation",
            "keywords": ["relationship"],
            "suggested_chart_types": ["scatter"],
            "confidence": 0.75
        }
    
    # Default: detail/table
    return {
        "intent": "detail",
        "keywords": [],
        "suggested_chart_types": ["table"],
        "confidence": 0.5
    }
