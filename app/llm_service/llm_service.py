import os
import re
import time
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

from app.schema_service.models import SchemaResponse
from app.query_service.prompt_builder import build_prompt, _scrub_pii
from app.llm_service.optimizer import optimize_sql
from app.llm_service.security import redact_history_for_llm, redact_results_for_summary, redact_schema, redact_vector_candidates

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

from collections import OrderedDict
import time

class TTLCache:
    def __init__(self, maxsize=500, ttl=300):
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl

    def get(self, key):
        if key in self.cache:
            val, expiry = self.cache[key]
            if time.time() > expiry:
                del self.cache[key]
                return None
            self.cache.move_to_end(key)
            return val
        return None

    def set(self, key, val):
        if key in self.cache:
            del self.cache[key]
        self.cache[key] = (val, time.time() + self.ttl)
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)

SQL_CACHE = TTLCache(maxsize=1000, ttl=600) # Increased capacity and TTL (10m)

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
        # Robustly extract username by removing known session suffixes
        # Suffix format from frontend is typically `_sess_<alphanumeric>`, `_default` or `_test_session`
        username = re.sub(r'_(sess_[a-z0-9]+|default|test_session)$', '', session_id, flags=re.IGNORECASE)
        if not username or username == session_id:
            # Fallback if no matching suffix was found
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


def _build_safe_retry_instruction(mode: str, engine: str = "postgres") -> str:
    identifier_guidance = (
        "Use backticks for MySQL identifiers."
        if engine.lower() == "mysql"
        else "Quote table and column names exactly for Postgres."
    )
    common_rules = (
        "- Use explicit columns only.\n"
        "- Never use SELECT *.\n"
        f"- {identifier_guidance}\n"
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


def _is_llm_refusal(text: str) -> bool:
    normalized = (text or "").lower()
    refusal_markers = [
        "i cannot answer",
        "i cannot fulfill",
        "cannot answer this question based on the available data",
        "you are not allowed to use this",
    ]
    return any(marker in normalized for marker in refusal_markers)


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
    engine: str = "postgres"
) -> Optional[str]:
    is_mysql = engine.lower() == "mysql"
    db_type = "MySQL" if is_mysql else "Postgres"
    q = "`" if is_mysql else "\""
    
    repair_prompt = f"""The previous SQL drafts incorrectly used SELECT * and were rejected.
Rewrite the query safely for the user's question.

Question: {question}
Detected analytical intent: {retry_mode}

Focused schema context:
{_format_context_for_star_repair(context, schema)}

Requirements:
{_build_safe_retry_instruction(retry_mode, engine=engine)}- Return one valid {db_type} SELECT query in a markdown sql block.
- Use only tables and columns that appear in the focused schema context.
- If the question is about counts, most common values, trends, rankings, or totals, produce aggregated SQL rather than raw detail rows.
- Use {q} identifiers for all table and column names exactly as they appear in the schema.
"""

    messages = [
        {
            "role": "system",
            "content": (
                f"You are a SQL repair expert. Produce one safe, explicit-column {db_type} SELECT query. "
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


async def _repair_invalid_joins_sql(
    adapter,
    api_key: str,
    model_name: str,
    base_url: Optional[str],
    question: str,
    context: Dict,
    schema: SchemaResponse,
    join_error: str,
    engine: str = "postgres"
) -> Optional[str]:
    is_mysql = engine.lower() == "mysql"
    db_type = "MySQL" if is_mysql else "Postgres"
    q = "`" if is_mysql else "\""
    
    join_path_str = "No specific join path identified."
    if context.get("joins"):
        join_path_str = "\n".join(f"- {join}" for join in context["joins"])

    repair_prompt = f"""The previous SQL generated contained invalid joins according to the schema.
Error: {join_error}

Rewrite the query safely for the user's question, ensuring you ONLY join tables that have a direct foreign key relationship.

Question: {question}

Valid Join Path to follow:
{join_path_str}

Focused schema context:
{_format_context_for_star_repair(context, schema)}

Requirements:
- Return one valid {db_type} SELECT query in a markdown sql block.
- You MUST ONLY join tables using the relationships listed under "Valid Join Path". Do NOT invent joins.
- If two tables are not directly related, you MUST use the intermediate bridge table(s).
- Use {q} identifiers for all table and column names exactly as they appear in the schema.
"""

    messages = [
        {
            "role": "system",
            "content": f"You are a SQL repair expert. Produce one strict {db_type} SELECT query following only the provided schema foreign keys."
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


async def _repair_semantic_intent_sql(
    adapter,
    api_key: str,
    model_name: str,
    base_url: Optional[str],
    question: str,
    context: Dict,
    schema: SchemaResponse,
    intent_error: str,
    engine: str = "postgres"
) -> Optional[str]:
    is_mysql = engine.lower() == "mysql"
    db_type = "MySQL" if is_mysql else "Postgres"
    q = "`" if is_mysql else "\""
    
    join_path_str = "No specific join path identified."
    if context.get("joins"):
        join_path_str = "\n".join(f"- {join}" for join in context["joins"])

    repair_prompt = f"""The previous SQL generated failed the Result Sanity Check for logical semantic intent.
Error: {intent_error}

You MUST rewrite the query safely for the user's question, strictly fulfilling the required semantic logic (e.g. existence or absence pattern).

Question: {question}

Valid Join Path to follow:
{join_path_str}

Requirements:
- Return one valid {db_type} SELECT query in a markdown sql block.
- You MUST correctly implement the required logical operator pattern to fulfill the classification.
- Use {q} identifiers for all table and column names exactly as they appear in the schema.
"""

    messages = [
        {
            "role": "system",
            "content": f"You are a SQL repair expert. Produce one strict {db_type} SELECT query fixing the logical semantic failure."
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


async def _repair_aggregation_sql(
    adapter,
    api_key: str,
    model_name: str,
    base_url: Optional[str],
    question: str,
    schema: SchemaResponse,
    agg_error: str,
    engine: str = "postgres"
) -> Optional[str]:
    is_mysql = engine.lower() == "mysql"
    db_type = "MySQL" if is_mysql else "Postgres"
    q = "`" if is_mysql else "\""
    
    repair_prompt = f"""The previous SQL generated failed the Aggregation and GROUP BY structural validations.
Error: {agg_error}

You MUST rewrite the query safely for the user's question, strictly fixing the mathematical aggregation logic.

Question: {question}

Requirements:
- Return one valid {db_type} SELECT query in a markdown sql block.
- You MUST ensure all non-aggregated columns used in the SELECT clause are formally grouped in the GROUP BY clause.
- You MUST correctly implement explicit COUNT, SUM, AVG functions if mathematical derivation is expected.
"""

    messages = [
        {
            "role": "system",
            "content": f"You are a SQL repair expert. Produce one strict {db_type} SELECT query fixing the aggregation structural failure."
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


def _run_all_validators(sql: str, safe_schema: SchemaResponse, intent_data: Dict, engine: str) -> tuple[bool, Optional[str]]:
    """
    Validators ensuring safety without over-blocking legitimate queries.
    
    Hard blocks: security violations (non-SELECT, SELECT *, mutations)
    Hard blocks: hallucinated table names
    Hard blocks: structural aggregation errors (missing GROUP BY)
    Soft pass: FK relationship mismatches (logged as warnings)
    Soft pass: intent mismatch (guidance only, not enforced)
    """
    # 1. Security validation — HARD BLOCK (mutations, SELECT *, etc.)
    is_valid, error = is_valid_sql(sql, engine=engine)
    if not is_valid:
        return False, error
        
    # 2. Join validation — now only hard-blocks hallucinated tables
    #    FK mismatches are soft warnings (returns None)
    from app.query_service.join_validator import validate_joins
    error = validate_joins(sql, safe_schema, engine=engine)
    if error:
        return False, error
        
    # 3. Aggregation validation — hard-blocks missing GROUP BY
    from app.query_service.aggregation_validator import validate_aggregation
    error = validate_aggregation(sql, engine=engine)
    if error:
        return False, error
    
    # 4. Intent match — NO-OP (guidance only, never blocks)
    # Kept for API compatibility but verify_intent_match always returns None
        
    return True, None


async def generate_sql(
    question: str,
    context: Dict,
    schema: SchemaResponse,
    history: List[Dict] = None,
    vector_candidates: List[Dict] = None,
    session_id: str = "default",
    engine: str = "postgres"
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
        
    import hashlib
    schema_fingerprint = str(len(schema.tables)) + str(len(schema.metrics))
    cache_key_str = f"{question.lower().strip()}|{schema_fingerprint}|{provider}|{model_name}|{engine}"
    cache_key = hashlib.md5(cache_key_str.encode('utf-8')).hexdigest()

    cached_sql = SQL_CACHE.get(cache_key)
    if cached_sql:
        logger.info(f"SQL Cache hit for question: {question}")
        return cached_sql
    
    # Classify Intent
    from app.query_service.intent_classifier import classify_intent, verify_intent_match
    intent_data = classify_intent(question)
    
    # Build prompt
    safe_schema = redact_schema(schema)
    safe_history = redact_history_for_llm(history)
    safe_vector_candidates = redact_vector_candidates(vector_candidates)
    prompt = build_prompt(
        question, 
        context, 
        safe_schema, 
        safe_history, 
        safe_vector_candidates, 
        intent_pattern=intent_data["instruction"],
        engine=engine
    )
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

    best_effort_prompt = (
        f"{prompt}\n\n"
        "BEST-EFFORT INSTRUCTION:\n"
        "- Do not refuse just because the wording is somewhat ambiguous.\n"
        "- If the schema contains usable date/time and amount or metric columns, choose the most reasonable analytical interpretation and write SQL.\n"
        "- For phrases like 'suddenly increased', 'spike', or 'jumped', prefer period-over-period change logic using window functions such as LAG when appropriate.\n"
        "- Only refuse if the schema truly lacks the fields needed to answer approximately.\n"
    )

    retry_mode = _classify_safe_retry_mode(question)
    if retry_mode:
        retry_prompt = (
            f"{prompt}\n\n"
            "RETRY INSTRUCTION:\n"
            f"{_build_safe_retry_instruction(retry_mode, engine=engine)}"
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

    attempts.append({
        "label": "best_effort_retry",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an expert SQL generator. Return only raw SQL in markdown. "
                    "Prefer a best-effort analytical SQL answer over refusal when the schema supports a reasonable interpretation."
                )
            },
            {
                "role": "user",
                "content": best_effort_prompt
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
            sql = optimize_sql(sql, safe_schema, engine=engine)

            # Check if LLM refused based on schema or policy
            if _is_llm_refusal(sql):
                logger.info(
                    f"LLM returned a refusal on attempt '{attempt['label']}' using {model_name}"
                )
                last_validation_error = "LLM refusal"
                if attempt is not attempts[-1]:
                    continue
                return sql

            is_valid, error = _run_all_validators(sql, safe_schema, intent_data, engine)
            if is_valid:
                logger.info(
                    f"Successfully generated and validated SQL using {model_name} "
                    f"on attempt '{attempt['label']}'"
                )
                logger.debug(f"Generated SQL:\n{sql}")
                SQL_CACHE.set(cache_key, sql)
                return sql

            last_validation_error = error
            logger.warning(
                f"Generated SQL failed validation on attempt '{attempt['label']}': {error}"
            )

        # ---------------------------------------------------------------------
        # Sequential Cascading Retry Loops (Max 1 per layer for speed)
        # ---------------------------------------------------------------------

        # 1. STAR VALIDATION RETRY
        star_retries = 0
        while retry_mode and last_validation_error == STAR_VALIDATION_MESSAGE and star_retries < 1:
            repaired_sql = await _repair_star_validation_sql(
                adapter=adapter, api_key=api_key, model_name=model_name,
                base_url=base_url, question=question, context=context, 
                schema=safe_schema, retry_mode=retry_mode, engine=engine
            )
            if repaired_sql:
                repaired_sql = optimize_sql(repaired_sql, safe_schema, engine=engine)
                is_valid, error = _run_all_validators(repaired_sql, safe_schema, intent_data, engine)
                if is_valid: 
                    SQL_CACHE.set(cache_key, repaired_sql)
                    return repaired_sql
                last_validation_error = error
            star_retries += 1

        # 2. SCHEMA JOIN RETRY
        join_retries = 0
        while last_validation_error and "Invalid schema join" in last_validation_error and join_retries < 1:
            repaired_sql = await _repair_invalid_joins_sql(
                adapter=adapter, api_key=api_key, model_name=model_name,
                base_url=base_url, question=question, context=context, 
                schema=safe_schema, join_error=last_validation_error, engine=engine
            )
            if repaired_sql:
                repaired_sql = optimize_sql(repaired_sql, safe_schema, engine=engine)
                is_valid, error = _run_all_validators(repaired_sql, safe_schema, intent_data, engine)
                if is_valid: 
                    SQL_CACHE.set(cache_key, repaired_sql)
                    return repaired_sql
                last_validation_error = error
            join_retries += 1

        # 3. AGGREGATION RETRY
        agg_retries = 0
        while last_validation_error and "Aggregation Error" in last_validation_error and agg_retries < 1:
            repaired_sql = await _repair_aggregation_sql(
                adapter=adapter, api_key=api_key, model_name=model_name,
                base_url=base_url, question=question,
                schema=safe_schema, agg_error=last_validation_error, engine=engine
            )
            if repaired_sql:
                repaired_sql = optimize_sql(repaired_sql, safe_schema, engine=engine)
                is_valid, error = _run_all_validators(repaired_sql, safe_schema, intent_data, engine)
                if is_valid: 
                    SQL_CACHE.set(cache_key, repaired_sql)
                    return repaired_sql
                last_validation_error = error
            agg_retries += 1

        # BEST-EFFORT FALLBACK: If all retries failed but we have SQL,
        # return it anyway and let the database execution + auto-repair handle issues.
        # This prevents the user from getting a blank "I cannot fulfill this request" error.
        if last_validation_error and sql:
            logger.warning(
                f"All validation retries exhausted. Returning best-effort SQL with warning: {last_validation_error}"
            )
            return sql
            
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
    session_id: str = "default",
    engine: str = "postgres"
) -> str:
    """
    Call LLM to repair SQL that failed execution using session config.
    """
    cfg = _get_session_config(session_id)
    if not cfg["api_key"]: return prior_sql
    
    from app.llm_service.llm_adapters import get_adapter
    adapter = get_adapter(cfg["provider"])
    
    is_mysql = engine.lower() == "mysql"
    db_type = "MySQL" if is_mysql else "Postgres"
    q = "`" if is_mysql else "\""
    
    # --- ENHANCEMENT: Type-Safe Repair ---
    # Find tables in the prior SQL to provide a focused schema hint
    tables_to_hint = []
    try:
        # Use sqlglot to find all table names in prior_sql
        dialect = "mysql" if engine.lower() == "mysql" else "postgres"
        for table_node in sqlglot.parse_one(prior_sql, read=dialect).find_all(exp.Table):
            if table_node.name:
                tables_to_hint.append(table_node.name.lower())
    except: pass
    
    schema_hint = "No specific table metadata found."
    if tables_to_hint:
        hints = []
        for table in schema.tables:
            if table.table.lower() in tables_to_hint:
                columns = [f"{c.name} ({c.data_type})" for c in table.columns]
                hints.append(f"Table {q}{table.table}{q}: {', '.join(columns)}")
        if hints:
            schema_hint = "\n".join(hints)

    repair_prompt = f"""The following SQL query was generated for the question: "{_scrub_pii(question)}"
Query: {prior_sql}
Error: {error}

FOCUSED SCHEMA FOR TABLES INVOLVED:
{schema_hint}

Please fix the SQL query to resolve the error. 
- Ensure you follow all schema rules and respect data types.
- If the error mentions a type mismatch (e.g. integer vs string), identifying which column is the integer/ID column and use a descriptive text column for filtering or joining instead.
- Return ONLY the corrected raw SQL in a markdown block.
- Use {q} identifiers for all table and column names exactly as they appear in the schema (e.g., {q}ProductID{q}).
- CASE SENSITIVITY CHECK: If the error mentions a column "does not exist", it is likely a casing issue. Check the schema and use the exact casing.
- This is for a {db_type} database.
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


def is_valid_sql(sql: str, engine: str = "postgres") -> tuple[bool, Optional[str]]:
    """
    Perform security and structural validation using sqlglot AST parsing.
    """
    if not sql:
        return False, "Empty SQL query"

    try:
        # 1. Parse SQL
        dialect = "mysql" if engine.lower() == "mysql" else "postgres"
        parsed = parse_one(sql, read=dialect)
        
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

        # 4. Block SELECT * but ALLOW COUNT(*) patterns
        for star in parsed.find_all(exp.Star):
            # If the star's parent is NOT a COUNT function, then it's a 'SELECT *' pattern which is blocked
            parent = star.parent
            if not (isinstance(parent, exp.Count) or (isinstance(parent, exp.Alias) and isinstance(parent.parent, exp.Count))):
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


async def extract_structured_memory(question: str, history: List[Dict] = None, session_id: str = "default") -> Dict:
    """
    Extract structured entities, filters, and time ranges from the question.
    """
    cfg = _get_session_config(session_id)
    api_key = cfg["api_key"]
    if not api_key: return {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}
    
    from app.llm_service.llm_adapters import get_adapter
    adapter = get_adapter(cfg["provider"])
    
    extraction_prompt = f"""Extract structured information from this user question: "{_scrub_pii(question)}"
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
        
        content = await adapter.chat_completion(messages, api_key, cfg["model"], base_url=cfg.get("base_url"))
        if content:
            import json
            return json.loads(content)
    except Exception as e:
        logger.error(f"Structured memory extraction failed: {e}")
    
    return {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}


async def generate_summary(
    question: str,
    data: Any,
    session_id: str = "default",
    total_count: int = None
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
    
    # Performance: Truncate data to Top 10 rows for faster and more focused summary generation
    if isinstance(data, list):
        truncated_data = data[:10]
        if len(data) > 10:
            logger.info(f"Summary data truncated (List) to Top 10 rows for session {session_id}")
    elif isinstance(data, dict) and "rows" in data and isinstance(data["rows"], list):
        if len(data["rows"]) > 10:
            truncated_data = {**data, "rows": data["rows"][:10]}
            logger.info(f"Summary data truncated (Dict) to Top 10 rows for session {session_id}")
        else:
            truncated_data = data
    else:
        truncated_data = data
    
    safe_data = redact_results_for_summary(truncated_data)
    attempts = [
        {
            "label": "primary",
            "model": cfg["model"],
            "prompt": build_summary_prompt(question, safe_data, total_count=total_count),
            "system": "You are a professional Data Analyst. Your summaries are always concise, factual, and highlight key metrics with bolding. Do not use conversational filler."
        }
    ]

    fallback_model = DEFAULT_MODELS.get(cfg["provider"])
    if fallback_model and fallback_model != cfg["model"]:
        attempts.append({
            "label": "provider-default-fallback",
            "model": fallback_model,
            "prompt": build_summary_prompt_compact(question, safe_data, total_count=total_count),
            "system": "You are a concise data analyst. Return a short factual summary with the key numbers only."
        })
    else:
        attempts.append({
            "label": "compact-retry",
            "model": cfg["model"],
            "prompt": build_summary_prompt_compact(question, safe_data, total_count=total_count),
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
    Analyze the user's intent to suggest appropriate visualizations.
    Uses a fast-path heuristic for obvious queries to avoid LLM latency.
    """
    # 1. Fast Path Heuristic (0ms Latency)
    fallback = _fallback_intent_analysis(question)
    if fallback.get("confidence", 0) >= 0.9:
        logger.info(f"Using high-confidence heuristic for intent of '{question}'")
        return fallback

    # 2. LLM Intent Analysis (Standard Path)
    try:
        cfg = _get_session_config(session_id)
        if not cfg["api_key"]:
            logger.info(f"No API key for intent analysis, using fallback for '{question}'")
            return fallback

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
            logger.info(f"LLM Intent analysis for session {session_id}: {result.get('intent')} ({result.get('confidence', 0):.0%} confidence)")
            return result
            
    except Exception as e:
        logger.warning(f"LLM intent analysis failed for session {session_id}: {e}, using fallback")
    
    return fallback


def _fallback_intent_analysis(question: str) -> Dict[str, Any]:
    """Enhanced rule-based intent analysis with confidence scoring."""
    q_lower = question.lower().strip()
    
    # 1. Metric / KPI (extremely high confidence for simple counts/totals)
    if any(kw in q_lower for kw in ['total', 'how many', 'count', 'sum of', 'aggregate', 'overall']):
        # If it also contains "by" or "over time", it might be a comparison/trend
        if " by " not in q_lower and " per " not in q_lower and " over " not in q_lower:
            return {
                "intent": "metric",
                "keywords": ["total", "count"],
                "suggested_chart_types": ["kpi"],
                "confidence": 0.95,
                "reason": "Direct numeric question without breakdown."
            }
    
    # 2. Trend Analysis
    if any(kw in q_lower for kw in ['trend', 'over time', 'monthly', 'daily', 'yearly', 'growth', 'change', 'by month', 'by year']):
        return {
            "intent": "trend_analysis",
            "keywords": ["trend", "time"],
            "suggested_chart_types": ["line", "area"],
            "confidence": 0.9,
            "reason": "Time-based query pattern detected."
        }
    
    # 3. Comparison
    if any(kw in q_lower for kw in ['compare', 'versus', ' vs ', 'difference between', 'top', 'bottom', 'best', 'worst', 'ranked', 'by category']):
        return {
            "intent": "comparison",
            "keywords": ["comparison", "vs"],
            "suggested_chart_types": ["bar"],
            "confidence": 0.8,
            "reason": "Comparative or ranking query pattern detected."
        }
    
    # 4. Composition / Breakdown
    if any(kw in q_lower for kw in ['breakdown', 'share', 'percent', 'portion', 'composition', 'split']):
        return {
            "intent": "composition",
            "keywords": ["breakdown", "percent"],
            "suggested_chart_types": ["pie", "donut"],
            "confidence": 0.8,
            "reason": "Composition query pattern detected."
        }

    # 5. Default Fallback (Low Confidence)
    return {
        "intent": "detail",
        "keywords": [],
        "suggested_chart_types": ["table"],
        "confidence": 0.1,
        "reason": "Generic or complex query, defaulting to data detail."
    }
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


async def explain_sql(question: str, sql: str, session_id: str = "default") -> str:
    """
    Generate a plain English explanation of the SQL query.
    """
    cfg = _get_session_config(session_id)
    api_key = cfg["api_key"]
    model_name = cfg["model"]
    provider = cfg["provider"]
    base_url = cfg.get("base_url")

    if not api_key:
        return "Explanation inherently omitted (API key missing)."

    try:
        from app.query_service.prompt_builder import build_explain_prompt
        prompt = build_explain_prompt(question, sql)
        
        messages = [
            {"role": "system", "content": "You are an expert abstract data translator. Do not use structural terms (JOIN, ON, GROUP BY) when answering."},
            {"role": "user", "content": prompt}
        ]
        
        from app.llm_service.llm_adapters import get_adapter
        adapter = get_adapter(provider)
        
        content = await adapter.chat_completion(
            messages,
            api_key,
            model_name,
            base_url=base_url
        )

        return content.strip() if content else "Explanation generation yielded no output."
    except Exception as e:
        logger.error(f"Failed to generate query explanation: {e}")
        return "Explanation could not be generated due to an internal error."
