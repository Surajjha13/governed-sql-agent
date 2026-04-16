import asyncio
import anyio
from fastapi import APIRouter, HTTPException, Header, Depends, Query, BackgroundTasks
from app.auth.api import get_current_user
from typing import Optional, List, Dict
from pydantic import BaseModel, field_validator
import logging
import re
import time
from app.auth.user_manager import user_manager

from app.query_service.context_builder import build_context
from app.query_service.prompt_builder import build_prompt
from app.query_service.rbac_guard import validate_sql_against_rbac
from app.llm_service.exceptions import LLMRateLimitError
from app.query_service.execution import execute_sql, execute_sql_async
from app.services.visualization_service import VisualizationService
from app.auth.policies import filter_schema_for_user, get_effective_rbac_for_user

import app.app_state as app_state


import hashlib
from collections import OrderedDict

router = APIRouter()
logger = logging.getLogger(__name__)

class ResultCache:
    def __init__(self, maxsize=100, ttl=60):
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

RESULT_CACHE = ResultCache(maxsize=100, ttl=60)
QUERY_PREVIEW_ROWS = 10
FAST_SUMMARY_MAX_ROWS = QUERY_PREVIEW_ROWS
FAST_VIZ_MAX_ROWS = QUERY_PREVIEW_ROWS
FAST_VIZ_MAX_COLS = 8


def _record_stage_timing(timings: Dict[str, float], stage: str, started_at: float):
    timings[stage] = round((time.perf_counter() - started_at) * 1000, 2)


def _filter_history_by_export_ids(history: List[Dict], item_ids: Optional[str]) -> List[Dict]:
    if item_ids is None:
        return history

    target_ids = {
        token.strip()
        for token in item_ids.split(",")
        if token and token.strip()
    }
    if not target_ids:
        return []

    return [
        item for item in history
        if str(item.get("id")) in target_ids or str(item.get("request_id")) in target_ids
    ]


def _build_row_limit_guidance(results: Dict) -> Optional[str]:
    if not results.get("truncated"):
        return None

    total_count = results.get("total_count")
    if isinstance(total_count, int) and total_count > 0:
        return f"A preview is shown here. Export the full result to review all {total_count} matching records."
    return "A preview is shown here. Export the full result if you need the complete dataset."


def _format_summary_value(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _prettify_column_name(column: str) -> str:
    return str(column).replace("_", " ").strip()


def _describe_result_fields(columns: List[str]) -> str:
    if not columns:
        return "the requested fields"

    labels = [_prettify_column_name(col) for col in columns[:3]]
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return f"{labels[0]}, {labels[1]}, and {labels[2]}"


def _build_fast_summary(question: str, results: Dict) -> Optional[str]:
    """Only short-circuit for trivial cases. All substantive results go to the LLM."""
    rows = results.get("rows", []) or []
    columns = results.get("columns", []) or []

    # Zero results — no LLM needed
    if not rows:
        return "No matching records were found for your request."

    # Single scalar value — no LLM needed
    if len(rows) == 1 and len(columns) == 1:
        column = columns[0]
        value = _format_summary_value(rows[0].get(column))
        guidance = _build_row_limit_guidance(results) or ""
        return f"**{value}** — {_prettify_column_name(column)}.{(' ' + guidance) if guidance else ''}"

    # All other cases: defer to LLM for real business insight
    return None


def _should_use_llm_summary(results: Dict) -> bool:
    """Always use LLM for business insight summaries when there is data."""
    rows = results.get("rows", []) or []
    return bool(rows)


def _should_use_llm_viz(results: Dict) -> bool:
    rows = results.get("rows", []) or []
    columns = results.get("columns", []) or []
    return bool(rows) and len(rows) <= FAST_VIZ_MAX_ROWS and len(columns) <= FAST_VIZ_MAX_COLS


async def _generate_summary_safe(question: str, results: Dict, session_id: str) -> Dict:
    started_at = time.perf_counter()
    if not results.get("rows"):
        return {
            "summary": "No matching records were found for your request.",
            "rate_limit": None,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": False,
            "error": False
        }
    fast_summary = _build_fast_summary(question, results)
    if fast_summary is not None or not _should_use_llm_summary(results):
        summary = fast_summary or "The query completed successfully."
        return {
            "summary": summary,
            "rate_limit": None,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": False,
            "error": False
        }
    try:
        from app.llm_service import generate_summary
        total_count = results.get("total_count")
        summary = await generate_summary(question, results.get("rows", [])[:10], session_id=session_id, total_count=total_count)

        return {
            "summary": summary,
            "rate_limit": None,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": False,
            "error": False
        }
    except LLMRateLimitError:
        # Re-raise so the background task can retry with backoff
        raise
    except Exception as e:
        logger.error(f"Summary generation failed for session {session_id}: {e}")
        return {
            "summary": "AI Insight summary generation failed (Internal Error).",
            "rate_limit": None,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": False,
            "error": True
        }


async def _recommend_visualization_safe(question: str, results: Dict, session_id: str):
    started_at = time.perf_counter()
    try:
        # --- PERFORMANCE BOOST: Data-First Check ---
        # If it's a single value (1 row, 1 col), it's always a KPI. Skip expensive intent analysis.
        rows = results.get("rows", [])
        cols = results.get("columns", [])
        if len(rows) == 1 and len(cols) == 1:
            logger.info(f"Fast-path: Single value 1x1 detected for session {session_id}, forcing KPI.")
            return {
                "recommendation": {
                    "recommended_chart": "kpi",
                    "confidence": 100,
                    "reason": "Single numeric value detected - optimal for KPI card.",
                    "alternatives": [],
                    "config": {"title": cols[0]}
                },
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "error": False
            }

        if _should_use_llm_viz(results):
            from app.llm_service import analyze_visualization_intent
            intent_analysis = await analyze_visualization_intent(question, session_id=session_id)
            viz_recommendation = VisualizationService.recommend_visualization_intelligent(
                results=results,
                question=question,
                llm_intent=intent_analysis
            )
        else:
            viz_recommendation = VisualizationService.recommend_visualization(results, question)
        logger.info(
            f"Visualization recommended: {viz_recommendation['recommended_chart']} "
            f"({viz_recommendation['confidence']}%)"
        )

        if viz_recommendation.get('alternatives'):
            alt_summary = ", ".join(
                [f"{alt['chart']}({alt['score']}%)" for alt in viz_recommendation['alternatives']]
            )
            logger.info(f"Alternative charts: {alt_summary}")
        return {
            "recommendation": viz_recommendation,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "error": False
        }
    except Exception as e:
        logger.error(f"Error recommending visualization: {e}")
        try:
            return {
                "recommendation": VisualizationService.recommend_visualization(results, question),
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "error": False
            }
        except Exception as fallback_error:
            logger.error(f"Fallback visualization also failed: {fallback_error}")
            return {
                "recommendation": None,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "error": True
            }


async def _extract_structured_memory_safe(question: str, history: List[Dict], session_id: str):
    try:
        from app.llm_service import extract_structured_memory
        return await extract_structured_memory(question, history, session_id)
    except Exception as e:
        logger.error(f"Failed to extract structured memory: {e}")
        return {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}


async def _explain_sql_safe(question: str, sql: str, session_id: str) -> Optional[str]:
    try:
        from app.llm_service import explain_sql
        return await explain_sql(question, sql, session_id=session_id)
    except Exception as e:
        logger.error(f"Failed to securely explain SQL: {e}")
        return None


class QueryRequest(BaseModel):
    question: str
    
    @field_validator('question')
    @classmethod
    def validate_question(cls, v: str) -> str:
        """
        Validate the question field with comprehensive checks:
        - Not empty or whitespace-only
        - Within reasonable length limits
        - No suspicious SQL injection patterns
        """
        # Check for empty or whitespace-only strings
        if not v or not v.strip():
            raise ValueError("Question cannot be empty or contain only whitespace")
        
        # Strip whitespace for further validation
        v = v.strip()
        
        # Check minimum length
        if len(v) < 3:
            raise ValueError("Question must be at least 3 characters long")
        
        # Check maximum length (prevent abuse)
        if len(v) > 1000:
            raise ValueError("Question must not exceed 1000 characters")
        
        # Check for suspicious patterns that might indicate SQL injection attempts
        # This is a basic check - the actual SQL generation should have proper parameterization
        dangerous_patterns = [
            r';\s*DROP\s+',         # DROP with semicolon
            r'^\s*DROP\s+',         # DROP at start of question
            r';\s*DELETE\s+',       # DELETE with semicolon
            r'^\s*DELETE\s+',       # DELETE at start of question
            r';\s*UPDATE\s+.*SET',  # UPDATE with semicolon
            r'^\s*UPDATE\s+',       # UPDATE at start of question
            r';\s*INSERT\s+INTO',   # INSERT with semicolon
            r'^\s*INSERT\s+',       # INSERT at start of question
            r';\s*TRUNCATE\s+',     # TRUNCATE with semicolon
            r'^\s*TRUNCATE\s+',     # TRUNCATE at start of question
            r';\s*ALTER\s+',        # ALTER with semicolon
            r'^\s*ALTER\s+',        # ALTER at start of question
            r'--\s*$',              # SQL comment at end
            r'/\*.*\*/',            # SQL block comment
            r'EXEC\s*\(',  # Execute command
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, v, re.IGNORECASE):
                logger.warning(f"Potentially dangerous pattern detected in question: {pattern}")
                raise ValueError("Question contains potentially unsafe patterns")
        
        return v


@router.post("/query")
async def run_query(
    req: QueryRequest,
    background_tasks: BackgroundTasks,
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    """
    Process a natural language query and generate SQL for the given session.
    """
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    logger.info(f"!!!! QUERY RECEIVED (Session: {session_id}) !!!!")
    logger.info(f"Question: {req.question}")
    
    start_time = time.perf_counter()
    stage_timings: Dict[str, float] = {}
    sql_rate_limit = None
    sql_error_stage = None
    
    app_state.update_activity(session_id)
    state = app_state.get_session(session_id)

    def persist_history_snapshot(
        *,
        summary: Optional[str],
        sql: Optional[str],
        results: Optional[Dict] = None,
        error: Optional[str] = None,
        visualization: Optional[Dict] = None,
    ) -> int:
        req_time = int(time.time() * 1000)
        history_results = results or {"columns": [], "rows": []}
        history_item = {
            "id": req_time,
            "request_id": str(req_time),
            "question": req.question,
            "user": req.question,
            "summary": summary,
            "assistant": summary,
            "sql": sql,
            "explanation": None,
            "results": history_results,
            "has_error": bool(error or history_results.get("error")),
            "visualization": visualization,
            "structured": {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}
        }
        state.chat_history.append(history_item)
        if len(state.chat_history) > 10:
            state.chat_history = state.chat_history[-10:]

        if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
            db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
            user_manager.save_chat_history(
                user=current_user,
                db_name=db_name,
                question=req.question,
                sql=sql,
                summary=summary,
                results=history_results,
                visualization=visualization,
                request_id=req_time
            )

        return req_time

    def log_observability_snapshot(
        *,
        request_id: Optional[int] = None,
        sql: Optional[str] = None,
        success: bool,
        had_rate_limit: bool = False,
        rate_limit_stage: Optional[str] = None,
        error_stage: Optional[str] = None,
        error_message: Optional[str] = None,
        total_ms: Optional[float] = None,
    ) -> Optional[int]:
        if app_state.SYSTEM_MODE != "enterprise" or current_user.role == "SOLO_USER":
            return None

        from app.llm_service.llm_service import _get_session_config

        llm_cfg = _get_session_config(session_id)
        db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
        return user_manager.log_observability_event(
            user=current_user,
            payload={
                "request_id": request_id,
                "db_name": db_name,
                "question": req.question,
                "sql_query": sql,
                "llm_provider": llm_cfg.get("provider"),
                "llm_model": llm_cfg.get("model"),
                "sql_gen_ms": stage_timings.get("sql_generation_ms"),
                "db_exec_ms": stage_timings.get("sql_execution_ms"),
                "summary_ms": None,
                "viz_ms": None,
                "total_ms": total_ms if total_ms is not None else round((time.perf_counter() - start_time) * 1000, 2),
                "success": success,
                "had_rate_limit": had_rate_limit,
                "rate_limit_stage": rate_limit_stage,
                "error_stage": error_stage,
                "error_message": error_message,
            }
        )

    # Step 0: Check if DB is connected
    if state.normalized_schema is None:
        logger.warning(f"Query attempt for session {session_id} while database not connected")
        return {
            "question": req.question,
            "sql": None,
            "results": {"columns": [], "rows": []},
            "summary": f"Database connection lost for session {session_id}. This can happen after a period of inactivity or a server restart. Please re-reconnect to your database.",
            "error": "No active database connection."
        }
    user_schema = filter_schema_for_user(state.normalized_schema, current_user.username)
    rbac_restrictions = get_effective_rbac_for_user(current_user.username)
    engine = state.current_connection.get("engine", "postgres") if state.current_connection else "postgres"

    try:
        vector_started_at = time.perf_counter()
        from app.semantic_service.vector_index import search_vector_index
        # --- PERFORMANCE BOOST: Offload CPU-bound vector search to thread ---
        candidates = await anyio.to_thread.run_sync(
            search_vector_index,
            req.question,
            state.vector_index,
            state.vector_metadata
        )
        logger.info(f"Vector candidates found: {len(candidates)}")
        _record_stage_timing(stage_timings, "vector_search_ms", vector_started_at)
    except Exception as e:
        logger.error(f"Error searching vector index: {e}")
        candidates = [] # Non-blocking
        _record_stage_timing(stage_timings, "vector_search_ms", vector_started_at)

    try:
        # Step 1: Build context with vector hits AND history (Hybrid Retrieval)
        history = state.chat_history
        context_started_at = time.perf_counter()
        context = build_context(
            req.question, 
            user_schema,
            vector_candidates=candidates,
            history=history
        )
        logger.info(f"Context built successfully")
        _record_stage_timing(stage_timings, "context_build_ms", context_started_at)
    except Exception as e:
        logger.error(f"Error building context: {e}")
        _record_stage_timing(stage_timings, "context_build_ms", context_started_at)
        return {
            "question": req.question,
            "sql": None,
            "results": {"columns": [], "rows": []},
            "summary": f"I encountered an error while analyzing your question. (Context Error: {str(e)})",
            "error": str(e)
        }

    # Generate SQL using LLM with history and vector hits
    try:
        sql_started_at = time.perf_counter()
        from app.llm_service import generate_sql
        sql = await generate_sql(
            question=req.question,
            context=context,
            schema=user_schema,
            history=history,
            vector_candidates=candidates,
            session_id=session_id,
            engine=engine
        )
        
        # Handle Policy Refusal — only when the LLM genuinely refused (not a false positive)
        # The response must look like a refusal message, NOT contain actual SQL
        sql_lower = sql.lower()
        is_refusal = (
            ("i cannot answer" in sql_lower or "i cannot fulfill" in sql_lower or "not in the schema" in sql_lower)
            and "select" not in sql_lower
        )
        if is_refusal:
            logger.warning(f"LLM refusal detected for question: {req.question}")
            req_time = persist_history_snapshot(
                summary=sql,
                sql=None,
                results={"columns": [], "rows": []},
                error=None
            )
            return {
                "id": req_time,
                "question": req.question,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": sql,
                "error": None
            }

        logger.info(f"SQL generated successfully")
        _record_stage_timing(stage_timings, "sql_generation_ms", sql_started_at)
    except LLMRateLimitError as e:
        error_str = str(e)
        logger.warning(f"LLM rate limit while generating SQL: {error_str}")
        _record_stage_timing(stage_timings, "sql_generation_ms", sql_started_at)
        sql_rate_limit = {
            "stage": "sql_gen",
            "provider": getattr(e, "provider", None),
            "model": getattr(e, "model", None)
        }

        recommendations = getattr(e, "recommendations", []) or []
        recommendation_text = ""
        if recommendations:
            recommendation_text = f" Try one of these models: {', '.join(recommendations)}."

        req_time = persist_history_snapshot(
            summary=(
                "Your configured LLM provider is currently rate-limited (HTTP 429). "
                "Please wait a moment and retry, or switch model/provider in LLM Configuration."
                f"{recommendation_text}"
            ),
            sql=None,
            results={"error": "LLM Rate Limit Reached.", "columns": [], "rows": []},
            error="LLM rate limit reached."
        )
        log_observability_snapshot(
            request_id=req_time,
            sql=None,
            success=False,
            had_rate_limit=True,
            rate_limit_stage="sql_gen",
            error_stage="sql_gen",
            error_message=error_str,
        )
        return {
            "id": req_time,
            "question": req.question,
            "sql": None,
            "results": {"columns": [], "rows": []},
            "summary": (
                "Your configured LLM provider is currently rate-limited (HTTP 429). "
                "Please wait a moment and retry, or switch model/provider in LLM Configuration."
                f"{recommendation_text}"
            ),
            "error": "LLM rate limit reached.",
            "llm_rate_limit": {
                "message": error_str,
                "recommendations": recommendations,
                "provider": getattr(e, "provider", None),
                "model": getattr(e, "model", None)
            }
        }
    except Exception as e:
        error_str = str(e)
        logger.error(f"LLM error: {error_str}")
        _record_stage_timing(stage_timings, "sql_generation_ms", sql_started_at)
        sql_error_stage = "sql_gen"
        
        if "Validation Error:" in error_str:
            friendly_message = error_str.split("Validation Error: ")[-1]
            logger.warning(f"Validation error (non-fatal): {friendly_message}")
            log_observability_snapshot(
                sql=None,
                success=False,
                error_stage="sql_gen",
                error_message=friendly_message,
            )
            return {
                "question": req.question,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": f"I had trouble generating the perfect query, but here's what I found: **{friendly_message}**. Please try rephrasing your question.",
                "error": None
            }
            
        raise HTTPException(status_code=503, detail=error_str)

    # Execute SQL
    try:
        execution_started_at = time.perf_counter()
        access_denial = validate_sql_against_rbac(sql, rbac_restrictions, engine=engine)
        if access_denial:
            logger.warning(
                f"RBAC denied SQL for user {current_user.username} in session {session_id}: {access_denial}"
            )
            if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                user_manager.log_security_event(
                    user=current_user,
                    event_type="policy_denial",
                    severity="high",
                    event_source="rbac_guard",
                    resource_name=access_denial,
                    details={"sql": sql}
                )
            req_time = persist_history_snapshot(
                summary=access_denial,
                sql=None,
                results={"columns": [], "rows": []},
                error="RBAC policy restriction."
            )
            log_observability_snapshot(
                request_id=req_time,
                sql=sql,
                success=False,
                error_stage="rbac",
                error_message=access_denial,
            )
            return {
                "id": req_time,
                "question": req.question,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": access_denial,
                "error": "RBAC policy restriction."
            }

        # --- PERFORMANCE BOOST: Result Caching ---
        sql_hash = hashlib.md5(f"{session_id}|{sql}".encode('utf-8')).hexdigest()
        cached_res = RESULT_CACHE.get(sql_hash)
        
        if cached_res:
            logger.info("Result Cache hit for SQL")
            results = cached_res
        else:
            results = await execute_sql_async(sql, session_id=session_id, row_limit=QUERY_PREVIEW_ROWS)
            if not results.get("error"):
                RESULT_CACHE.set(sql_hash, results)
        
        # --- AUTO-REPAIR LOOP (1 step) ---
        if results.get("error"):
            logger.warning(f"SQL execution failed: {results['error']}. Attempting repair...")
            from app.llm_service.llm_service import repair_sql
            
            repaired_sql = await repair_sql(
                question=req.question,
                error=results["error"],
                prior_sql=sql,
                context=context,
                schema=user_schema,
                history=history,
                session_id=session_id,
                engine=engine
            )
            
            if repaired_sql != sql:
                logger.info("Retrying with repaired SQL...")
                sql = repaired_sql
                access_denial = validate_sql_against_rbac(sql, rbac_restrictions, engine=engine)
                if access_denial:
                    logger.warning(
                        f"RBAC denied repaired SQL for user {current_user.username} in session {session_id}: {access_denial}"
                    )
                    if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                        user_manager.log_security_event(
                            user=current_user,
                            event_type="policy_denial",
                            severity="high",
                            event_source="rbac_guard_repair",
                            resource_name=access_denial,
                            details={"sql": sql}
                        )
                    req_time = persist_history_snapshot(
                        summary=access_denial,
                        sql=None,
                        results={"columns": [], "rows": []},
                        error="RBAC policy restriction."
                    )
                    log_observability_snapshot(
                        request_id=req_time,
                        sql=sql,
                        success=False,
                        error_stage="rbac",
                        error_message=access_denial,
                    )
                    return {
                        "id": req_time,
                        "question": req.question,
                        "sql": None,
                        "results": {"columns": [], "rows": []},
                        "summary": access_denial,
                        "error": "RBAC policy restriction."
                    }
                results = await execute_sql_async(sql, session_id=session_id, row_limit=QUERY_PREVIEW_ROWS)
        
        logger.info(f"SQL processed (Execution Status: {'Success' if not results.get('error') else 'Failed'})")
        _record_stage_timing(stage_timings, "sql_execution_ms", execution_started_at)
    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        results = {"error": str(e), "columns": [], "rows": []}
        _record_stage_timing(stage_timings, "sql_execution_ms", execution_started_at)

    stage_timings["total_request_ms"] = round((time.perf_counter() - start_time) * 1000, 2)
    post_processing_started_at = time.perf_counter()
    req_time = int(time.time() * 1000)
    
    # Save skeleton to memory and DB immediately for frontend polling
    try:
        state.chat_history.append({
            "id": req_time,
            "request_id": str(req_time),
            "question": req.question,
            "user": req.question,
            "summary": None,
            "assistant": None,
            "sql": sql,
            "explanation": None,
            "results": results,
            "has_error": bool(results.get("error")),
            "visualization": None,
            "structured": {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}
        })
        if len(state.chat_history) > 10: # Increased memory buffer
            state.chat_history = state.chat_history[-10:]
            
        # Immediate DB persistence for Enterprise mode
        if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
            db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
            user_manager.save_chat_history(
                user=current_user, 
                db_name=db_name, 
                question=req.question, 
                sql=sql, 
                summary=None, 
                results=results, 
                request_id=req_time
            )
            
            # --- NEW: Immediate Audit/Observability logging for real-time dashboard update ---
            user_manager.log_audit(
                user=current_user, 
                question=req.question, 
                sql_query=sql, 
                latency_sec=stage_timings.get("total_request_ms", 0)/1000, 
                success=True if not results.get("error") else False
            )

            obs_id = log_observability_snapshot(
                request_id=req_time,
                sql=sql,
                success=True if not results.get("error") else False,
                error_stage=None if not results.get("error") else "execution",
                error_message=results.get("error"),
                total_ms=stage_timings.get("total_request_ms", 0),
            )
        else:
            obs_id = None
    except Exception as e:
        logger.error(f"Initial history persistence failed: {e}")

    logger.info(f"Query fast-path timings for session {session_id}: {stage_timings}")
    
    # Schedule slow metadata processing in background
    if not results.get("error"):
        initial_total_ms = stage_timings.get("total_request_ms", 0)
        async def hydrate_metadata(q, s, sid, req_id, res, user, o_id, init_ms):
            try:
                state_ref = app_state.get_session(sid)
                
                # Helper to update history item (Memory + DB)
                def update_history_item(key, value):
                    # 1. Update in-memory state for fast lookup
                    updated_memory = False
                    for h in reversed(state_ref.chat_history):
                        if str(h.get("id")) == str(req_id):
                            h[key] = value
                            if key == "summary":
                                h["assistant"] = value
                            updated_memory = True
                            break
                    
                    # 2. Update persistent DB for Enterprise mode polling
                    if app_state.SYSTEM_MODE == "enterprise" and user.role != "SOLO_USER":
                        user_manager.update_chat_history_partial(user.username, req_id, {key: value})
                        
                    return updated_memory

                # Start tasks concurrently but update as they finish
                async def run_summary():
                    max_retries = 3
                    base_delay = 2
                    for attempt in range(max_retries):
                        try:
                            res_summ = await _generate_summary_safe(q, res, sid)
                            update_history_item("summary", res_summ.get("summary"))
                            return res_summ
                        except LLMRateLimitError as e:
                            if attempt < max_retries - 1:
                                delay = base_delay * (2 ** attempt)
                                logger.warning(f"Summary rate-limited (429). Retry {attempt+1}/{max_retries} in {delay}s...")
                                await asyncio.sleep(delay)
                            else:
                                # Final failure
                                err_msg = "Summary generation failed due to persistent rate limiting. Please try again later."
                                update_history_item("summary", err_msg)
                                return {"summary": err_msg, "error": True}
                        except Exception as e:
                            logger.error(f"Sync-level error in run_summary background task: {e}")
                            return None

                async def run_viz():
                    res_viz = await _recommend_visualization_safe(q, res, sid)
                    update_history_item("visualization", res_viz.get("recommendation"))
                    return res_viz

                async def run_struct():
                    res_struct = await _extract_structured_memory_safe(q, state_ref.chat_history, sid)
                    update_history_item("structured", res_struct)
                    return res_struct

                # Run the important enrichments concurrently; SQL explanation is skipped to keep the
                # interactive path production-friendly.
                summ_res, viz_res, struct_res = await asyncio.gather(
                    run_summary(),
                    run_viz(),
                    run_struct()
                )
                 
                # Keep total_ms aligned with the user-visible API response time; background
                # enrichment latency is tracked per-stage without inflating request latency.
                if o_id and app_state.SYSTEM_MODE == "enterprise" and user.role != "SOLO_USER":
                    s_ms = summ_res.get("duration_ms", 0) if isinstance(summ_res, dict) else 0
                    v_ms = viz_res.get("duration_ms", 0) if isinstance(viz_res, dict) else 0
                    user_manager.update_observability_event(o_id, {
                        "summary_ms": s_ms,
                        "viz_ms": v_ms,
                        "total_ms": init_ms
                    })
                
                # Final console log when hydration completes
                logger.info(f"Background metadata hydration complete for req {req_id}")
            except Exception as e:
                logger.error(f"Global background hydration error: {e}")
                
        background_tasks.add_task(
            hydrate_metadata,
            req.question,
            sql,
            session_id,
            req_time,
            results,
            current_user,
            obs_id,
            initial_total_ms
        )
    else:
        # Fast fail persistence for DB errors
        s_val = f"**Query Blocked or Failed**\n\n{results.get('error')}"
        for h in reversed(state.chat_history):
            if h.get("id") == req_time:
                h["summary"] = s_val
                h["assistant"] = s_val
                break
        if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
            user_manager.update_chat_history_partial(current_user.username, req_time, {"summary": s_val})

    return {
        "id": req_time,
        "question": req.question,
        "sql": sql,
        "explanation": None,
        "results": results,
        "summary": None if not results.get("error") else s_val,
        "visualization": None,
        "error": results.get("error"),
        "llm_rate_limit": None
    }


# ── Chat History Endpoints ───────────────────────────────────────────────────

@router.get("/history")
async def get_history(
    limit: int = Query(50, description="Limit records"),
    start_date: Optional[str] = Query(None, description="ISO Start date filter"),
    end_date: Optional[str] = Query(None, description="ISO End date filter"),
    sort: str = Query("desc", description="desc or asc"),
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    """Fetch history isolated for the current user and database."""
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    state = app_state.get_session(session_id)
    
    # If solo mode, return the in-memory history (simplified filtering for solo)
    if app_state.SYSTEM_MODE == "solo" or current_user.role == "SOLO_USER":
        # Sort in-memory history
        hist = list(state.chat_history)
        if sort == "desc":
            hist = hist[::-1]
            
        # Basic slicing for limit
        return hist[:limit]
        
    # If enterprise, fetch from DB
    db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
    return user_manager.get_chat_history(
        current_user, 
        db_name, 
        limit=limit,
        start_date=start_date,
        end_date=end_date,
        sort=sort
    )

@router.delete("/history/clear")
async def clear_history(
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    """Clear history for the current user and database."""
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    state = app_state.get_session(session_id)
    
    if app_state.SYSTEM_MODE == "solo" or current_user.role == "SOLO_USER":
        state.chat_history = []
        return {"message": "Solo session history cleared"}
        
    db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
    user_manager.clear_chat_history(current_user, db_name)
    return {"message": f"History cleared for {db_name}"}

@router.delete("/history")
async def delete_history_items(
    item_ids: str = Query(..., description="Comma-separated list of item IDs"),
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    """Delete multiple history items."""
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    state = app_state.get_session(session_id)
    
    ids_to_delete = [int(i.strip()) for i in item_ids.split(",") if i.strip().isdigit()]
    
    if app_state.SYSTEM_MODE == "solo" or current_user.role == "SOLO_USER":
         initial_len = len(state.chat_history)
         state.chat_history = [h for h in state.chat_history if h.get("id") not in ids_to_delete]
         if len(state.chat_history) == initial_len:
             raise HTTPException(status_code=404, detail="History items not found")
         return {"message": f"{initial_len - len(state.chat_history)} items deleted"}
         
    success_count = 0
    for iid in ids_to_delete:
        if user_manager.delete_history_item(current_user, iid):
            success_count += 1
            
    if success_count == 0:
        raise HTTPException(status_code=404, detail="No matching history items found")
    return {"message": f"{success_count} items deleted"}


# ── Export Endpoints ─────────────────────────────────────────────────────────

@router.get("/history/export/excel")
async def export_excel(
    item_ids: Optional[str] = Query(None, description="Comma-separated list of item IDs"),
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    state = app_state.get_session(session_id)
    
    history = []
    if app_state.SYSTEM_MODE == "solo" or current_user.role == "SOLO_USER":
        history = state.chat_history
    else:
        db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
        history = user_manager.get_chat_history(current_user, db_name)
        
    history = _filter_history_by_export_ids(history, item_ids)
        
    if not history:
        raise HTTPException(status_code=404, detail="No history to export")
        
    import pandas as pd
    import io
    from fastapi.responses import StreamingResponse
    
    # Determine if we should export full results or metadata summary
    if len(history) == 1 and history[0].get('results') and history[0].get('results', {}).get('rows'):
        # For single item, export the actual SQL result table rows
        h = history[0]
        results_data = h['results']
        
        # Re-execute the query with no row limit to get ALL data for export
        if h.get('sql'):
            try:
                from app.query_service.execution import execute_sql
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"Re-executing query for export to fetch all rows without limit. ID: {h.get('id')}")
                # row_limit=0 corresponds to no limit in execution logic
                full_results = execute_sql(h['sql'], session_id=session_id, row_limit=0)
                if full_results and not full_results.get('error'):
                    results_data = full_results
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to fetch full results for export, using cached rows. Error: {e}")
                
        df = pd.DataFrame(results_data['rows'])
        
        # Ensure column order if available
        if results_data.get('columns'):
            cols = [c for c in results_data['columns'] if c in df.columns]
            df = df[cols]
            
        sheet_name = 'Query Results'
        filename = f"query_results_{h.get('id', 'item')}.xlsx"
    else:
        # Flatten history for metadata summary (Bulk export)
        flattened = []
        for h in history:
            flattened.append({
                "Timestamp": h.get("timestamp", ""),
                "Question": h.get("question", h.get("user", "")),
                "SQL": h.get("sql", ""),
                "Summary": h.get("summary", h.get("assistant", ""))
            })
        df = pd.DataFrame(flattened)
        sheet_name = 'Chat History'
        filename = 'chat_history.xlsx'
        
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@router.get("/history/export/pdf")
async def export_pdf(
    item_ids: Optional[str] = Query(None, description="Comma-separated list of item IDs"),
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    # For PDF, we'll implement a simple text-based PDF for now as a placeholder
    # because complex PDF layout requires specific libraries like reportlab or fpdf
    # I'll check if those are available or just use a simple approach.
    
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    state = app_state.get_session(session_id)
    
    history = []
    if app_state.SYSTEM_MODE == "solo" or current_user.role == "SOLO_USER":
        history = state.chat_history
    else:
        db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
        history = user_manager.get_chat_history(current_user, db_name)
        
    history = _filter_history_by_export_ids(history, item_ids)
        
    if not history:
        raise HTTPException(status_code=404, detail="No history to export")
        
    import io
    from fastapi.responses import StreamingResponse
    
    try:
        from fpdf import FPDF
    except ImportError:
        # Fallback to text file if fpdf is missing
        output = io.StringIO()
        output.write("SQL AGENT CHAT HISTORY\n")
        output.write("======================\n\n")
        for h in history:
            output.write(f"TIME:     {h.get('timestamp', '')}\n")
            output.write(f"QUESTION: {h.get('question', h.get('user', ''))}\n")
            output.write(f"SQL:      {h.get('sql', 'None')}\n")
            
            # Clean summary
            summary = h.get('summary', h.get('assistant', ''))
            summary = summary.replace('**', '') if summary else ''
            
            output.write(f"SUMMARY:  {summary}\n")
            output.write("-" * 60 + "\n\n")
        
        output.seek(0)
        headers = {'Content-Disposition': 'attachment; filename="chat_history.txt"'}
        return StreamingResponse(io.BytesIO(output.getvalue().encode('utf-8')), headers=headers, media_type='text/plain')

    # Generate actual PDF
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 15)
            self.cell(0, 10, 'SQL Agent Chat History', 0, 1, 'C')
            self.ln(5)

        def footer(self):
            self.set_y(-15)
            self.set_font('Arial', 'I', 8)
            self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    pdf = PDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Use standard Arial, it has good built-in encoding handling in fpdf
    pdf.set_font("Arial", size=11)
    
    # Helper for robust text addition
    def add_robust_text(pdf_obj, label, text, is_code=False):
        if not text:
            return
            
        # Strip simple markdown and non-latin1 chars for FPDF compatibility
        clean_text = str(text).replace('**', '').encode('latin-1', 'replace').decode('latin-1')
        
        pdf_obj.set_font("Arial", 'B', 11)
        pdf_obj.cell(0, 8, label)
        pdf_obj.ln(8)
        
        if is_code:
            pdf_obj.set_font("Courier", size=10)
        else:
            pdf_obj.set_font("Arial", size=10)
            
        pdf_obj.multi_cell(0, 6, clean_text)
        pdf_obj.ln(4)

    for i, h in enumerate(history):
        timestamp = h.get('timestamp', '')
        question = h.get('question', h.get('user', 'Unknown Question'))
        sql_query = h.get('sql', '')
        summary = h.get('summary', h.get('assistant', ''))
        
        # Block Header
        pdf.set_font("Arial", 'B', 12)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(0, 10, f" Item {i+1} | {timestamp}", 0, 1, 'L', fill=True)
        pdf.ln(5)
        
        # Content
        add_robust_text(pdf, "Question:", question)
        if sql_query and sql_query.strip():
            add_robust_text(pdf, "Generated SQL:", sql_query, is_code=True)
        add_robust_text(pdf, "Summary:", summary)
        
        # Results Table
        results_data = h.get('results')
        if results_data and results_data.get('columns') and results_data.get('rows'):
            pdf.set_font("Arial", 'B', 11)
            pdf.cell(0, 8, "Results:")
            pdf.ln(8)
            
            # Using fpdf2 table API
            try:
                with pdf.table(borders_layout="SINGLE_TOP_LINE", cell_fill_color=250, cell_fill_mode="ROWS", line_height=7) as table:
                    header = table.row()
                    for col in results_data['columns']:
                        header.cell(str(col))
                    
                    # Add data rows (limit to 50 for PDF size/performance)
                    for row in results_data['rows'][:50]:
                        row_cells = table.row()
                        for col in results_data['columns']:
                            row_cells.cell(str(row.get(col, '')))
            except Exception as e:
                logger.error(f"Failed to render table in PDF: {e}")
                pdf.set_font("Arial", 'I', 10)
                pdf.cell(0, 8, "(Table data could not be rendered)")
                pdf.ln(8)
        
        pdf.ln(10)
        
    # Output to BytesIO
    # Output to bytes
    pdf_bytes = pdf.output()
    
    # If using fpdf2, .output() typically returns bytes directly.
    # We ensure it's in a BytesIO for StreamingResponse.
    if isinstance(pdf_bytes, str):
        pdf_bytes = pdf_bytes.encode('latin-1')
        
    output = io.BytesIO(pdf_bytes)
    output.seek(0)
    
    headers = {'Content-Disposition': 'attachment; filename="chat_history.pdf"'}
    return StreamingResponse(output, headers=headers, media_type='application/pdf')
