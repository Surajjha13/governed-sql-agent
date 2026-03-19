import asyncio
from fastapi import APIRouter, HTTPException, Header, Depends, Query
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
from app.semantic_service.vector_index import search_vector_index
from app.llm_service import generate_sql, generate_summary, LLMError
from app.llm_service.exceptions import LLMRateLimitError
from app.query_service.execution import execute_sql
from app.services.visualization_service import VisualizationService
from app.auth.policies import filter_schema_for_user, get_effective_rbac_for_user

import app.app_state as app_state


router = APIRouter()
logger = logging.getLogger(__name__)


def _record_stage_timing(timings: Dict[str, float], stage: str, started_at: float):
    timings[stage] = round((time.perf_counter() - started_at) * 1000, 2)


def _build_row_limit_guidance(results: Dict) -> Optional[str]:
    if not results.get("truncated"):
        return None

    returned_rows = results.get("returned_rows", len(results.get("rows", [])))
    row_limit = results.get("row_limit")
    return (
        f" Showing the first {returned_rows} rows due to the server row limit"
        f"{f' ({row_limit})' if row_limit else ''}. Refine the query or export a narrower slice for more detail."
    )


async def _generate_summary_safe(question: str, results: Dict, session_id: str) -> Dict:
    started_at = time.perf_counter()
    try:
        summary = await generate_summary(question, results.get("rows", []), session_id=session_id)
        guidance = _build_row_limit_guidance(results)
        if guidance:
            summary = f"{summary}{guidance}"
        return {
            "summary": summary,
            "rate_limit": None,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": False,
            "error": False
        }
    except LLMRateLimitError as e:
        logger.warning(f"Summary generation rate-limited: {e}")
        summary = (
            "Results are ready, but summary generation is temporarily rate-limited (HTTP 429). "
            "Please retry in a moment or switch to another model/provider."
        )
        guidance = _build_row_limit_guidance(results)
        if guidance:
            summary = f"{summary}{guidance}"
        return {
            "summary": summary,
            "rate_limit": {
                "message": str(e),
                "recommendations": getattr(e, "recommendations", []) or [],
                "provider": getattr(e, "provider", None),
                "model": getattr(e, "model", None)
            },
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": True,
            "error": True
        }
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        summary = "Could not generate insight summary."
        guidance = _build_row_limit_guidance(results)
        if guidance:
            summary = f"{summary}{guidance}"
        return {
            "summary": summary,
            "rate_limit": None,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "had_rate_limit": False,
            "error": True
        }


async def _recommend_visualization_safe(question: str, results: Dict, session_id: str):
    started_at = time.perf_counter()
    try:
        from app.llm_service import analyze_visualization_intent

        intent_analysis = await analyze_visualization_intent(question, session_id=session_id)
        viz_recommendation = VisualizationService.recommend_visualization_intelligent(
            results=results,
            question=question,
            llm_intent=intent_analysis
        )
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


async def _extract_structured_memory_safe(question: str, history: List[Dict]):
    try:
        from app.llm_service import extract_structured_memory
        return await extract_structured_memory(question, history)
    except Exception as e:
        logger.error(f"Failed to extract structured memory: {e}")
        return {"tables": [], "metrics": [], "dimensions": [], "time_range": None, "filters": []}


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
            r'xp_cmdshell',         # SQL Server command execution
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

    # Step 0.1: Early rejection of "SELECT *" or "SELECT ALL" patterns in natural language
    star_patterns = [r"\bselect\s+\*", r"\bselect\s+all\b", r"\bgive\b.*\ball\b", r"\beverything\b"]
    for pattern in star_patterns:
        if re.search(pattern, req.question, re.IGNORECASE):
            logger.info(f"Early rejection triggered for pattern: {pattern}")
            if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                user_manager.log_security_event(
                    event_type="suspicious_query_pattern",
                    severity="medium",
                    username=current_user.username,
                    role=current_user.role,
                    event_source="question_guard",
                    details={"pattern": pattern, "question": req.question[:200]}
                )
            return {
                "question": req.question,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": "For security and performance reasons, please specify the columns you need individually rather than requesting 'all' or '*'.",
                "error": None
            }

    # Step 0.1: Early rejection of "SELECT *" or "SELECT ALL" patterns in natural language
    # We remove 'everything' and 'all' as they can be part of valid questions (e.g., 'all products in category')
    star_patterns = [r"\bselect\s+\*", r"\bselect\s+all\b"]
    for pattern in star_patterns:
        if re.search(pattern, req.question, re.IGNORECASE):
            logger.info(f"Early rejection triggered for pattern: {pattern}")
            if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                user_manager.log_security_event(
                    event_type="suspicious_query_pattern",
                    severity="medium",
                    username=current_user.username,
                    role=current_user.role,
                    event_source="question_guard",
                    details={"pattern": pattern, "question": req.question[:200]}
                )
            return {
                "question": req.question,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": "For security and performance reasons, please specify the columns you need individually rather than requesting 'all' or '*'.",
                "error": None
            }

    try:
        vector_started_at = time.perf_counter()
        candidates = search_vector_index(
            query=req.question,
            index=state.vector_index,
            metadata=state.vector_metadata
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
        sql = await generate_sql(
            question=req.question,
            context=context,
            schema=user_schema,
            history=history,
            vector_candidates=candidates,
            session_id=session_id
        )
        
        # Handle Policy Refusal (SELECT * / SELECT ALL)
        if "policy reasons" in sql.lower() or "not allowed" in sql.lower() or "individually" in sql.lower():
            logger.warning(f"Policy refusal triggered for question: {req.question}")
            return {
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

        if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
            from app.llm_service.llm_service import _get_session_config
            llm_cfg = _get_session_config(session_id)
            user_manager.log_observability_event({
                "username": current_user.username,
                "role": current_user.role,
                "db_name": state.current_connection.get("database", "unknown") if state.current_connection else "unknown",
                "llm_provider": llm_cfg.get("provider"),
                "llm_model": llm_cfg.get("model"),
                "sql_gen_ms": stage_timings.get("sql_generation_ms"),
                "db_exec_ms": None,
                "summary_ms": None,
                "viz_ms": None,
                "total_ms": round((time.perf_counter() - start_time) * 1000, 2),
                "success": False,
                "had_rate_limit": True,
                "rate_limit_stage": "sql_gen",
                "error_stage": "sql_gen"
            })

        return {
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
    except LLMError as e:
        error_str = str(e)
        logger.error(f"LLM error: {error_str}")
        _record_stage_timing(stage_timings, "sql_generation_ms", sql_started_at)
        sql_error_stage = "sql_gen"
        
        if "Validation Error:" in error_str:
            friendly_message = error_str.split("Validation Error: ")[-1]
            if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                from app.llm_service.llm_service import _get_session_config
                llm_cfg = _get_session_config(session_id)
                user_manager.log_observability_event({
                    "username": current_user.username,
                    "role": current_user.role,
                    "db_name": state.current_connection.get("database", "unknown") if state.current_connection else "unknown",
                    "llm_provider": llm_cfg.get("provider"),
                    "llm_model": llm_cfg.get("model"),
                    "sql_gen_ms": stage_timings.get("sql_generation_ms"),
                    "db_exec_ms": None,
                    "summary_ms": None,
                    "viz_ms": None,
                    "total_ms": round((time.perf_counter() - start_time) * 1000, 2),
                    "success": False,
                    "had_rate_limit": False,
                    "rate_limit_stage": None,
                    "error_stage": "sql_gen"
                })
            return {
                "question": req.question,
                "context": context,
                "candidates": candidates,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": f"I cannot fulfill this request. **{friendly_message}**."
            }
            
        raise HTTPException(status_code=503, detail=error_str)

    # Execute SQL
    try:
        execution_started_at = time.perf_counter()
        access_denial = validate_sql_against_rbac(sql, rbac_restrictions)
        if access_denial:
            logger.warning(
                f"RBAC denied SQL for user {current_user.username} in session {session_id}: {access_denial}"
            )
            if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                user_manager.log_security_event(
                    event_type="policy_denial",
                    severity="high",
                    username=current_user.username,
                    role=current_user.role,
                    event_source="rbac_guard",
                    resource_name=access_denial,
                    details={"sql": sql}
                )
            return {
                "question": req.question,
                "sql": None,
                "results": {"columns": [], "rows": []},
                "summary": access_denial,
                "error": "RBAC policy restriction."
            }

        results = execute_sql(sql, session_id=session_id)
        
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
                session_id=session_id
            )
            
            if repaired_sql != sql:
                logger.info("Retrying with repaired SQL...")
                sql = repaired_sql
                access_denial = validate_sql_against_rbac(sql, rbac_restrictions)
                if access_denial:
                    logger.warning(
                        f"RBAC denied repaired SQL for user {current_user.username} in session {session_id}: {access_denial}"
                    )
                    if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
                        user_manager.log_security_event(
                            event_type="policy_denial",
                            severity="high",
                            username=current_user.username,
                            role=current_user.role,
                            event_source="rbac_guard_repair",
                            resource_name=access_denial,
                            details={"sql": sql}
                        )
                    return {
                        "question": req.question,
                        "sql": None,
                        "results": {"columns": [], "rows": []},
                        "summary": access_denial,
                        "error": "RBAC policy restriction."
                    }
                results = execute_sql(sql, session_id=session_id)
        
        logger.info(f"SQL processed (Execution Status: {'Success' if not results.get('error') else 'Failed'})")
        _record_stage_timing(stage_timings, "sql_execution_ms", execution_started_at)
    except Exception as e:
        logger.error(f"Unexpected error during execution: {e}")
        results = {"error": str(e), "columns": [], "rows": []}
        _record_stage_timing(stage_timings, "sql_execution_ms", execution_started_at)

    post_processing_started_at = time.perf_counter()
    summary_payload, viz_recommendation, structured = await asyncio.gather(
        _generate_summary_safe(req.question, results, session_id),
        _recommend_visualization_safe(req.question, results, session_id),
        _extract_structured_memory_safe(req.question, state.chat_history)
    )
    _record_stage_timing(stage_timings, "post_processing_ms", post_processing_started_at)
    summary = summary_payload["summary"]
    summary_rate_limit = summary_payload["rate_limit"]
    stage_timings["summary_ms"] = summary_payload["duration_ms"]
    stage_timings["viz_ms"] = viz_recommendation["duration_ms"]
    viz_result = viz_recommendation["recommendation"]
    had_rate_limit = bool(sql_rate_limit or summary_payload["had_rate_limit"])
    rate_limit_stage = sql_rate_limit["stage"] if sql_rate_limit else ("summary" if summary_payload["had_rate_limit"] else None)
    error_stage = sql_error_stage
    if error_stage is None and results.get("error"):
        error_stage = "db_exec"
    if error_stage is None and summary_payload["error"]:
        error_stage = "summary"
    if error_stage is None and viz_recommendation["error"]:
        error_stage = "viz"

    # Save to memory (Safe)
    try:
        state.chat_history.append({
            "id": int(time.time() * 1000),
            "question": req.question,
            "user": req.question,
            "summary": summary,
            "assistant": summary,
            "sql": sql,
            "results": results,
            "has_error": bool(results.get("error")),
            "visualization": viz_result,
            "structured": structured
        })
        # Trim history to keep context manageable
        if len(state.chat_history) > 5:
            state.chat_history = state.chat_history[-5:]

        # Log to System Audit (Only for Enterprise users)
        if app_state.SYSTEM_MODE == "enterprise" and current_user.role != "SOLO_USER":
            # Audit log (Security/Admin)
            latency = round(time.perf_counter() - start_time, 3)
            user_manager.log_audit(
                username=current_user.username,
                role=current_user.role,
                question=req.question,
                sql_query=sql,
                latency_sec=latency,
                success=not bool(results.get("error"))
            )

            from app.llm_service.llm_service import _get_session_config
            llm_cfg = _get_session_config(session_id)
            user_manager.log_observability_event({
                "username": current_user.username,
                "role": current_user.role,
                "db_name": state.current_connection.get("database", "unknown") if state.current_connection else "unknown",
                "llm_provider": llm_cfg.get("provider"),
                "llm_model": llm_cfg.get("model"),
                "sql_gen_ms": stage_timings.get("sql_generation_ms"),
                "db_exec_ms": stage_timings.get("sql_execution_ms"),
                "summary_ms": stage_timings.get("summary_ms"),
                "viz_ms": stage_timings.get("viz_ms"),
                "total_ms": round((time.perf_counter() - start_time) * 1000, 2),
                "success": not bool(results.get("error")) and error_stage is None,
                "had_rate_limit": had_rate_limit,
                "rate_limit_stage": rate_limit_stage,
                "error_stage": error_stage
            })
            
            # Chat History (User persistence)
            db_name = state.current_connection.get("database", "unknown") if state.current_connection else "unknown"
            user_manager.save_chat_history(
                username=current_user.username,
                db_name=db_name,
                question=req.question,
                sql=sql,
                summary=summary,
                results=results,
                visualization=viz_result
            )
        else:
            logger.info(f"Skipping persistence for session {session_id} (Solo user or non-enterprise context)")
    except Exception as e:
        logger.error(f"Failed to update chat history: {e}")

    stage_timings["total_request_ms"] = round((time.perf_counter() - start_time) * 1000, 2)
    logger.info(f"Query stage timings for session {session_id}: {stage_timings}")

    return {
        "question": req.question,
        "sql": sql,
        "results": results,
        "summary": summary,
        "visualization": viz_result,
        "error": results.get("error"),
        "llm_rate_limit": summary_rate_limit
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
        current_user.username, 
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
    user_manager.clear_chat_history(current_user.username, db_name)
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
        if user_manager.delete_history_item(current_user.username, iid):
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
        history = user_manager.get_chat_history(current_user.username, db_name)
        
    if item_ids is not None:
        target_ids = [int(i.strip()) for i in item_ids.split(",") if i.strip().isdigit()]
        history = [h for h in history if h.get("id") in target_ids]
        
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
        history = user_manager.get_chat_history(current_user.username, db_name)
        
    if item_ids is not None:
        target_ids = [int(i.strip()) for i in item_ids.split(",") if i.strip().isdigit()]
        history = [h for h in history if h.get("id") in target_ids]
        
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


