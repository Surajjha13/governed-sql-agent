import os
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ["SUPABASE_DATABASE_URL"] = ""

from app.llm_service.llm_service import STAR_VALIDATION_MESSAGE, SQL_CACHE, generate_sql


def _run_retry_test(question: str, second_sql: str):
    schema = SimpleNamespace(engine="postgres", database="test_db", tables=[], metrics=[])
    adapter = SimpleNamespace(
        chat_completion=AsyncMock(
            side_effect=[
                "```sql\nSELECT * FROM \"Books\"\n```",
                second_sql,
            ]
        )
    )

    async def run_test():
        SQL_CACHE.cache.clear()
        with patch("app.llm_service.llm_service._get_session_config", return_value={
            "api_key": "test-key",
            "model": "test-model",
            "provider": "groq",
            "base_url": None,
        }), patch(
            "app.llm_service.llm_service.build_prompt",
            return_value="Prompt body"
        ), patch(
            "app.llm_service.llm_service.optimize_sql",
            side_effect=lambda sql, _, engine="postgres": sql
        ), patch(
            "app.llm_service.llm_service.is_valid_sql",
            side_effect=[(False, STAR_VALIDATION_MESSAGE), (True, None)]
        ), patch(
            "app.llm_service.llm_adapters.get_adapter",
            return_value=adapter
        ):
            sql = await generate_sql(
                question=question,
                context={},
                schema=schema,
                session_id="admin_default"
            )
            assert adapter.chat_completion.await_count == 2
            return sql

    return asyncio.run(run_test())


def test_generate_sql_retries_after_select_star_validation():
    sql = _run_retry_test(
        "Which ratings are most common?",
        "```sql\nSELECT \"rating\", COUNT(*) AS \"rating_count\" FROM \"Books\" GROUP BY \"rating\" ORDER BY \"rating_count\" DESC\n```"
    )
    assert "COUNT(*)" in sql


def test_generate_sql_retries_for_trend_questions():
    sql = _run_retry_test(
        "Show monthly sales trend",
        "```sql\nSELECT DATE_TRUNC('month', \"sale_date\") AS \"month\", SUM(\"amount\") AS \"total_sales\" FROM \"Sales\" GROUP BY DATE_TRUNC('month', \"sale_date\") ORDER BY \"month\"\n```"
    )
    assert "DATE_TRUNC" in sql
    assert "SUM" in sql


def test_generate_sql_retries_for_metric_questions():
    sql = _run_retry_test(
        "How many orders do we have?",
        "```sql\nSELECT COUNT(*) AS \"order_count\" FROM \"Orders\"\n```"
    )
    assert "COUNT(*)" in sql


def test_generate_sql_uses_repair_after_second_star_failure():
    schema = SimpleNamespace(
        engine="postgres",
        database="test_db",
        tables=[],
        metrics=[]
    )
    adapter = SimpleNamespace(
        chat_completion=AsyncMock(
            side_effect=[
                "```sql\nSELECT * FROM \"Books\"\n```",
                "```sql\nSELECT * FROM \"Books\"\n```",
                "```sql\nSELECT \"rating\", COUNT(*) AS \"rating_count\" FROM \"Books\" GROUP BY \"rating\" ORDER BY \"rating_count\" DESC\n```",
            ]
        )
    )

    async def run_test():
        SQL_CACHE.cache.clear()
        with patch("app.llm_service.llm_service._get_session_config", return_value={
            "api_key": "test-key",
            "model": "test-model",
            "provider": "groq",
            "base_url": None,
        }), patch(
            "app.llm_service.llm_service.build_prompt",
            return_value="Prompt body"
        ), patch(
            "app.llm_service.llm_service.optimize_sql",
            side_effect=lambda sql, _, engine="postgres": sql
        ), patch(
            "app.llm_service.llm_service.is_valid_sql",
            side_effect=[
                (False, STAR_VALIDATION_MESSAGE),
                (False, STAR_VALIDATION_MESSAGE),
                (True, None),
            ]
        ), patch(
            "app.llm_service.llm_adapters.get_adapter",
            return_value=adapter
        ):
            sql = await generate_sql(
                question="Which ratings are most common?",
                context={"tables": ["Books"], "columns": {"Books": ["rating"]}, "joins": []},
                schema=schema,
                session_id="admin_default"
            )
            assert "COUNT(*)" in sql
            assert adapter.chat_completion.await_count == 3

    asyncio.run(run_test())


def test_generate_sql_mysql_retry_uses_mysql_identifier_guidance():
    schema = SimpleNamespace(engine="mysql", database="test_db", tables=[], metrics=[])
    adapter = SimpleNamespace(
        chat_completion=AsyncMock(
            side_effect=[
                "```sql\nSELECT * FROM `Books`\n```",
                "```sql\nSELECT `rating`, COUNT(*) AS `rating_count` FROM `Books` GROUP BY `rating` ORDER BY `rating_count` DESC\n```",
            ]
        )
    )

    async def run_test():
        SQL_CACHE.cache.clear()
        with patch("app.llm_service.llm_service._get_session_config", return_value={
            "api_key": "test-key",
            "model": "test-model",
            "provider": "groq",
            "base_url": None,
        }), patch(
            "app.llm_service.llm_service.build_prompt",
            return_value="Prompt body"
        ), patch(
            "app.llm_service.llm_service.optimize_sql",
            side_effect=lambda sql, _, engine="postgres": sql
        ), patch(
            "app.llm_service.llm_service.is_valid_sql",
            side_effect=[(False, STAR_VALIDATION_MESSAGE), (True, None)]
        ), patch(
            "app.llm_service.llm_adapters.get_adapter",
            return_value=adapter
        ):
            sql = await generate_sql(
                question="Which ratings are most common?",
                context={},
                schema=schema,
                session_id="admin_default",
                engine="mysql"
            )
            retry_prompt = adapter.chat_completion.await_args_list[1].args[0][1]["content"]
            assert "Use backticks for MySQL identifiers." in retry_prompt
            return sql

    sql = asyncio.run(run_test())
    assert "`rating`" in sql
