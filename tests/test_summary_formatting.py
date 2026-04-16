import os

os.environ["SUPABASE_DATABASE_URL"] = ""

from app.query_service.api import _build_fast_summary, _build_row_limit_guidance
from app.query_service.prompt_builder import build_summary_prompt_compact


def test_fast_summary_avoids_preview_row_headline_for_large_results():
    summary = _build_fast_summary(
        "show all customers",
        {
            "columns": ["full_name", "email"],
            "rows": [
                {"full_name": "MARY SMITH", "email": "MARY.SMITH@example.org"},
                {"full_name": "PATRICIA JOHNSON", "email": "PATRICIA.JOHNSON@example.org"},
            ],
            "returned_rows": 10,
            "total_count": 599,
            "truncated": True,
            "row_limit": 10,
        },
    )

    assert "Matched 599 records" in summary
    assert "full name and email" in summary.lower()
    assert "preview rows" not in summary.lower()
    assert "server row limit" not in summary.lower()


def test_row_limit_guidance_is_secondary_and_user_friendly():
    guidance = _build_row_limit_guidance(
        {
            "truncated": True,
            "total_count": 599,
            "row_limit": 10,
        }
    )

    assert guidance == "A preview is shown here. Export the full result to review all 599 matching records."


def test_compact_summary_prompt_discourages_preview_chatter():
    prompt = build_summary_prompt_compact(
        "show all customers",
        [{"full_name": "MARY SMITH", "email": "mary@example.org"}],
        total_count=599,
    )

    assert "business meaning" in prompt
    assert "avoid talking about preview rows or UI limits" in prompt
