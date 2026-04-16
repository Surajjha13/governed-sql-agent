import pytest

from app.auth.user_manager import user_manager
from app.llm_service.security import (
    redact_results_for_summary,
    validate_provider_base_url,
)


def test_last_connection_does_not_persist_password():
    username = "security_test_user"
    password = "TempPassword123!"
    role = "VIEWER"

    try:
        user_manager.create_user(username, password, role)
        user_manager.update_last_connection(
            username,
            {
                "engine": "postgres",
                "host": "db.example.com",
                "port": 5432,
                "database": "analytics",
                "user": "readonly_user",
                "password": "super-secret",
            },
        )

        stored = user_manager.get_last_connection(username)
        assert stored["engine"] == "postgres"
        assert stored["host"] == "db.example.com"
        assert stored["user"] == "readonly_user"
        assert "password" not in stored
    finally:
        user_manager.delete_user(username)


def test_validate_provider_base_url_blocks_private_hosts():
    with pytest.raises(ValueError):
        validate_provider_base_url("custom", "https://localhost:11434/v1/chat/completions")

    with pytest.raises(ValueError):
        validate_provider_base_url("openai", "https://10.0.0.5/v1/chat/completions")


def test_redact_results_for_summary_masks_sensitive_columns():
    payload = {
        "columns": ["email", "total"],
        "rows": [{"email": "user@example.com", "total": 42}],
    }

    redacted = redact_results_for_summary(payload)
    assert redacted["rows"][0]["email"] == "[REDACTED]"
    assert redacted["rows"][0]["total"] == 42


def test_redact_schema_keeps_business_email_fields_for_query_generation():
    from app.llm_service.security import redact_schema
    from app.schema_service.models import SchemaResponse, TableMeta, ColumnMeta

    schema = SchemaResponse(
        engine="mysql",
        database="sakila",
        tables=[
            TableMeta(
                schema="sakila",
                table="customer",
                columns=[
                    ColumnMeta(name="full_name", data_type="varchar", nullable=False),
                    ColumnMeta(name="email", data_type="varchar", nullable=True, sensitive=True),
                    ColumnMeta(name="password_hash", data_type="varchar", nullable=True, sensitive=True),
                ],
            )
        ],
        metrics=[],
    )

    redacted = redact_schema(schema)
    cols = [c.name for c in redacted.tables[0].columns]

    assert "email" in cols
    assert "password_hash" not in cols
