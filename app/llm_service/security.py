import ipaddress
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from app.schema_service.models import SchemaResponse, TableMeta, ColumnMeta, MetricDefinition


SENSITIVE_FIELD_MARKERS = (
    "password",
    "passwd",
    "secret",
    "api_token",
    "auth_token",
    "ssn",
    "social_security",
    "street_address",
    "home_address",
    "mailing_address",
    "dob",
    "date_of_birth",
    "salary",
    "credit_card",
    "card_number",
    "pan_number",
    "aadhar",
)

SUMMARY_REDACTION_MARKERS = SENSITIVE_FIELD_MARKERS + (
    "email",
    "email_address",
    "phone",
    "phone_number",
    "mobile",
    "mobile_number",
)


def is_sensitive_field(name: str) -> bool:
    lowered = (name or "").lower()
    return any(marker in lowered for marker in SENSITIVE_FIELD_MARKERS)


def should_redact_summary_field(name: str) -> bool:
    lowered = (name or "").lower()
    return any(marker in lowered for marker in SUMMARY_REDACTION_MARKERS)


def redact_schema(schema: SchemaResponse) -> SchemaResponse:
    safe_tables = []
    for table in schema.tables:
        safe_columns = [
            ColumnMeta(**column.model_dump())
            for column in table.columns
            if not is_sensitive_field(column.name)
        ]
        safe_tables.append(
            TableMeta(
                schema=table.schema_name,
                table=table.table,
                columns=safe_columns,
            )
        )

    safe_metrics = []
    for metric in getattr(schema, "metrics", []) or []:
        safe_metrics.append(MetricDefinition(**metric.model_dump()))

    return SchemaResponse(
        engine=schema.engine,
        database=schema.database,
        tables=safe_tables,
        metrics=safe_metrics,
    )


def redact_vector_candidates(candidates: Optional[list[Dict[str, Any]]]) -> list[Dict[str, Any]]:
    safe_candidates = []
    for candidate in candidates or []:
        column_name = candidate.get("column")
        if column_name and is_sensitive_field(column_name):
            continue
        safe_candidates.append(dict(candidate))
    return safe_candidates


def redact_history_for_llm(history: Optional[list[Dict[str, Any]]]) -> list[Dict[str, Any]]:
    safe_history = []
    for item in history or []:
        safe_history.append(
            {
                "user": str(item.get("user") or item.get("question") or "")[:500],
                "assistant": str(item.get("assistant") or item.get("summary") or "")[:1000],
            }
        )
    return safe_history


def redact_results_for_summary(data: Any) -> Any:
    if isinstance(data, list):
        safe_rows = []
        for row in data[:10]:
            if isinstance(row, dict):
                safe_rows.append(
                    {
                        key: ("[REDACTED]" if should_redact_summary_field(str(key)) else value)
                        for key, value in row.items()
                    }
                )
            else:
                safe_rows.append(row)
        return safe_rows
    if isinstance(data, dict):
        safe_data = {}
        for key, value in data.items():
            if key == "rows" and isinstance(value, list):
                safe_data[key] = redact_results_for_summary(value)
            elif should_redact_summary_field(str(key)):
                safe_data[key] = "[REDACTED]"
            else:
                safe_data[key] = value
        return safe_data
    return data


def validate_provider_base_url(provider: str, base_url: Optional[str]) -> Optional[str]:
    if not base_url:
        return None

    parsed = urlparse(base_url)
    if parsed.scheme not in {"https"}:
        raise ValueError("Custom provider URLs must use HTTPS.")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Custom provider URL must include a valid hostname.")

    lowered_host = hostname.lower()
    if lowered_host in {"localhost"} or lowered_host.endswith(".local"):
        raise ValueError("Local provider URLs are not allowed.")

    try:
        ip = ipaddress.ip_address(lowered_host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
            raise ValueError("Private or local provider IPs are not allowed.")
    except ValueError as exc:
        if "are not allowed" in str(exc):
            raise

    provider = (provider or "").lower()
    allow_custom = os.getenv("ALLOW_CUSTOM_LLM_BASE_URLS", "false").lower() == "true"
    approved_hosts = {
        "groq": ("api.groq.com",),
        "openai": ("api.openai.com",),
        "gemini": ("generativelanguage.googleapis.com",),
        "deepseek": ("api.deepseek.com",),
        "anthropic": ("api.anthropic.com",),
    }
    if provider != "custom":
        allowed = approved_hosts.get(provider, ())
        if allowed and lowered_host not in allowed:
            raise ValueError(f"Custom base URL is not allowed for provider '{provider}'.")
    elif not allow_custom:
        raise ValueError("Custom LLM provider URLs are disabled.")

    return base_url
