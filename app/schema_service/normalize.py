from typing import List
from app.schema_service.models import (
    SchemaResponse,
    TableMeta,
    ColumnMeta,
)



TIME_KEYWORDS = {
    "date", "time", "timestamp", "created", "updated",
    "modified", "order_date", "created_at", "updated_at"
}

SENSITIVE_KEYWORDS = {
    "email", "phone", "mobile", "password", "salary",
    "ssn", "aadhaar", "pan", "credit", "card"
}

METRIC_TYPES = {
    "int", "integer", "bigint", "smallint",
    "numeric", "decimal", "float", "double",
    "real", "money"
}


def infer_semantic_type(col: ColumnMeta) -> str:
    name = col.name.lower()
    dtype = col.data_type.lower()

    if col.is_primary_key:
        return "id"

    if col.foreign_key:
        return "foreign_key"

    if any(k in name for k in TIME_KEYWORDS):
        return "time"

    if dtype in METRIC_TYPES:
        return "metric"

    return "dimension"


def infer_sensitive(col_name: str) -> bool:
    name = col_name.lower()
    return any(k in name for k in SENSITIVE_KEYWORDS)


def auto_description(table: str, col: ColumnMeta) -> str:
    if col.is_primary_key:
        return f"Primary identifier for {table}"
    if col.foreign_key:
        return f"Reference to related entity from {table}"
    if col.semantic_type == "time":
        return f"Timestamp associated with {table}"
    if col.semantic_type == "metric":
        return f"Numeric measure used for analysis in {table}"
    return f"Descriptive attribute of {table}"


# -----------------------------
# Public API
# -----------------------------

def normalize_schema(schema: SchemaResponse) -> SchemaResponse:
    """
    Enriches raw schema with semantic meaning.
    This layer is PURE and deterministic.
    """

    normalized_tables: List[TableMeta] = []

    for table in schema.tables:
        normalized_columns: List[ColumnMeta] = []

        for col in table.columns:
            semantic_type = infer_semantic_type(col)
            sensitive = infer_sensitive(col.name)
            
            normalized_col = ColumnMeta(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                is_primary_key=col.is_primary_key,
                foreign_key=col.foreign_key,
                semantic_type=semantic_type,
                sensitive=sensitive
            )
            # description depends on semantic_type being set
            normalized_col.description = auto_description(table.table, normalized_col)
            
            normalized_columns.append(normalized_col)

        normalized_tables.append(
            TableMeta(
                schema_name=table.schema_name,
                table=table.table,
                columns=normalized_columns
            )
        )

    return SchemaResponse(
        engine=schema.engine,
        database=schema.database,
        tables=normalized_tables
    )
