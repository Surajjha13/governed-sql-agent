from pydantic import BaseModel, Field
from typing import List, Optional


class DBConnectionRequest(BaseModel):
    engine: str          # "postgres" | "mysql" | "sqlserver"
    host: str
    port: int
    database: str
    user: str
    password: str


class ColumnMeta(BaseModel):
    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool = False
    foreign_key: Optional[str] = None
    semantic_type: str = "dimension"
    sensitive: bool = False
    description: Optional[str] = None


class TableMeta(BaseModel):
    schema_name: str = Field(alias="schema")
    table: str
    columns: List[ColumnMeta]

    class Config:
        populate_by_name = True


class MetricDefinition(BaseModel):
    name: str
    description: str
    sql_template: str
    required_tables: List[str]


class SchemaResponse(BaseModel):
    engine: str
    database: str
    tables: List[TableMeta]
    metrics: List[MetricDefinition] = []
