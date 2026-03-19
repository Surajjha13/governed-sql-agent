import pyodbc
from app.schema_service.models import ColumnMeta, TableMeta


def introspect_sqlserver(conn):
    connection = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn.host},{conn.port};"
        f"DATABASE={conn.database};"
        f"UID={conn.user};"
        f"PWD={conn.password};"
        "TrustServerCertificate=yes;"
    )
    cur = connection.cursor()

    # Columns
    cur.execute("""
        SELECT s.name, t.name, c.name, ty.name, c.is_nullable
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.columns c ON t.object_id = c.object_id
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        ORDER BY s.name, t.name, c.column_id
    """)
    columns = cur.fetchall()

    # Primary keys
    cur.execute("""
        SELECT s.name, t.name, c.name
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.is_primary_key = 1
    """)
    pk_set = {(r[0], r[1], r[2]) for r in cur.fetchall()}

    # Foreign keys
    cur.execute("""
        SELECT
            sch1.name, tab1.name, col1.name,
            sch2.name, tab2.name, col2.name
        FROM sys.foreign_key_columns fkc
        JOIN sys.tables tab1 ON fkc.parent_object_id = tab1.object_id
        JOIN sys.schemas sch1 ON tab1.schema_id = sch1.schema_id
        JOIN sys.columns col1 ON fkc.parent_object_id = col1.object_id AND fkc.parent_column_id = col1.column_id
        JOIN sys.tables tab2 ON fkc.referenced_object_id = tab2.object_id
        JOIN sys.schemas sch2 ON tab2.schema_id = sch2.schema_id
        JOIN sys.columns col2 ON fkc.referenced_object_id = col2.object_id AND fkc.referenced_column_id = col2.column_id
    """)
    fk_map = {
        (r[0], r[1], r[2]): f"{r[3]}.{r[4]}.{r[5]}"
        for r in cur.fetchall()
    }

    connection.close()

    tables = {}
    for schema, table, col, dtype, nullable in columns:
        key = (schema, table)
        tables.setdefault(key, []).append(
            ColumnMeta(
                name=col,
                data_type=dtype,
                nullable=bool(nullable),
                is_primary_key=(schema, table, col) in pk_set,
                foreign_key=fk_map.get((schema, table, col))
            )
        )

    return [
        TableMeta(schema_name=k[0], table=k[1], columns=v)
        for k, v in tables.items()
    ]
