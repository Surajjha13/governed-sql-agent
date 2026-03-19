import psycopg2
from app.schema_service.models import ColumnMeta, TableMeta


def introspect_postgres(conn):
    connection = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.database,
        user=conn.user,
        password=conn.password
    )
    cur = connection.cursor()

    # Columns
    cur.execute("""
        SELECT table_schema, table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position
    """)
    columns = cur.fetchall()

    # Primary keys
    cur.execute("""
        SELECT tc.table_schema, tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
    """)
    pk_set = {(r[0], r[1], r[2]) for r in cur.fetchall()}

    # Foreign keys
    cur.execute("""
        SELECT
            tc.table_schema, tc.table_name, kcu.column_name,
            ccu.table_schema, ccu.table_name, ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
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
                nullable=(nullable == "YES"),
                is_primary_key=(schema, table, col) in pk_set,
                foreign_key=fk_map.get((schema, table, col))
            )
        )

    return [
        TableMeta(schema_name=k[0], table=k[1], columns=v)
        for k, v in tables.items()
    ]
