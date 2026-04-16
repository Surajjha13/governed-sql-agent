import pymysql
import logging
from app.schema_service.models import ColumnMeta, TableMeta

logger = logging.getLogger(__name__)

def introspect_mysql(conn):
    connection = pymysql.connect(
        host=conn.host,
        port=conn.port,
        user=conn.user,
        password=conn.password,
        database=conn.database,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            # Use DATABASE() to get the current schema name if conn.database is subtly different (case-sensitivity)
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()['DATABASE()']
            logger.info(f"Introspecting MySQL database: {current_db} (Requested: {conn.database})")

            cursor.execute("""
                SELECT 
                    TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s OR TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """, (current_db, conn.database))
            columns = cursor.fetchall()
            logger.info(f"Found {len(columns)} columns in information_schema.COLUMNS")
            
            cursor.execute("""
                SELECT
                    TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE (TABLE_SCHEMA = %s OR TABLE_SCHEMA = %s) AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (current_db, conn.database))
            fks = cursor.fetchall()
            fk_map = {
                (f['TABLE_NAME'], f['COLUMN_NAME']): f"{f['REFERENCED_TABLE_SCHEMA']}.{f['REFERENCED_TABLE_NAME']}.{f['REFERENCED_COLUMN_NAME']}"
                for f in fks
            }
            
            tables = {}
            for col in columns:
                table_name = col['TABLE_NAME']
                col_name = col['COLUMN_NAME']
                key = (col['TABLE_SCHEMA'], table_name)
                
                if key not in tables:
                    tables[key] = []
                    
                tables[key].append(ColumnMeta(
                    name=col_name,
                    data_type=col['DATA_TYPE'],
                    nullable=(col['IS_NULLABLE'] == "YES"),
                    is_primary_key=(col['COLUMN_KEY'] == "PRI"),
                    foreign_key=fk_map.get((table_name, col_name))
                ))
                
            return [
                TableMeta(schema=k[0], table=k[1], columns=v)
                for k, v in tables.items()
            ]
            
    finally:
        connection.close()
