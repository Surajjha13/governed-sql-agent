import os
import sys
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

# Set up logging for SQLAlchemy
logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Ensure the root directory is in the sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv(".env")

print("🔄 Loading models from app.auth.models...")
from app.auth.database import Base
from app.auth.models import (
    EnterpriseModel, UserModel, AuthSessionModel, RoleModel, AuditLogModel, 
    ChatHistoryModel, ObservabilityEventModel, AdminActionLogModel, SecurityEventModel
)
print("✅ Models loaded.")

def migrate():
    print("🚀 Starting SQL Agent migration from SQLite to Supabase...")
    
    # 1. Connect to SQLite
    sqlite_url = "sqlite:///./users.db"
    print(f"🔗 Connecting to local SQLite: {sqlite_url}")
    sqlite_engine = create_engine(sqlite_url)
    SqliteSession = sessionmaker(bind=sqlite_engine)
    
    # 2. Connect to Supabase
    supabase_url = os.getenv("SUPABASE_DATABASE_URL")
    if not supabase_url:
        print("❌ Error: SUPABASE_DATABASE_URL not found in .env")
        return
        
    if supabase_url.startswith("postgres://"):
        supabase_url = supabase_url.replace("postgres://", "postgresql://", 1)
        
    print(f"🔌 Connecting to Supabase (PostgreSQL)...")
    try:
        supabase_engine = create_engine(supabase_url, connect_args={'connect_timeout': 10})
        # Test connection
        with supabase_engine.connect() as conn:
            print("✅ Supabase connection verified!")
    except Exception as e:
        print(f"❌ Failed to connect to Supabase: {e}")
        return

    SupabaseSession = sessionmaker(bind=supabase_engine)
    
    # 3. Create tables in Supabase
    print("🏗️  Initializing schema in Supabase (create_all)...")
    try:
        Base.metadata.create_all(bind=supabase_engine)
        print("✅ Tables created or already exist.")
    except Exception as e:
        print(f"❌ Error during schema initialization: {e}")
        return
    
    sqlite_session = SqliteSession()
    supabase_session = SupabaseSession()
    
    # 4. Migrate data ordered by foreign key dependencies
    tables_to_migrate = [
        EnterpriseModel,
        UserModel,
        AuthSessionModel,
        RoleModel,
        AuditLogModel,
        ChatHistoryModel,
        ObservabilityEventModel,
        AdminActionLogModel,
        SecurityEventModel
    ]
    
    for model in tables_to_migrate:
        table_name = model.__tablename__
        print(f"\n📦 Table: {table_name}")
        
        try:
            # Get all records from sqlite
            records = sqlite_session.query(model).all()
            print(f"  ├─ Found {len(records)} local records.")
            
            if not records:
                print("  └─ Skipping (empty).")
                continue
                
            print(f"  ├─ Transferring {len(records)} records to Supabase...")
            for count, record in enumerate(records, 1):
                # Expunge from sqlite session and merge into supabase
                sqlite_session.expunge(record)
                supabase_session.merge(record)
                
                if count % 50 == 0:
                    supabase_session.commit()
                    print(f"  │  Merged {count}...")
                    
            supabase_session.commit()
            print(f"  └─ ✅ SUCCESS: Migrated {len(records)} records.")
        except Exception as e:
            supabase_session.rollback()
            print(f"  └─ ❌ FAILED: {e}")
            
    print("\n🎉 Migration process complete!")
    sqlite_session.close()
    supabase_session.close()
    
if __name__ == "__main__":
    migrate()
