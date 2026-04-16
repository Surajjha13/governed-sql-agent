from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
if SUPABASE_URL:
    if SUPABASE_URL.startswith("postgres://"):
        SUPABASE_URL = SUPABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(SUPABASE_URL)
else:
    SQLALCHEMY_DATABASE_URL = "sqlite:///./users.db"
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
