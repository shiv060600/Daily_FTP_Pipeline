import os
import urllib.parse
from contextlib import contextmanager
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import Engine
load_dotenv()

@contextmanager
def get_db():
    """Context manager that yields engine and auto-closes it"""
    SERVER = os.getenv("SERVER")
    DB = os.getenv("DATABASE")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    
    # Check if all required environment variables are set
    if not all([SERVER, DB, USER, PASSWORD]):
        missing = []
        if not SERVER: missing.append("SERVER")
        if not DB: missing.append("DATABASE")
        if not USER: missing.append("DB_USER")
        if not PASSWORD: missing.append("DB_PASSWORD")
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    conn_string = f"mssql+pymssql://{urllib.parse.quote_plus(USER)}:{urllib.parse.quote_plus(PASSWORD)}@{SERVER}/{DB}"
    
    engine = sqlalchemy.create_engine(
        conn_string,
        connect_args={'timeout': 30, 'login_timeout': 60},
        pool_recycle=3600
    )
    
    try:
        yield engine
    finally:
        engine.dispose()
