import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_db_config():
    return {
        "host": os.getenv("PGHOST"),
        "port": int(os.getenv("PGPORT")),
        "database": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
    }

def get_engine():
    config = get_db_config()
    db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    engine = create_engine(db_url)
    return engine
