import pandas as pd
from pathlib import Path
import logging
from sqlalchemy import text
from src.load.db import get_engine
from src.load.loaders import load_dataframe_to_db
from src.load.extract import extract_csv
from src.load.transform import transform_df

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
MAKES_FILE = DATA_DIR / "raw" / "makes.csv"
MODELS_FILE = DATA_DIR / "raw" / "models.csv"
CREATE_TABLES_FILE = BASE_DIR / "SQL" / "create_tables.sql"

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler()
    ]
)

logging.info("ETL process start")

def run_ddl():
    logging.info("run_ddl() start")
    engine = get_engine()
    ddl_file = CREATE_TABLES_FILE
    try:
        with open(ddl_file, "r", encoding="UTF-8") as f:
            sql_command = f.read()
        with engine.begin() as connection:
            logging.info("Make connection and create tables")
            connection.execute(text(sql_command))
            logging.info("Tables created")
        logging.info("run_ddl() finished")
    except Exception as e:
        logging.exception(f"Error while running ddl script, Error: {e}")
        raise

def run_etl():
    logging.info("run_etl() started")
    try:    
        run_ddl()
        df_makes = extract_csv(MAKES_FILE)
        df_makes = transform_df(df_makes)
        load_dataframe_to_db(df_makes, table_name="makes", key_cols=["make_id"])

        df_models = extract_csv(MODELS_FILE)
        df_models = transform_df(df_models)
        load_dataframe_to_db(df_models, table_name="models", key_cols=["model_id"])
        logging.info("ETL finished successfully")
    except Exception as e:
        logging.exception(f"Error while running etl, Error: {e}")
        raise

if __name__ == "__main__":
    run_etl()