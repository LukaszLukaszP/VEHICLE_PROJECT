import pandas as pd
from pathlib import Path
import logging
from sqlalchemy import text
from db import get_engine
from laders import load_dataframe_to_db  


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

Path("logs").mkdir(exist_ok=True)

logging.info("ETL process start")
def run_ddl():
    logging.info("run_ddl() start")
    engine = get_engine()
    ddl_file = Path(__file__).resolve().parent / "SQL" / "create_tables.sql"
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
    logging.info("ETL started")
    try:    
        run_ddl()
        df = extract_csv()
        df = transform_df(df)
        load_dataframe_to_db(df)
        logging.info("ETL finished successfully")
    except Exception as e:
        logging.exception(f"Error while running etl, Error: {e}")
        raise

if __name__ == "__main__":
    run_etl()