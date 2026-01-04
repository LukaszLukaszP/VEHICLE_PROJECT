from pathlib import Path
import logging
import pandas as pd

def extract_csv(csv_path: Path | str):
    scv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {csv_path}")

    try:
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            logging.info(f"Extracted dataframe from csv: rows={df.shape[0]}, columns={df.shape[1]}")
            return df
    except Exception as e:
        logging.exception(f"Error while reading CSV file {csv_path}: {e}")
        raise
