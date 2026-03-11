import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

def save_to_csv(df: pd.DataFrame, path: Path) -> None:
    logger.info(f"Saving data to {path}")
    df.to_csv(path, index=False)

def append_to_csv(df: pd.DataFrame, path: Path) -> None:
    logger.info(f"Appending data to {path}")

    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)

def read_csv(path: Path) -> pd.DataFrame:
    logger.info(f"Reading data from {path}")
    return pd.read_csv(path)