import logging
import pandas as pd
from datetime import datetime, timezone

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transform: normalizing column names to lowercase")
    df = df.copy()
    df.columns = df.columns.str.lower()
    if "load_timestamp" in df.columns:
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"], errors="coerce")
    else:
        df["load_timestamp"] = datetime.now(timezone.utc).isoformat()
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"], errors="coerce")
    return df
