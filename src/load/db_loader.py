import pandas as pd
from sqlalchemy import text
from src.load.db import get_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_dataframe_to_db(
    df: pd.DataFrame,
    table_name: str,
    key_cols: list[str],
    staging_table: str | None = None,
) -> None:

    if df is None or df.empty:
        logger.warning(f"[load] DataFrame is empty. Skipping load to '{table_name}'.")
        return

    staging_table = staging_table or f"stg_{table_name}"
    engine = get_engine()

    cols = list(df.columns)

    missing_keys = [c for c in key_cols if c not in cols]
    if missing_keys:
        raise ValueError(f"Missing key columns in df: {missing_keys}. df columns={cols}")

    non_key_cols = [c for c in cols if c not in key_cols]

    insert_cols_sql = ", ".join(cols)
    select_cols_sql = ", ".join(cols)
    conflict_cols_sql = ", ".join(key_cols)

    if non_key_cols:
        set_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_key_cols])
        upsert_sql = f"""
            INSERT INTO {table_name} ({insert_cols_sql})
            SELECT {select_cols_sql}
            FROM {staging_table}
            ON CONFLICT ({conflict_cols_sql})
            DO UPDATE SET {set_sql};
        """
    else:
        upsert_sql = f"""
            INSERT INTO {table_name} ({insert_cols_sql})
            SELECT {select_cols_sql}
            FROM {staging_table}
            ON CONFLICT ({conflict_cols_sql})
            DO NOTHING;
        """

    try:
        with engine.begin() as conn:
            logger.info(f"[load] Writing {len(df)} rows to staging table '{staging_table}' (replace).")
            df.to_sql(staging_table, con=conn, if_exists="replace", index=False)

            logger.info(f"[load] Upserting from '{staging_table}' into '{table_name}' on key={key_cols}.")
            logger.info(upsert_sql)
            conn.execute(text(upsert_sql))

        logger.info(f"[load] Load finished for target table '{table_name}'.")
    except Exception as e:
        logger.exception(f"[load] Error while loading into '{table_name}': {e}")
        raise