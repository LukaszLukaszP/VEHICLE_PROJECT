import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict
from src.utils.logger import get_logger

logger = get_logger(__name__)

def normalize_makes(raw_makes: List[Dict]) -> pd.DataFrame:
    if not raw_makes:
        raise ValueError("No vehicle makes data provided for normalization.")

    df = pd.DataFrame(raw_makes)

    required_columns = ["Make_ID", "Make_Name"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = (
        df[required_columns]
        .drop_duplicates(subset=["Make_ID"])
        .sort_values("Make_Name")
        .reset_index(drop=True)
    )

    df = df.rename(columns={
        "Make_ID": "make_id",
        "Make_Name": "make_name"
    })

    df["load_timestamp"] = datetime.now(timezone.utc).isoformat()

    invalid_id = (~df["make_id"].apply(lambda x: isinstance(x, int))) | (df["make_id"] <= 0)
    invalid_name = ~df["make_name"].apply(lambda x: isinstance(x, str) and x.strip() != "")

    if invalid_id.any() or invalid_name.any():
        logger.warning(
            f"Validation issues detected: invalid_id={invalid_id.sum()}, invalid_name={invalid_name.sum()}"
        )

    df = df.rename(columns={
        "Make_ID": "make_id",
        "Make_Name": "make_name"
    })

    return df

def normalize_models(raw_models: List[Dict]) -> pd.DataFrame:
    if not raw_models:
        raise ValueError("No vehicle models data provided for normalization.")

    df = pd.DataFrame(raw_models)

    required_columns = ["Make_ID", "Make_Name", "Model_ID", "Model_Name"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = (
        df[required_columns]
        .drop_duplicates(subset=["Model_ID"])
        .sort_values(["Make_Name", "Model_Name"])
        .reset_index(drop=True)
    )

    df["load_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Basic validation checks
    invalid_make_id = (~df["Make_ID"].apply(lambda x: isinstance(x, int))) | (df["Make_ID"] <= 0)
    invalid_model_id = (~df["Model_ID"].apply(lambda x: isinstance(x, int))) | (df["Model_ID"] <= 0)
    invalid_make_name = ~df["Make_Name"].apply(lambda x: isinstance(x, str) and x.strip() != "")
    invalid_model_name = ~df["Model_Name"].apply(lambda x: isinstance(x, str) and x.strip() != "")

    if invalid_make_id.any() or invalid_model_id.any() or invalid_make_name.any() or invalid_model_name.any():
        logger.warning(
            "Validation issues detected in models: "
            f"invalid_make_id={int(invalid_make_id.sum())}, "
            f"invalid_model_id={int(invalid_model_id.sum())}, "
            f"invalid_make_name={int(invalid_make_name.sum())}, "
            f"invalid_model_name={int(invalid_model_name.sum())}"
        )

    df = df.rename(columns={
        "Make_ID": "make_id",
        "Make_Name": "make_name",
        "Model_ID": "model_id",
        "Model_Name": "model_name",
    })

    df["make_name"] = df["make_name"].astype(str).str.strip()
    df["model_name"] = df["model_name"].astype(str).str.strip()

    return df