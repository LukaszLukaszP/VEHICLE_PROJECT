import requests
import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/etl.log"),   # zapis do pliku
        logging.StreamHandler()           # wyświetlanie w konsoli
    ]
)

BASE_DIR = Path(__file__).resolve().parent[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MAKES_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"
MODELS_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeId/{}?format=json"
OUTPUT_FILE = DATA_DIR / "raw/makes.csv"
SAMPLE_JSON = DATA_DIR / "raw/makes_sample.json"
OUTPUT_MODELS_FILE = DATA_DIR / "raw/models.csv"
MODELS_LIMIT = 100

def save_raw_json(data: dict, path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info(f"Zapisano surowy JSON do: {path}")

def fetch_makes() -> list[dict]:
    """Pobierz listę marek z API NHTSA i zwróć jako listę słowników."""
    logging.info(f"Pobieram dane z: {MAKES_URL}")
    try:
        response = requests.get(MAKES_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Błąd pobierania marek: {e}")
        return []
    
    data = response.json()
    # save_raw_json(data, SAMPLE_JSON)
    results = data.get("Results", [])
    logging.info(f"Pobrano {len(results)} rekordów.")
    return results
    

def fetch_models_for_make(make_ids: list[int]) -> list[dict]:
    all_results = []

    for make_id in make_ids:
        url = MODELS_URL.format(make_id)
        logging.info(f"Pobieram modele dla marki {make_id} z: {url}") 
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Błąd pobierania modeli dla Make_ID={make_id}: {e}")
            continue

        data = response.json()
        results = data.get("Results", [])
        if not results:
            logging.warning(f"Brak modeli dla Make_ID={make_id}")
        all_results.extend(results)
    return all_results

def normalize_makes(raw_makes: list[dict]) -> pd.DataFrame:
    if not raw_makes:
        raise ValueError("Brak danych o markach do przetworzenia.")

    df = pd.DataFrame(raw_makes)

    expected_cols = ["Make_ID", "Make_Name"]
    df = df[expected_cols]

    df = df.drop_duplicates(subset=["Make_ID"]).sort_values("Make_Name").reset_index(drop=True)
    df["load_timestamp"] = datetime.now(timezone.utc).isoformat()#datetime.now(datetime.astimezone).isoformat()

    mask_id = df['Make_ID'].apply(lambda x: not isinstance(x, int) or x <= 0)
    for idx in df[mask_id].index:
        logging.warning(f"Incorrect Make_ID detected at index: {idx}")

    mask_make_name = df['Make_Name'].apply(lambda x: not (isinstance(x, str) and x.strip() != ""))
    for idx in df[mask_make_name].index:
        logging.warning(f"Incorrect Make_Name detected at index: {idx}")

    return df   

def normalize_models(raw_models: list[dict]) -> pd.DataFrame:
    if not raw_models:
        raise ValueError("Brak danych o modelach do przetworzenia.")
    
    df_1 = pd.DataFrame(raw_models)
    expected_cols = ["Make_ID", "Make_Name", "Model_ID", "Model_Name"]
    df_1 = df_1[expected_cols]

    df_1 = df_1.drop_duplicates(subset=["Model_ID"]).sort_values(["Make_Name", "Model_Name"]).reset_index(drop=True)
    return df_1

def get_make_ids_to_fetch(df_makes: pd.DataFrame, models_path: Path, limit: int | None = None) -> list[int]:
    """
    Zwraca listę Make_ID, dla których jeszcze nie mamy modeli w pliku models.csv.
    Jeśli limit jest podany, zwróci maksymalnie 'limit' ID.
    """
    if models_path.exists():
        existing = pd.read_csv(models_path)
        if "Make_ID" in existing.columns:
            already_have = set(existing["Make_ID"].unique())
        else:
            already_have = set()
    else:
        already_have = set()

    all_make_ids = df_makes["Make_ID"].tolist()

    candidates = [mid for mid in all_make_ids if mid not in already_have]

    if limit is not None:
        candidates = candidates[:limit]

    logging.info(
        f"Do pobrania modeli dla {len(candidates)} marek (limit={limit})."
    )
    return candidates

def save_makes(df: pd.DataFrame, path: Path) -> None:
    """Zapisz DataFrame z markami do pliku CSV."""
    df.to_csv(path, index=False)
    logging.info(f"Zapisano {len(df)} marek do pliku: {path}")

def save_models(df: pd.DataFrame, path: Path) -> None:
    """Zapisz DataFrame z modelami do pliku CSV."""
    df.to_csv(path, index=False)
    logging.info(f"Zapisano {len(df)} modeli do pliku: {path}")

def save_models_incremental(df_new: pd.DataFrame, path: Path) -> None:
    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, df_new], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Model_ID"]).reset_index(drop=True)
    else:
        combined = df_new.copy()
    
    combined.to_csv(path, index=False)
    logging. info(f"Zapisano {len(df_new)} nowych modeli, łącznie {len(combined)} modeli w pliku: {path}")

def count_models_per_make(makes: list, df_models: pd.DataFrame) -> pd.DataFrame:
    cols = ["Make_Name", "Count"]
    counts = df_models["Make_Name"].value_counts()
    rows = [(make, counts.get(make, 0)) for make in makes]
    return pd.DataFrame(rows, columns=cols)

def main() -> None:
    raw_makes = fetch_makes()
    df_makes = normalize_makes(raw_makes)
    logging.info("\nPodgląd pierwszych 10 marek:\n%s", df_makes.head(10))
    save_makes(df_makes, OUTPUT_FILE)
    make_ids_to_fetch = get_make_ids_to_fetch(df_makes, OUTPUT_MODELS_FILE, limit=MODELS_LIMIT)
    if not make_ids_to_fetch:
        logging.info("Brak nowych marek do pobrania modeli. Kończę.")
        return
    
    raw_models = fetch_models_for_make(make_ids_to_fetch)
    df_models = normalize_models(raw_models)
    logging.info("\nPodgląd pierwszych 10 modeli:\n%s",df_models.head(10))  
    logging.info("\nLiczba modeli na markę:\n%s'," , count_models_per_make(df_makes["Make_Name"].tolist(), df_models))
    #save_models(df_models, OUTPUT_MODELS_FILE)
    save_models_incremental(df_models, OUTPUT_MODELS_FILE)


if __name__ == "__main__":
    main()
