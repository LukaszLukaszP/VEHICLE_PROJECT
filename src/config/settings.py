from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = BASE_DIR / "logs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

MAKES_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"
MODELS_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeId/{}?format=json"

MAKES_FILE = RAW_DIR / "makes.csv"
MODELS_FILE = RAW_DIR / "models.csv"

MODELS_LIMIT = int(os.getenv("MODELS_LIMIT", 100))