import requests
from typing import List, Dict
from src.config.settings import MAKES_URL, MODELS_URL
from src.utils.logger import get_logger

logger = get_logger(__name__)

class NHTSAClient:
    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def fetch_makes(self) -> list[dict]:
        logger.info("Fetching vehicle makes from NHTSA API")
        
        response = requests.get(MAKES_URL, timeout=self.timeout)
        response.raise_for_status()
        
        data = response.json()
        return data.get("Results", [])
        
    def fetch_models_for_make(self, make_id: list[int]) -> list[dict]:
        logger.info(f"Fetching models for make_id={make_id}")

        url = MODELS_URL.format(make_id)
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        return data.get("Results", [])