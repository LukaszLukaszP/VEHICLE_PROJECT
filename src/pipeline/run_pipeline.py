from src.extract.nhtsa_client import NHTSAClient
from src.transform.normalize import normalize_makes, normalize_models
from src.load.file_loader import save_to_csv
from src.config.settings import MAKES_FILE, MODELS_FILE
from src.utils.logger import get_logger

logger = get_logger(__name__)

def run():
    logger.info("Pipeline started")

    client = NHTSAClient()

    makes_raw = client.fetch_makes()
    makes_df = normalize_makes(makes_raw)

    save_to_csv(makes_df, MAKES_FILE)

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()