from src.extract.nhtsa_client import NHTSAClient
from src.transform.normalize import normalize_makes, normalize_models
from src.load.file_loader import save_to_csv
from src.config.settings import MAKES_FILE, MODELS_FILE, MODELS_LIMIT
from src.utils.logger import get_logger

logger = get_logger(__name__)

def run() -> None:
    logger.info("Pipeline started")

    try:
        client = NHTSAClient()

        #Makes
        logger.info("Fetching makes")
        makes_raw = client.fetch_makes()
        df_makes = normalize_makes(makes_raw)
        logger.info(f"Number of makes fetched: {df_makes.shape[0]}")

        logger.info("Saving makes")
        save_to_csv(df_makes, MAKES_FILE)

        #Models
        logger.info("Preparing make_ids for models")
        if MODELS_LIMIT:
            make_ids = df_makes["make_id"].head(MODELS_LIMIT).tolist()
        else:
            make_ids = df_makes["make_id"].tolist()
        logger.info(f"Number of make_ids selected: {len(make_ids)}")

        logger.info("Fetching models")
        all_models = []
        for make_id in make_ids:        
            models = client.fetch_models_for_make(make_id)
            all_models.extend(models)
        logger.info(f"Total raw models fetched: {len(all_models)}")

        logger.info("Normalizing models")    
        df_models = normalize_models(all_models)

        logger.info("Saving models")
        save_to_csv(df_models, MODELS_FILE)

        logger.info("Pipeline finished successfully")

    except Exception:
        logger.exception("Pipeline failed")
        raise

if __name__ == "__main__":
    run()