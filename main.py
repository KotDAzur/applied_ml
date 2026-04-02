import logging
from src.dataset import ensure_raw_data
from src.preprocessing.process import run_preprocessing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_data() -> None:
    """Télécharge les données depuis HuggingFace si absent."""
    logger.info("Téléchargement des données")
    input_dir = ensure_raw_data()
    logger.info(f"Données prêtes dans : {input_dir}")


def run_pipeline() -> None:
    """Parse les fichiers et génère les datasets."""
    logger.info("Pipeline de preprocessing")
    run_preprocessing()
    logger.info("Datasets générés dans data/processed/")


def main() -> None:
    """Pipeline complet : télécharge puis traite les données."""
    logger.info("Début du pipeline complet")
    
    download_data()
    run_pipeline()
    
    logger.info("Pipeline terminé")


if __name__ == "__main__":
    main()