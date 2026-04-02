"""Orchestration du téléchargement des données."""

from pathlib import Path
import logging

from src.config import POKER_DATA_DIR
from src.dataset.tasks import check_local_data, download_from_hf, count_files

logger = logging.getLogger(__name__)


def ensure_raw_data() -> Path:
    """
    Vérifie que les fichiers .txt sont présents localement.
    Si non, les télécharge depuis Hugging Face Hub.

    Returns:
        Path vers le répertoire contenant les fichiers .txt.
    """
    if check_local_data():
        txt_count = count_files()
        logger.info(f"Données locales trouvées : {txt_count} fichier(s) dans {POKER_DATA_DIR}")
        return POKER_DATA_DIR

    logger.info("Données absentes, téléchargement...")
    data_dir = download_from_hf()
    
    txt_count = count_files()
    logger.info(f"Téléchargement terminé : {txt_count} fichier(s) dans {POKER_DATA_DIR}")
    
    return data_dir

