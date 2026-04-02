"""Tâches bas niveau pour la gestion des données."""

from pathlib import Path
import logging
from huggingface_hub import snapshot_download

from src.config import POKER_DATA_DIR, HF_DATASET_REPO

logger = logging.getLogger(__name__)


def check_local_data() -> bool:
    """Vérifie si les fichiers .txt sont présents localement."""
    return POKER_DATA_DIR.exists() and any(POKER_DATA_DIR.glob("*.txt"))


def download_from_hf() -> Path:
    """Télécharge les données depuis Hugging Face Hub."""
    logger.info(f"Téléchargement depuis Hugging Face ({HF_DATASET_REPO})...")
    POKER_DATA_DIR.mkdir(parents=True, exist_ok=True)

    snapshot_download(
        repo_id=HF_DATASET_REPO,
        repo_type="dataset",
        local_dir=POKER_DATA_DIR,
    )

    return POKER_DATA_DIR


def count_files() -> int:
    """Compte le nombre de fichiers .txt."""
    return len(list(POKER_DATA_DIR.glob("*.txt")))

