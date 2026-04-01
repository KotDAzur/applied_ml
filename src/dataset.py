from pathlib import Path
import kagglehub

from src.config import KAGGLE_DATASET_HANDLE, POKER_DATA_DIR


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def download_poker_dataset(force_download: bool = False) -> Path:
    """
    Télécharge le dataset Kaggle dans data/raw/poker_holdem_games
    et retourne le chemin local.
    """
    ensure_dir(POKER_DATA_DIR)

    dataset_path = kagglehub.dataset_download(
        KAGGLE_DATASET_HANDLE,
        output_dir=str(POKER_DATA_DIR),
        force_download=force_download,
    )

    return Path(dataset_path)


if __name__ == "__main__":
    path = download_poker_dataset()
    print(f"Dataset téléchargé dans : {path}")