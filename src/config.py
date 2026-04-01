from pathlib import Path

DATA_RAW = Path("data/raw/data.csv")
DATA_PROCESSED = Path("data/processed/data.parquet")
MODEL_PATH = Path("models/model.pkl")
RANDOM_STATE = 42

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
POKER_DATA_DIR = RAW_DATA_DIR / "poker_holdem_games"

KAGGLE_DATASET_HANDLE = "smeilz/poker-holdem-games"