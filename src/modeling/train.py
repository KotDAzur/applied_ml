"""Entraînement simple avec régression linéaire."""

from pathlib import Path
import pickle
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from src.config import MODEL_PATH, RANDOM_STATE


def load_data(data_dir: Path = None) -> pd.DataFrame:
    """Charge player_hands.parquet."""
    if data_dir is None:
        from src.config import RAW_DATA_DIR
        data_dir = RAW_DATA_DIR.parent / "processed"
    
    return pd.read_parquet(data_dir / "player_hands.parquet")


def prepare_features(df: pd.DataFrame) -> tuple:
    """Prépare X et y."""
    features = [
        "vpip", "preflop_raise", "saw_flop", "saw_turn", "saw_river",
        "showdown", "total_bet", "total_collect", "is_button",
        "is_small_blind", "is_big_blind", "cards_known"
    ]
    
    X = df[[f for f in features if f in df.columns]].values
    y = df["net_result"].values
    
    # Supprimer NaN
    mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
    return X[mask], y[mask], [f for f in features if f in df.columns]


def train_model(X, y):
    """Entraîne et retourne model, scaler."""
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    
    return model, scaler, X_test_scaled, y_test


def save_model(model, scaler, feature_names, path=None):
    """Sauvegarde le modèle."""
    if path is None:
        path = MODEL_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, "wb") as f:
        pickle.dump({"model": model, "scaler": scaler, "feature_names": feature_names}, f)


if __name__ == "__main__":
    df = load_data()
    X, y, feature_names = prepare_features(df)
    model, scaler, X_test, y_test = train_model(X, y)
    save_model(model, scaler, feature_names)
    print(f"✅ Modèle entraîné et sauvegardé!")

