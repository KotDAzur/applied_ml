"""Prédictions simples."""

from pathlib import Path
import pickle
import numpy as np
import pandas as pd
from src.config import MODEL_PATH


def load_model(path=None):
    """Charge le modèle."""
    if path is None:
        path = MODEL_PATH
    with open(path, "rb") as f:
        return pickle.load(f)


def predict(df: pd.DataFrame, model_path=None) -> pd.DataFrame:
    """Prédit sur un DataFrame."""
    artifacts = load_model(model_path)
    model, scaler, feature_names = artifacts["model"], artifacts["scaler"], artifacts["feature_names"]
    
    X = df[feature_names].values
    X_scaled = scaler.transform(X)
    predictions = model.predict(X_scaled)
    
    return df.copy().assign(predicted_net_result=predictions)


if __name__ == "__main__":
    from src.config import RAW_DATA_DIR
    df = pd.read_parquet(RAW_DATA_DIR.parent / "processed" / "player_hands.parquet")
    result = predict(df)
    print(result[["player", "net_result", "predicted_net_result"]].head())

