from src.dataset import download_poker_dataset
from src.preprocessing.process import run_preprocessing

def main() -> None:
    # Télécharger les données
    dataset_path = download_poker_dataset(force_download=False)
    print(f"Dataset prêt dans : {dataset_path}")

    # Parser et convertir les fichiers .txt en .parquet structurés
    print("Parsing des fichiers poker en datasets...")
    run_preprocessing()
    print("Traitement terminé!")


if __name__ == "__main__":
    main()