from src.dataset import download_poker_dataset
from src.preprocessing import process_poker_files


def main() -> None:
    # Télécharger les données
    dataset_path = download_poker_dataset(force_download=False)
    print(f"Dataset prêt dans : {dataset_path}")

    # Parser et convertir les fichiers .txt en .csv structurés
    print("Parsing des fichiers poker en datasets...")
    process_poker_files()
    print("Traitement terminé!")


if __name__ == "__main__":
    main()