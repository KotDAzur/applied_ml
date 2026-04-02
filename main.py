from src.preprocessing.process import run_preprocessing

def main() -> None:

    # Parser les fichiers HoldemManager (.txt) → data/processed/
    print("Parsing des fichiers...")
    run_preprocessing()
    print("Traitement terminé!")


if __name__ == "__main__":
    main()