"""
Orchestration du traitement des fichiers poker au format AbsolutePoker/UltimateBet.
"""

from pathlib import Path
import logging
import pandas as pd

from src.config import POKER_DATA_DIR, RAW_DATA_DIR
from src.preprocessing.tasks import parse_poker_txt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_poker_files(input_dir: Path = None, output_dir: Path = None) -> None:
    """
    Parse tous les fichiers .txt du format AbsolutePoker/UltimateBet
    et génère 3 fichiers .parquet dans output_dir.

    Args:
        input_dir:  Répertoire source (défaut: data/raw/Other DATA)
        output_dir: Répertoire de destination (défaut: data/processed_v2)
    """
    if input_dir is None:
        input_dir = POKER_DATA_DIR

    if output_dir is None:
        output_dir = RAW_DATA_DIR.parent / "processed"

    output_dir.mkdir(parents=True, exist_ok=True)

    txt_files = sorted(input_dir.glob("*.txt"))

    if not txt_files:
        logger.warning(f"Aucun fichier .txt trouvé dans {input_dir}")
        return

    logger.info(f"Traitement de {len(txt_files)} fichier(s) .txt...")

    all_hands, all_players, all_actions = [], [], []

    for txt_file in txt_files:
        try:
            hands_df, players_df, actions_df = parse_poker_txt(txt_file)
            if not hands_df.empty:
                all_hands.append(hands_df)
                all_players.append(players_df)
                all_actions.append(actions_df)
        except Exception as e:
            logger.error(f"Erreur lors du parsing de {txt_file.name}: {e}")
            continue

    if not all_hands:
        logger.error("Aucune donnée n'a pu être parsée")
        return

    hands_combined   = pd.concat(all_hands,   ignore_index=True)
    players_combined = pd.concat(all_players, ignore_index=True)
    actions_combined = pd.concat(all_actions, ignore_index=True)

    hands_combined.to_parquet(output_dir / "hands.parquet")
    players_combined.to_parquet(output_dir / "player_hands.parquet")
    actions_combined.to_parquet(output_dir / "actions.parquet")

    logger.info(f"Résultats sauvegardés dans {output_dir.name}/")
    logger.info(f"   - hands.parquet        : {len(hands_combined)} mains")
    logger.info(f"   - player_hands.parquet : {len(players_combined)} entrées joueur")
    logger.info(f"   - actions.parquet      : {len(actions_combined)} actions")


def run_preprocessing() -> None:
    """Lance le pipeline de preprocessing v2."""
    process_poker_files()


if __name__ == "__main__":
    run_preprocessing()
