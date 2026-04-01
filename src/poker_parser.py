"""
Parser pour les fichiers de logs de poker Hold'em.
Transforme les fichiers .txt en 3 dataframes structurées (Polars).
"""

import re
import logging
from pathlib import Path
import polars as pl

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_float(x):
    """Convertit une valeur en float, retourne None si impossible."""
    try:
        return float(x)
    except (ValueError, TypeError):
     return None

def parse_single_hand(block):
    """
    Parse une seule main de poker.

    Args:
        block: Bloc de texte représentant une main

    Returns:
        Tuple: (hand_dict, player_rows_list, actions_rows_list)
    """
    lines = [line.strip() for line in block.splitlines() if line.strip()]
    if not lines:
        return None, [], []

    hand = {
    "game_id": None,
    "start_time": None,
    "end_time": None,
    "table_name": None,
    "small_blind": None,
    "big_blind": None,
    "game_type": None,
    "button_seat": None,
    "num_players": 0,
    "board_flop": None,
    "board_turn": None,
    "board_river": None,
    "pot_total": None,
    "rake": None,
    "jp_fee": None,
    "has_showdown": 0,
    "winner_count": 0
}

    players = {}
    current_street = "preflop"
    action_order = 0
    actions_rows_local = []

    def ensure_player(name):
        if name not in players:
            players[name] = {
                "player": name,
                "seat": None,
                "stack_start": None,
                "is_button": 0,
                "is_small_blind": 0,
                "is_big_blind": 0,
                "hole_cards": [],
                "cards_known": 0,
                "preflop_actions": [],
                "flop_actions": [],
                "turn_actions": [],
                "river_actions": [],
                "total_bet": 0.0,
                "total_collect": 0.0,
                "summary_result": 0.0,
                "showdown": 0,
                "won_hand": 0,
                "saw_flop": 0,
                "saw_turn": 0,
                "saw_river": 0,
                "vpip": 0,
                "preflop_raise": 0
            }

    def add_action(player, street, action_type, amount=0.0):
        nonlocal action_order
        ensure_player(player)
        action_order += 1

        actions_rows_local.append({
            "game_id": hand["game_id"],
            "player": player,
            "street": street,
            "action_order": action_order,
            "action_type": action_type,
            "amount": amount
        })

        if street == "preflop":
            players[player]["preflop_actions"].append(action_type)
            if action_type in ["call", "raise", "bet"]:
                players[player]["vpip"] = 1
            if action_type == "raise":
                players[player]["preflop_raise"] = 1

        elif street == "flop":
            players[player]["flop_actions"].append(action_type)
            players[player]["saw_flop"] = 1

        elif street == "turn":
            players[player]["turn_actions"].append(action_type)
            players[player]["saw_turn"] = 1

        elif street == "river":
            players[player]["river_actions"].append(action_type)
            players[player]["saw_river"] = 1

    # Parsing ligne par ligne
    for line in lines:
        # Début
        m = re.match(r"^Game started at: (.+)$", line)
        if m:
            hand["start_time"] = m.group(1)
            continue

        # Game ID / blindes / table
        m = re.match(r"^Game ID: (\d+) ([\d.]+)/([\d.]+) \(([^)]+)\) (.+) \(Hold'em\)$", line)
        if m:
            hand["game_id"] = int(m.group(1))
            hand["small_blind"] = safe_float(m.group(2))
            hand["big_blind"] = safe_float(m.group(3))
            hand["game_type"] = m.group(4)
            hand["table_name"] = m.group(5)
            continue

        # Bouton
        m = re.match(r"^Seat (\d+) is the button$", line)
        if m:
            hand["button_seat"] = int(m.group(1))
            continue

        # Joueurs / sièges / stacks
        m = re.match(r"^Seat (\d+): ([^(]+) \(([\d.]+)\)\.$", line)
        if m:
            seat = int(m.group(1))
            player = m.group(2).strip()
            stack = safe_float(m.group(3))
            ensure_player(player)
            players[player]["seat"] = seat
            players[player]["stack_start"] = stack
            continue

        # Changement de street
        if line.startswith("*** FLOP ***"):
            current_street = "flop"
            m = re.match(r"^\*\*\* FLOP \*\*\*: \[([^\]]+)\]$", line)
            if m:
                hand["board_flop"] = m.group(1)
            continue

        if line.startswith("*** TURN ***"):
            current_street = "turn"
            m = re.match(r"^\*\*\* TURN \*\*\*: \[[^\]]+\] \[([^\]]+)\]$", line)
            if m:
                hand["board_turn"] = m.group(1)
            continue

        if line.startswith("*** RIVER ***"):
            current_street = "river"
            m = re.match(r"^\*\*\* RIVER \*\*\*: \[[^\]]+\] \[([^\]]+)\]$", line)
            if m:
                hand["board_river"] = m.group(1)
            continue

        # Cartes montrées au joueur
        m = re.match(r"^Player ([^\s]+) received card: \[([^\]]+)\]$", line)
        if m:
            player, card = m.group(1), m.group(2)
            ensure_player(player)
            players[player]["hole_cards"].append(card)
            players[player]["cards_known"] = 1
            continue

        # Blinds
        m = re.match(r"^Player ([^\s]+) has small blind \(([\d.]+)\)$", line)
        if m:
            player, amount = m.group(1), safe_float(m.group(2))
            ensure_player(player)
            players[player]["is_small_blind"] = 1
            players[player]["total_bet"] += amount
            add_action(player, "preflop", "small_blind", amount)
            continue

        m = re.match(r"^Player ([^\s]+) has big blind \(([\d.]+)\)$", line)
        if m:
            player, amount = m.group(1), safe_float(m.group(2))
            ensure_player(player)
            players[player]["is_big_blind"] = 1
            players[player]["total_bet"] += amount
            add_action(player, "preflop", "big_blind", amount)
            continue

        # Actions sans montant
        m = re.match(r"^Player ([^\s]+) folds$", line)
        if m:
            add_action(m.group(1), current_street, "fold", 0.0)
            continue

        m = re.match(r"^Player ([^\s]+) checks$", line)
        if m:
            add_action(m.group(1), current_street, "check", 0.0)
            continue

        # Actions avec montant
        m = re.match(r"^Player ([^\s]+) calls \(([\d.]+)\)$", line)
        if m:
            player, amount = m.group(1), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += amount
            add_action(player, current_street, "call", amount)
            continue

        m = re.match(r"^Player ([^\s]+) bets \(([\d.]+)\)$", line)
        if m:
            player, amount = m.group(1), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += amount
            add_action(player, current_street, "bet", amount)
            continue

        m = re.match(r"^Player ([^\s]+) raises \(([\d.]+)\)$", line)
        if m:
            player, amount = m.group(1), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += amount
            add_action(player, current_street, "raise", amount)
            continue

        # Pot / rake / jackpot fee
        m = re.match(r"^Pot: ([\d.]+)\. Rake ([\d.]+)(?:\. JP fee ([\d.]+))?$", line)
        if m:
            hand["pot_total"] = safe_float(m.group(1))
            hand["rake"] = safe_float(m.group(2))
            hand["jp_fee"] = safe_float(m.group(3)) if m.group(3) else 0.0
            continue

        # Summary : joueur avec showdown
        m = re.match(
            r"^\*?Player ([^\s]+) shows: .* \[([^\]]+)\]\. Bets: ([\d.]+)\. Collects: ([\d.]+)\. (Wins|Loses): ([\d.]+)\.$",
            line
        )
        if m:
            player = m.group(1)
            cards = m.group(2).split()
            bets = safe_float(m.group(3))
            collects = safe_float(m.group(4))

            ensure_player(player)
            players[player]["hole_cards"] = cards
            players[player]["cards_known"] = 1
            players[player]["showdown"] = 1
            players[player]["total_bet"] = bets
            players[player]["total_collect"] = collects
            players[player]["summary_result"] = collects - bets
            players[player]["won_hand"] = 1 if collects > bets else 0
            hand["has_showdown"] = 1
            continue

        # Summary : joueur sans showdown
        m = re.match(
            r"^\*?Player ([^\s]+) does not show cards\. Bets: ([\d.]+)\. Collects: ([\d.]+)\. (Wins|Loses): ([\d.]+)\.$",
            line
        )
        if m:
            player = m.group(1)
            bets = safe_float(m.group(2))
            collects = safe_float(m.group(3))

            ensure_player(player)
            players[player]["total_bet"] = bets
            players[player]["total_collect"] = collects
            players[player]["summary_result"] = collects - bets
            players[player]["won_hand"] = 1 if collects > bets else 0
            continue

        # Main terminée
        m = re.match(r"^Game ended at: (.+)$", line)
        if m:
            hand["end_time"] = m.group(1)
            continue

    # Finalisation joueurs
    hand["num_players"] = len(players)

    if hand["button_seat"] is not None:
        for p in players.values():
            if p["seat"] == hand["button_seat"]:
                p["is_button"] = 1

    # Nombre de gagnants
    hand["winner_count"] = sum(1 for p in players.values() if p["won_hand"] == 1)

    player_rows = []
    for player, pdata in players.items():
        player_rows.append({
            "game_id": hand["game_id"],
            "player": player,
            "seat": pdata["seat"],
            "stack_start": pdata["stack_start"],
            "is_button": pdata["is_button"],
            "is_small_blind": pdata["is_small_blind"],
            "is_big_blind": pdata["is_big_blind"],
            "hole_card_1": pdata["hole_cards"][0] if len(pdata["hole_cards"]) > 0 else None,
            "hole_card_2": pdata["hole_cards"][1] if len(pdata["hole_cards"]) > 1 else None,
            "cards_known": pdata["cards_known"],
            "preflop_actions": "|".join(pdata["preflop_actions"]) if pdata["preflop_actions"] else None,
            "flop_actions": "|".join(pdata["flop_actions"]) if pdata["flop_actions"] else None,
            "turn_actions": "|".join(pdata["turn_actions"]) if pdata["turn_actions"] else None,
            "river_actions": "|".join(pdata["river_actions"]) if pdata["river_actions"] else None,
            "vpip": pdata["vpip"],
            "preflop_raise": pdata["preflop_raise"],
            "saw_flop": pdata["saw_flop"],
            "saw_turn": pdata["saw_turn"],
            "saw_river": pdata["saw_river"],
            "showdown": pdata["showdown"],
            "total_bet": pdata["total_bet"],
            "total_collect": pdata["total_collect"],
            "net_result": pdata["total_collect"] - pdata["total_bet"],
            "won_hand": pdata["won_hand"]
        })

    return hand, player_rows, actions_rows_local

def parse_poker_txt(file_path):
    """
    Parse un fichier de logs poker et retourne 3 dataframes Polars.

    Args:
        file_path: Chemin vers le fichier .txt

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: (hands, player_hands, actions)
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
    except UnicodeDecodeError:
        logger.warning(f"Encodage UTF-8 failed pour {file_path}, essai avec latin-1...")
        with open(file_path, "r", encoding="latin-1") as f:
            text = f.read()

    # Découpage en mains
    blocks = [b.strip() for b in re.split(r'(?=Game started at:)', text) if b.strip()]

    if not blocks:
        logger.warning(f"Aucune main trouvée dans {file_path}")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    hands_rows = []
    player_hands_rows = []
    actions_rows = []

    for block_idx, block in enumerate(blocks):
        try:
            hand_row, player_rows, action_rows = parse_single_hand(block)
            if hand_row:
                hands_rows.append(hand_row)
                player_hands_rows.extend(player_rows)
                actions_rows.extend(action_rows)
        except Exception as e:
            logger.warning(f"Erreur lors du parsing de la main {block_idx} dans {file_path}: {e}")
            continue

    hands_df = pl.DataFrame(hands_rows) if hands_rows else pl.DataFrame()
    player_hands_df = pl.DataFrame(player_hands_rows) if player_hands_rows else pl.DataFrame()
    actions_df = pl.DataFrame(actions_rows) if actions_rows else pl.DataFrame()

    logger.info(f"{Path(file_path).name}: {len(hands_df)} mains, {len(player_hands_df)} joueurs, {len(actions_df)} actions")

    return hands_df, player_hands_df, actions_df

