"""
Tâches de parsing pour le format AbsolutePoker / UltimateBet.

Format différent du format HoldemManager :
  - Délimiteur de main : Stage #XXXXXXXX:
  - Préflop marqué par *** POCKET CARDS ***
  - Actions : PLAYER - Raises $X to $Y  (X = incrément, Y = total)
  - Mises non suivies : PLAYER - returned ($X) : not called
  - Pas de résumé par joueur → total_bet calculé à partir des actions
  - Collect inline : PLAYER Collects $X from main pot
"""

import re
import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_float(x):
    """Convertit une valeur en float en ignorant les virgules de milliers."""
    try:
        if x is None:
            return None
        return float(str(x).replace(",", ""))
    except (ValueError, TypeError):
        return None


def parse_single_hand(block):
    """
    Parse une seule main au format AbsolutePoker/UltimateBet.

    Calcul du total_bet par action :
      - Ante, blind         → +amount
      - Ante returned       → -amount  (ante non jouée rendue)
      - Calls $X            → +X
      - Bets $X             → +X
      - Raises $X to $Y     → +X  (X = incrément de relance)
      - returned ($X)       → -X  (mise non suivie rendue)
      - Collects $X         → total_collect

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
        "ante": None,
        "game_type": None,
        "button_seat": None,
        "num_players": 0,
        "board_flop": None,
        "board_turn": None,
        "board_river": None,
        "pot_total": None,
        "rake": None,
        "jp_fee": 0.0,
        "has_showdown": 0,
        "winner_count": 0,
    }

    players = {}
    current_street = "preflop"
    action_order = 0
    actions_rows_local = []
    in_summary = False

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
                "showdown": 0,
                "won_hand": 0,
                "saw_flop": 0,
                "saw_turn": 0,
                "saw_river": 0,
                "vpip": 0,
                "preflop_raise": 0,
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
            "amount": amount,
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

    for line in lines:

        # --- En-tête de main ---
        # Stage #3063936648: Holdem  No Limit $10, $2.50 ante - 2009-07-14 08:40:14 (ET)
        # Stage #3063937412: Holdem (1 on 1)  No Limit $10 - 2009-07-14 08:40:28 (ET)
        m = re.match(
            r"^Stage #(\d+): (.+?) - (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
            line
        )
        if m:
            hand["game_id"] = int(m.group(1))
            hand["start_time"] = m.group(3)
            game_desc = m.group(2)
            blind_m = re.search(r"No Limit \$([\d,]+(?:\.\d+)?)", game_desc)
            if blind_m:
                hand["big_blind"] = safe_float(blind_m.group(1))
                hand["small_blind"] = (hand["big_blind"] or 0) / 2
            ante_m = re.search(r"\$([\d.]+) ante", game_desc)
            if ante_m:
                hand["ante"] = safe_float(ante_m.group(1))
            hand["game_type"] = "NLH"
            continue

        # Table: CONCORD (Real Money) Seat #6 is the dealer
        m = re.match(r"^Table: (.+?) \(Real Money\) Seat #(\d+) is the dealer$", line)
        if m:
            hand["table_name"] = m.group(1).strip()
            hand["button_seat"] = int(m.group(2))
            continue

        # Seat 6 - GUxuIilV1hpyiiD3W74Pvw ($1,034 in chips)
        m = re.match(r"^Seat (\d+) - (.+?) \(\$([\d,]+(?:\.\d+)?) in chips\)$", line)
        if m:
            seat = int(m.group(1))
            player = m.group(2).strip()
            stack = safe_float(m.group(3))
            ensure_player(player)
            players[player]["seat"] = seat
            players[player]["stack_start"] = stack
            continue

        # --- Summary ---
        if line == "*** SUMMARY ***":
            in_summary = True
            continue

        if in_summary:
            # Total Pot($112.50) | Rake ($3)  ou  Total Pot($20)
            m = re.match(
                r"^Total Pot\(\$([\d,]+(?:\.\d+)?)\)(?:\s*\|\s*Rake \(\$([\d.]+)\))?",
                line
            )
            if m:
                hand["pot_total"] = safe_float(m.group(1))
                hand["rake"] = safe_float(m.group(2)) if m.group(2) else 0.0
            continue

        # --- Marqueurs de street ---
        if line == "*** POCKET CARDS ***":
            current_street = "preflop"
            continue

        if line.startswith("*** FLOP ***"):
            current_street = "flop"
            m = re.match(r"^\*\*\* FLOP \*\*\* \[([^\]]+)\]$", line)
            if m:
                hand["board_flop"] = m.group(1)
            continue

        if line.startswith("*** TURN ***"):
            current_street = "turn"
            m = re.match(r"^\*\*\* TURN \*\*\* \[[^\]]+\] \[([^\]]+)\]$", line)
            if m:
                hand["board_turn"] = m.group(1)
            continue

        if line.startswith("*** RIVER ***"):
            current_street = "river"
            m = re.match(r"^\*\*\* RIVER \*\*\* \[[^\]]+\] \[[^\]]+\] \[([^\]]+)\]$", line)
            if m:
                hand["board_river"] = m.group(1)
            continue

        if line == "*** SHOW DOWN ***":
            hand["has_showdown"] = 1
            continue

        # --- Antes ---
        # Player - Ante $2.50
        m = re.match(r"^(.+?) - Ante \$([\d.]+)$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += amount
            add_action(player, "preflop", "ante", amount)
            continue

        # Player - Ante returned $2.50
        m = re.match(r"^(.+?) - Ante returned \$([\d.]+)$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] -= amount
            continue

        # Sitout — ignoré
        if re.match(r"^(.+?) - sitout", line):
            continue

        # --- Blindes ---
        m = re.match(r"^(.+?) - Posts small blind \$([\d,]+(?:\.\d+)?)$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["is_small_blind"] = 1
            players[player]["total_bet"] += amount
            add_action(player, "preflop", "small_blind", amount)
            continue

        m = re.match(r"^(.+?) - Posts big blind \$([\d,]+(?:\.\d+)?)$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["is_big_blind"] = 1
            players[player]["total_bet"] += amount
            add_action(player, "preflop", "big_blind", amount)
            continue

        # --- Actions ---
        m = re.match(r"^(.+?) - Folds$", line)
        if m:
            add_action(m.group(1).strip(), current_street, "fold", 0.0)
            continue

        m = re.match(r"^(.+?) - Checks$", line)
        if m:
            add_action(m.group(1).strip(), current_street, "check", 0.0)
            continue

        # Calls $X
        m = re.match(r"^(.+?) - Calls \$([\d,]+(?:\.\d+)?)$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += amount
            add_action(player, current_street, "call", amount)
            continue

        # Bets $X
        m = re.match(r"^(.+?) - Bets \$([\d,]+(?:\.\d+)?)$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += amount
            add_action(player, current_street, "bet", amount)
            continue

        # Raises $X to $Y  →  on comptabilise X (l'incrément)
        m = re.match(
            r"^(.+?) - Raises \$([\d,]+(?:\.\d+)?) to \$([\d,]+(?:\.\d+)?)$",
            line
        )
        if m:
            player = m.group(1).strip()
            increment = safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] += increment
            add_action(player, current_street, "raise", increment)
            continue

        # returned ($X) : not called  →  mise non suivie rendue
        m = re.match(r"^(.+?) - returned \(\$([\d,]+(?:\.\d+)?)\) : not called$", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_bet"] -= amount
            continue

        # --- Showdown ---
        # Player - Does not show
        m = re.match(r"^(.+?) - Does not show$", line)
        if m:
            player = m.group(1).strip()
            ensure_player(player)
            players[player]["showdown"] = 1
            continue

        # Player - Shows [Ac Kd] (description)
        m = re.match(r"^(.+?) - Shows \[([^\]]+)\]", line)
        if m:
            player = m.group(1).strip()
            cards = m.group(2).split()
            ensure_player(player)
            if len(cards) >= 2:
                players[player]["hole_cards"] = cards[:2]
            players[player]["cards_known"] = 1
            players[player]["showdown"] = 1
            continue

        # Player Collects $X from main pot / side pot
        m = re.match(r"^(.+?) Collects \$([\d,]+(?:\.\d+)?) from", line)
        if m:
            player, amount = m.group(1).strip(), safe_float(m.group(2))
            ensure_player(player)
            players[player]["total_collect"] += amount
            players[player]["won_hand"] = 1
            continue

    # --- Finalisation ---
    hand["num_players"] = len(players)
    hand["winner_count"] = sum(1 for p in players.values() if p["won_hand"] == 1)

    if hand["button_seat"] is not None:
        for p in players.values():
            if p["seat"] == hand["button_seat"]:
                p["is_button"] = 1

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
            "won_hand": pdata["won_hand"],
        })

    return hand, player_rows, actions_rows_local


def parse_poker_txt(file_path):
    """
    Parse un fichier de logs poker au format AbsolutePoker/UltimateBet.

    Args:
        file_path: Chemin vers le fichier .txt

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (hands, player_hands, actions)
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
    except UnicodeDecodeError:
        logger.warning(f"Encodage UTF-8 failed pour {file_path}, essai avec latin-1...")
        with open(file_path, "r", encoding="latin-1") as f:
            text = f.read()

    match = re.search(r"Stage #\d+:", text)
    if not match:
        logger.warning(f"Aucune main trouvée dans {file_path}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    text = text[match.start():]
    blocks = [b.strip() for b in re.split(r"(?=Stage #\d+:)", text) if b.strip()]

    hands_rows, player_hands_rows, actions_rows = [], [], []

    for block_idx, block in enumerate(blocks):
        try:
            hand_row, player_rows, action_rows = parse_single_hand(block)
            if hand_row and hand_row["game_id"] is not None:
                hands_rows.append(hand_row)
                player_hands_rows.extend(player_rows)
                actions_rows.extend(action_rows)
        except Exception as e:
            logger.warning(f"Erreur main {block_idx} dans {file_path}: {e}")
            continue

    hands_df = pd.DataFrame(hands_rows) if hands_rows else pd.DataFrame()
    player_hands_df = pd.DataFrame(player_hands_rows) if player_hands_rows else pd.DataFrame()
    actions_df = pd.DataFrame(actions_rows) if actions_rows else pd.DataFrame()

    logger.info(
        f"{Path(file_path).name}: {len(hands_df)} mains, "
        f"{len(player_hands_df)} joueurs, {len(actions_df)} actions"
    )

    return hands_df, player_hands_df, actions_df
