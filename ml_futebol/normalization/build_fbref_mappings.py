from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher

import pandas as pd
from psycopg.rows import dict_row

from database.db import get_db_pool


PLAYER_AUTO_THRESHOLD = 0.90
PLAYER_MANUAL_THRESHOLD = 0.75

TEAM_AUTO_THRESHOLD = 0.90
TEAM_MANUAL_THRESHOLD = 0.75


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None

    value = str(value).strip().lower()
    if not value:
        return None

    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))

    tokens = re.findall(r"[a-z0-9]+", value)

    stopwords = {
        "da", "de", "do", "dos", "das",
        "di", "del", "della", "van", "von",
        "jr", "junior", "filho", "neto"
    }

    tokens = [token for token in tokens if token not in stopwords]

    return " ".join(tokens) if tokens else None


def token_similarity(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.0

    set_a = set(a.split())
    set_b = set(b.split())

    if not set_a or not set_b:
        return 0.0

    intersection = len(set_a & set_b)
    union = len(set_a | set_b)

    if union == 0:
        return 0.0

    return intersection / union


def sequence_similarity(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def similarity(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.0

    a_norm = normalize_text(a)
    b_norm = normalize_text(b)

    if not a_norm or not b_norm:
        return 0.0

    seq_score = sequence_similarity(a_norm, b_norm)
    token_score = token_similarity(a_norm, b_norm)

    # bônus se um nome estiver contido no outro
    containment_bonus = 0.0
    if a_norm in b_norm or b_norm in a_norm:
        containment_bonus = 0.08

    return min(1.0, max(seq_score, token_score) + containment_bonus)


def load_distinct_values(query: str) -> pd.DataFrame:
    db_pool = get_db_pool()
    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return pd.DataFrame(rows)


def best_match(source_name: str, candidates: list[str]) -> tuple[str | None, float]:
    best_candidate = None
    best_score = 0.0

    for candidate in candidates:
        score = similarity(source_name, candidate)
        if score > best_score:
            best_score = score
            best_candidate = candidate

    return best_candidate, best_score


def classify_match(score: float, auto_threshold: float, manual_threshold: float) -> tuple[str, bool]:
    if score >= auto_threshold:
        return "auto_high_confidence", True
    if score >= manual_threshold:
        return "manual_review", True
    return "unmatched", False


def build_player_mappings() -> pd.DataFrame:
    statsbomb_players = load_distinct_values("""
        SELECT DISTINCT player_name
        FROM silver.lineup_players
        WHERE player_name IS NOT NULL
        ORDER BY player_name
    """)

    fbref_players = load_distinct_values("""
        SELECT DISTINCT player_name
        FROM silver.fbref_player_stats
        WHERE player_name IS NOT NULL
        ORDER BY player_name
    """)

    if statsbomb_players.empty:
        raise ValueError("Nenhum player_name encontrado em silver.lineup_players.")

    if fbref_players.empty:
        raise ValueError("Nenhum player_name encontrado em silver.fbref_player_stats.")

    fbref_player_list = fbref_players["player_name"].tolist()
    rows = []

    for player_name in statsbomb_players["player_name"]:
        matched_name, score = best_match(player_name, fbref_player_list)
        match_method, keep_match = classify_match(
            score=score,
            auto_threshold=PLAYER_AUTO_THRESHOLD,
            manual_threshold=PLAYER_MANUAL_THRESHOLD,
        )

        rows.append({
            "statsbomb_player_name": player_name,
            "normalized_statsbomb_player_name": normalize_text(player_name),
            "fbref_player_name": matched_name if keep_match else None,
            "normalized_fbref_player_name": normalize_text(matched_name) if keep_match else None,
            "match_method": match_method,
            "confidence_score": round(score, 4),
        })

    return pd.DataFrame(rows)


def build_team_mappings() -> pd.DataFrame:
    statsbomb_teams = load_distinct_values("""
        SELECT DISTINCT team_name
        FROM silver.lineup_players
        WHERE team_name IS NOT NULL
        ORDER BY team_name
    """)

    fbref_teams = load_distinct_values("""
        SELECT DISTINCT squad_name
        FROM silver.fbref_player_stats
        WHERE squad_name IS NOT NULL
        ORDER BY squad_name
    """)

    if statsbomb_teams.empty:
        raise ValueError("Nenhum team_name encontrado em silver.lineup_players.")

    if fbref_teams.empty:
        raise ValueError("Nenhum squad_name encontrado em silver.fbref_player_stats.")

    fbref_team_list = fbref_teams["squad_name"].tolist()
    rows = []

    for team_name in statsbomb_teams["team_name"]:
        matched_name, score = best_match(team_name, fbref_team_list)
        match_method, keep_match = classify_match(
            score=score,
            auto_threshold=TEAM_AUTO_THRESHOLD,
            manual_threshold=TEAM_MANUAL_THRESHOLD,
        )

        rows.append({
            "statsbomb_team_name": team_name,
            "normalized_statsbomb_team_name": normalize_text(team_name),
            "fbref_team_name": matched_name if keep_match else None,
            "normalized_fbref_team_name": normalize_text(matched_name) if keep_match else None,
            "match_method": match_method,
            "confidence_score": round(score, 4),
        })

    return pd.DataFrame(rows)


def save_player_mappings(df: pd.DataFrame) -> None:
    db_pool = get_db_pool()

    with db_pool.get_cursor() as (conn, cur):
        cur.execute("TRUNCATE TABLE curated.player_name_mapping;")

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO curated.player_name_mapping (
                    statsbomb_player_name,
                    normalized_statsbomb_player_name,
                    fbref_player_name,
                    normalized_fbref_player_name,
                    match_method,
                    confidence_score
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row["statsbomb_player_name"],
                row["normalized_statsbomb_player_name"],
                row["fbref_player_name"],
                row["normalized_fbref_player_name"],
                row["match_method"],
                row["confidence_score"],
            ))

        conn.commit()


def save_team_mappings(df: pd.DataFrame) -> None:
    db_pool = get_db_pool()

    with db_pool.get_cursor() as (conn, cur):
        cur.execute("TRUNCATE TABLE curated.team_name_mapping;")

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO curated.team_name_mapping (
                    statsbomb_team_name,
                    normalized_statsbomb_team_name,
                    fbref_team_name,
                    normalized_fbref_team_name,
                    match_method,
                    confidence_score
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row["statsbomb_team_name"],
                row["normalized_statsbomb_team_name"],
                row["fbref_team_name"],
                row["normalized_fbref_team_name"],
                row["match_method"],
                row["confidence_score"],
            ))

        conn.commit()


def print_summary(title: str, df: pd.DataFrame, name_col: str) -> None:
    print(f"\n=== {title} summary ===")
    print(df["match_method"].value_counts(dropna=False).to_string())

    print(f"\n=== {title} manual review ===")
    manual_df = df[df["match_method"] == "manual_review"].copy()
    if manual_df.empty:
        print("Nenhum caso para revisão manual.")
    else:
        print(
            manual_df
            .sort_values(["confidence_score", name_col], ascending=[False, True])
            .head(50)
            .to_string(index=False)
        )

    print(f"\n=== {title} unmatched ===")
    unmatched_df = df[df["match_method"] == "unmatched"].copy()
    if unmatched_df.empty:
        print("Nenhum caso sem match.")
    else:
        print(
            unmatched_df
            .sort_values(name_col)
            .head(50)
            .to_string(index=False)
        )


def main() -> None:
    player_df = build_player_mappings()
    team_df = build_team_mappings()

    save_player_mappings(player_df)
    save_team_mappings(team_df)

    print_summary("Player mapping", player_df, "statsbomb_player_name")
    print_summary("Team mapping", team_df, "statsbomb_team_name")


if __name__ == "__main__":
    main()