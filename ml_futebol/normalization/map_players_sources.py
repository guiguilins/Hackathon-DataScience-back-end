from __future__ import annotations

from rapidfuzz import fuzz
from psycopg.rows import dict_row

from database.db import get_db_pool


SEASON_NAME = "2025/2026"
AUTO_APPROVE_SCORE = 95.0
REVIEW_SCORE = 85.0


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).strip().lower().split())


def fetch_statsbomb_players() -> list[dict]:
    db_pool = get_db_pool()

    query = """
        SELECT DISTINCT
            p.player_id,
            p.player_name,
            p.normalized_player_name,
            t.team_name,
            t.normalized_team_name
        FROM core.players p
        JOIN core.lineups l
          ON l.player_id = p.player_id
        JOIN core.teams t
          ON t.team_id = l.team_id
        ORDER BY p.player_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchall()


def fetch_fbref_players() -> list[dict]:
    db_pool = get_db_pool()

    query = """
        SELECT DISTINCT
            p.player_id,
            p.player_name,
            p.normalized_player_name,
            t.team_name,
            t.normalized_team_name
        FROM core.player_season_stats pss
        JOIN core.players p
          ON p.player_id = pss.player_id
        JOIN core.teams t
          ON t.team_id = pss.team_id
        JOIN core.seasons s
          ON s.season_id = pss.season_id
        WHERE s.season_name = %s
        ORDER BY p.player_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (SEASON_NAME,))
            return cur.fetchall()


def calculate_match_score(statsbomb_player: dict, fbref_player: dict) -> float:
    name_score = fuzz.token_sort_ratio(
        normalize_text(statsbomb_player["player_name"]),
        normalize_text(fbref_player["player_name"]),
    )

    team_score = fuzz.token_sort_ratio(
        normalize_text(statsbomb_player["team_name"]),
        normalize_text(fbref_player["team_name"]),
    )

    final_score = (0.8 * name_score) + (0.2 * team_score)
    return round(final_score, 2)


def find_best_match(statsbomb_player: dict, fbref_players: list[dict]) -> dict | None:
    best_candidate = None
    best_score = 0.0

    for fbref_player in fbref_players:
        score = calculate_match_score(statsbomb_player, fbref_player)

        if score > best_score:
            best_score = score
            best_candidate = fbref_player | {"score": score}

    return best_candidate


def upsert_mapping(statsbomb_player: dict, best_match: dict | None) -> None:
    db_pool = get_db_pool()

    if not best_match:
        return

    score = best_match["score"]

    if score >= AUTO_APPROVE_SCORE:
        mapping_status = "auto_approved"
    elif score >= REVIEW_SCORE:
        mapping_status = "needs_review"
    else:
        mapping_status = "low_confidence"

    query = """
        INSERT INTO map.player_source_mapping (
            player_id,
            statsbomb_player_id,
            statsbomb_player_name,
            fbref_player_id,
            fbref_player_name,
            statsbomb_team_name,
            fbref_team_name,
            season_name,
            confidence_score,
            mapping_status,
            notes
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
    """

    notes = f"name/team fuzzy match - score={score}"

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    statsbomb_player["player_id"],
                    str(statsbomb_player["player_id"]),
                    statsbomb_player["player_name"],
                    str(best_match["player_id"]),
                    best_match["player_name"],
                    statsbomb_player["team_name"],
                    best_match["team_name"],
                    SEASON_NAME,
                    score,
                    mapping_status,
                    notes,
                ),
            )
        conn.commit()


def clear_previous_mappings() -> None:
    db_pool = get_db_pool()

    query = """
        DELETE FROM map.player_source_mapping
        WHERE season_name = %s
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (SEASON_NAME,))
        conn.commit()


def main():
    statsbomb_players = fetch_statsbomb_players()
    fbref_players = fetch_fbref_players()

    if not statsbomb_players:
        raise ValueError("Nenhum player StatsBomb encontrado a partir de core.lineups.")

    if not fbref_players:
        raise ValueError("Nenhum player FBref encontrado a partir de core.player_season_stats.")

    print(f"StatsBomb players: {len(statsbomb_players)}")
    print(f"FBref players: {len(fbref_players)}")

    clear_previous_mappings()

    processed = 0
    auto_approved = 0
    needs_review = 0
    low_confidence = 0

    for idx, statsbomb_player in enumerate(statsbomb_players, start=1):
        best_match = find_best_match(statsbomb_player, fbref_players)

        if best_match:
            upsert_mapping(statsbomb_player, best_match)
            processed += 1

            score = best_match["score"]
            if score >= AUTO_APPROVE_SCORE:
                auto_approved += 1
            elif score >= REVIEW_SCORE:
                needs_review += 1
            else:
                low_confidence += 1

        if idx % 500 == 0:
            print(f"[{idx}/{len(statsbomb_players)}] processados")

    print("\nMapeamento de players finalizado.")
    print(f"Processados: {processed}")
    print(f"Auto aprovados: {auto_approved}")
    print(f"Needs review: {needs_review}")
    print(f"Low confidence: {low_confidence}")


if __name__ == "__main__":
    main()