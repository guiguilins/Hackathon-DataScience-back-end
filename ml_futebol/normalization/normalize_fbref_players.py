from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from database.db import get_db_pool


DEFAULT_COMPETITION_NAME = None
DEFAULT_SEASON_NAME = "2025/2026"


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).strip().lower().split())


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def first_not_null(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, "", "nan", "NaN"):
            return payload[key]
    return None


def fetch_raw_fbref_players() -> list[dict[str, Any]]:
    db_pool = get_db_pool()

    query = """
        SELECT payload, source_file
        FROM raw.fbref_players
        ORDER BY raw_fbref_player_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return rows


def get_or_create_team(team_name: str | None) -> int | None:
    if not team_name:
        return None

    db_pool = get_db_pool()
    normalized_team_name = normalize_text(team_name)

    select_query = """
        SELECT team_id
        FROM core.teams
        WHERE normalized_team_name = %s
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(select_query, (normalized_team_name,))
            row = cur.fetchone()
            if row:
                return row[0]

    insert_query = """
        INSERT INTO core.teams (
            source_name,
            source_team_id,
            team_name,
            normalized_team_name,
            country_name
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING team_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                insert_query,
                (
                    "fbref",
                    None,
                    team_name,
                    normalized_team_name,
                    None,
                ),
            )
            team_id = cur.fetchone()[0]
        conn.commit()

    return team_id


def get_player_by_normalized_name(normalized_player_name: str) -> int | None:
    db_pool = get_db_pool()

    query = """
        SELECT player_id
        FROM core.players
        WHERE normalized_player_name = %s
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (normalized_player_name,))
            row = cur.fetchone()

    return row[0] if row else None


def upsert_player_from_fbref(payload: dict[str, Any]) -> int:
    db_pool = get_db_pool()

    player_name = first_not_null(payload, "Player", "player", "player_name")
    if not player_name:
        raise ValueError("Registro FBref sem nome de jogador.")

    normalized_player_name = normalize_text(player_name)
    nationality = first_not_null(payload, "Nation", "nation", "nationality")
    age = to_int(first_not_null(payload, "Age", "age"))
    position = first_not_null(payload, "Pos", "position", "Position")

    existing_player_id = get_player_by_normalized_name(normalized_player_name)
    if existing_player_id:
        update_query = """
            UPDATE core.players
            SET
                player_name = COALESCE(%s, player_name),
                age = COALESCE(%s, age),
                nationality = COALESCE(%s, nationality),
                preferred_position = COALESCE(%s, preferred_position),
                updated_at = NOW()
            WHERE player_id = %s
        """

        with db_pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    update_query,
                    (
                        player_name,
                        age,
                        nationality,
                        position,
                        existing_player_id,
                    ),
                )
            conn.commit()

        return existing_player_id

    insert_query = """
        INSERT INTO core.players (
            player_name,
            normalized_player_name,
            age,
            nationality,
            preferred_position
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING player_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                insert_query,
                (
                    player_name,
                    normalized_player_name,
                    age,
                    nationality,
                    position,
                ),
            )
            player_id = cur.fetchone()[0]
        conn.commit()

    return player_id


def find_season_id(season_name: str = DEFAULT_SEASON_NAME, competition_name: str | None = DEFAULT_COMPETITION_NAME) -> int | None:
    db_pool = get_db_pool()

    if competition_name:
        query = """
            SELECT s.season_id
            FROM core.seasons s
            JOIN core.competitions c
              ON c.competition_id = s.competition_id
            WHERE s.season_name = %s
              AND c.competition_name = %s
            LIMIT 1
        """
        params = (season_name, competition_name)
    else:
        query = """
            SELECT season_id
            FROM core.seasons
            WHERE season_name = %s
            LIMIT 1
        """
        params = (season_name,)

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()

    return row[0] if row else None


def find_competition_id_by_name(competition_name: str | None) -> int | None:
    if not competition_name:
        return None

    db_pool = get_db_pool()

    query = """
        SELECT competition_id
        FROM core.competitions
        WHERE LOWER(competition_name) = LOWER(%s)
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (competition_name,))
            row = cur.fetchone()

    return row[0] if row else None


def upsert_player_season_stats(
    payload: dict[str, Any],
    player_id: int,
    team_id: int | None,
    season_id: int | None,
    competition_id: int | None,
) -> None:
    if not team_id or not season_id:
        return

    db_pool = get_db_pool()

    matches_played = to_int(first_not_null(payload, "Playing Time_MP", "MP", "matches", "Matches"))
    starts = to_int(first_not_null(payload, "Playing Time_Starts", "Starts", "starts"))
    minutes_played = to_int(first_not_null(payload, "Playing Time_Min", "Min", "minutes", "Minutes"))

    goals = to_int(first_not_null(payload, "Performance_Gls", "Gls", "goals", "Goals"))
    assists = to_int(first_not_null(payload, "Performance_Ast", "Ast", "assists", "Assists"))
    shots = to_int(first_not_null(payload, "Performance_Sh", "Sh", "shots", "Shots"))
    shots_on_target = to_int(first_not_null(payload, "Performance_SoT", "SoT", "shots_on_target"))
    xg = to_float(first_not_null(payload, "Expected_xG", "xG"))
    xa = to_float(first_not_null(payload, "Expected_xAG", "xAG", "xA"))

    progressive_passes = to_int(first_not_null(payload, "Progression_PrgP", "PrgP"))
    progressive_carries = to_int(first_not_null(payload, "Progression_PrgC", "PrgC"))
    key_passes = to_int(first_not_null(payload, "KP", "Passes_KP", "key_passes"))

    tackles = to_int(first_not_null(payload, "Tackles_Tkl", "Tkl", "tackles"))
    interceptions = to_int(first_not_null(payload, "Int", "interceptions"))
    blocks = to_int(first_not_null(payload, "Blocks_Blocks", "blocks"))
    clearances = to_int(first_not_null(payload, "Clr", "clearances"))
    aerial_duels_won = to_int(first_not_null(payload, "Aerial Duels_Won", "Won", "aerial_duels_won"))

    yellow_cards = to_int(first_not_null(payload, "Performance_CrdY", "CrdY", "yellow_cards"))
    red_cards = to_int(first_not_null(payload, "Performance_CrdR", "CrdR", "red_cards"))

    query = """
        INSERT INTO core.player_season_stats (
            player_id,
            team_id,
            competition_id,
            season_id,
            matches_played,
            starts,
            minutes_played,
            goals,
            assists,
            shots,
            shots_on_target,
            xg,
            xa,
            progressive_passes,
            progressive_carries,
            key_passes,
            tackles,
            interceptions,
            blocks,
            clearances,
            aerial_duels_won,
            yellow_cards,
            red_cards
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (player_id, team_id, season_id)
        DO UPDATE SET
            competition_id = EXCLUDED.competition_id,
            matches_played = EXCLUDED.matches_played,
            starts = EXCLUDED.starts,
            minutes_played = EXCLUDED.minutes_played,
            goals = EXCLUDED.goals,
            assists = EXCLUDED.assists,
            shots = EXCLUDED.shots,
            shots_on_target = EXCLUDED.shots_on_target,
            xg = EXCLUDED.xg,
            xa = EXCLUDED.xa,
            progressive_passes = EXCLUDED.progressive_passes,
            progressive_carries = EXCLUDED.progressive_carries,
            key_passes = EXCLUDED.key_passes,
            tackles = EXCLUDED.tackles,
            interceptions = EXCLUDED.interceptions,
            blocks = EXCLUDED.blocks,
            clearances = EXCLUDED.clearances,
            aerial_duels_won = EXCLUDED.aerial_duels_won,
            yellow_cards = EXCLUDED.yellow_cards,
            red_cards = EXCLUDED.red_cards,
            updated_at = NOW()
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    player_id,
                    team_id,
                    competition_id,
                    season_id,
                    matches_played,
                    starts,
                    minutes_played,
                    goals,
                    assists,
                    shots,
                    shots_on_target,
                    xg,
                    xa,
                    progressive_passes,
                    progressive_carries,
                    key_passes,
                    tackles,
                    interceptions,
                    blocks,
                    clearances,
                    aerial_duels_won,
                    yellow_cards,
                    red_cards,
                ),
            )
        conn.commit()


def process_fbref_record(payload: dict[str, Any]) -> None:
    player_id = upsert_player_from_fbref(payload)

    team_name = first_not_null(payload, "Squad", "Team", "team_name", "team")
    team_id = get_or_create_team(team_name)

    competition_name = first_not_null(payload, "Comp", "competition", "league")
    competition_id = find_competition_id_by_name(competition_name)
    season_id = find_season_id(DEFAULT_SEASON_NAME, competition_name)

    if season_id is None:
        season_id = find_season_id(DEFAULT_SEASON_NAME)

    upsert_player_season_stats(
        payload=payload,
        player_id=player_id,
        team_id=team_id,
        season_id=season_id,
        competition_id=competition_id,
    )


def main():
    rows = fetch_raw_fbref_players()

    if not rows:
        raise ValueError("Nenhum registro encontrado em raw.fbref_players.")

    print(f"Total de registros FBref encontrados: {len(rows)}")

    processed = 0
    errors = 0

    for idx, row in enumerate(rows, start=1):
        payload = row["payload"]

        try:
            process_fbref_record(payload)
            processed += 1

            if idx % 500 == 0:
                print(f"[{idx}/{len(rows)}] processados")
        except Exception as e:
            errors += 1
            player_name = first_not_null(payload, "Player", "player", "player_name")
            print(f"Erro ao processar FBref player '{player_name}': {e}")

    print("\nNormalização de FBref finalizada.")
    print(f"Processados com sucesso: {processed}")
    print(f"Erros: {errors}")


if __name__ == "__main__":
    main()