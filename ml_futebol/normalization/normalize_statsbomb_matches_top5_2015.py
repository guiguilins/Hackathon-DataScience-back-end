from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from database.db import get_db_pool


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def extract_source_match_id_from_source_file(source_file: str) -> str:
    return source_file.split("\\")[-1].split("/")[-1].replace(".json", "")


def fetch_target_source_match_ids() -> set[str]:
    db_pool = get_db_pool()

    query = """
        SELECT raw_payload ->> 'match_id' AS source_match_id
        FROM silver.matches
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return {str(row[0]) for row in rows if row[0] is not None}


def get_core_match_id(source_match_id: str) -> int | None:
    db_pool = get_db_pool()

    query = """
        SELECT match_id
        FROM core.matches
        WHERE source_name = 'statsbomb'
          AND source_match_id = %s
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (source_match_id,))
            row = cur.fetchone()

    return row[0] if row else None


def fetch_raw_lineups() -> list[dict[str, Any]]:
    db_pool = get_db_pool()

    query = """
        SELECT payload, source_file
        FROM raw.statsbomb_lineups
        ORDER BY raw_lineup_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()

    valid_source_match_ids = fetch_target_source_match_ids()

    filtered_rows = [
        row
        for row in rows
        if extract_source_match_id_from_source_file(row["source_file"]) in valid_source_match_ids
    ]

    return filtered_rows


def get_silver_match_id(source_match_id: str) -> int | None:
    db_pool = get_db_pool()

    query = """
        SELECT match_id
        FROM silver.matches
        WHERE source_match_id = %s
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (source_match_id,))
            row = cur.fetchone()

    return row[0] if row else None


def get_core_team_id(source_team_id: str | None, team_name: str | None) -> int | None:
    db_pool = get_db_pool()

    if source_team_id:
        query = """
            SELECT team_id
            FROM core.teams
            WHERE source_name = 'statsbomb'
              AND source_team_id = %s
            LIMIT 1
        """
        with db_pool.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (str(source_team_id),))
                row = cur.fetchone()
        if row:
            return row[0]

    normalized_team_name = normalize_text(team_name)
    if not normalized_team_name:
        return None

    query = """
        SELECT team_id
        FROM core.teams
        WHERE normalized_team_name = %s
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (normalized_team_name,))
            row = cur.fetchone()

    return row[0] if row else None


def upsert_player(player_data: dict[str, Any]) -> int:
    db_pool = get_db_pool()

    player_name = player_data.get("player_name")
    normalized_player_name = normalize_text(player_name)
    source_player_id = str(player_data.get("player_id")) if player_data.get("player_id") is not None else None
    country = player_data.get("country", {})
    nationality = country.get("name") if isinstance(country, dict) else None

    positions = player_data.get("positions", [])
    first_position = positions[0] if positions else {}
    position_name = first_position.get("position")

    query = """
        INSERT INTO core.players (
            player_name,
            normalized_player_name,
            nationality,
            preferred_position
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING player_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    player_name,
                    normalized_player_name,
                    nationality,
                    position_name,
                ),
            )
            inserted = cur.fetchone()

            if inserted:
                player_id = inserted[0]
                conn.commit()
                return player_id

        conn.commit()

    query_select = """
        SELECT player_id
        FROM core.players
        WHERE normalized_player_name = %s
        LIMIT 1
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query_select, (normalized_player_name,))
            row = cur.fetchone()

    if not row:
        raise ValueError(f"Não foi possível localizar/inserir player: {player_name} ({source_player_id})")

    return row[0]


def upsert_lineup(
    match_id: int,
    team_id: int,
    player_id: int,
    player_data: dict[str, Any],
) -> None:
    db_pool = get_db_pool()

    positions = player_data.get("positions", [])
    first_position = positions[0] if positions else {}

    position_name = first_position.get("position")
    starter = bool(first_position.get("start_reason")) if positions else False
    jersey_number = player_data.get("jersey_number")
    minutes_played = None

    if positions:
        from_minute = first_position.get("from")
        to_minute = first_position.get("to")
        if from_minute is not None and to_minute is not None:
            try:
                minutes_played = int(to_minute) - int(from_minute)
            except Exception:
                minutes_played = None

    query = """
        INSERT INTO core.lineups (
            match_id,
            team_id,
            player_id,
            position_name,
            starter,
            jersey_number,
            minutes_played
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id, team_id, player_id)
        DO UPDATE SET
            position_name = EXCLUDED.position_name,
            starter = EXCLUDED.starter,
            jersey_number = EXCLUDED.jersey_number,
            minutes_played = EXCLUDED.minutes_played,
            updated_at = NOW()
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    match_id,
                    team_id,
                    player_id,
                    position_name,
                    starter,
                    jersey_number,
                    minutes_played,
                ),
            )
        conn.commit()


def process_raw_lineup(payload: dict[str, Any], source_file: str) -> tuple[int, int]:
    source_match_id = extract_source_match_id_from_source_file(source_file)
    match_id = get_core_match_id(source_match_id)

    if not match_id:
        raise ValueError(f"Match não encontrado em core.matches para source_match_id={source_match_id}")

    team_name = payload.get("team_name")
    team_id_raw = payload.get("team_id")

    team_id = get_core_team_id(str(team_id_raw) if team_id_raw is not None else None, team_name)
    if not team_id:
        raise ValueError(f"Team não encontrado em core.teams para team_id={team_id_raw}, team_name={team_name}")

    lineup = payload.get("lineup", [])
    players_processed = 0

    for player_data in lineup:
        player_id = upsert_player(player_data)
        upsert_lineup(
            match_id=match_id,
            team_id=team_id,
            player_id=player_id,
            player_data=player_data,
        )
        players_processed += 1

    return match_id, players_processed


def main():
    raw_lineups = fetch_raw_lineups()

    if not raw_lineups:
        raise ValueError("Nenhum registro encontrado em raw.statsbomb_lineups para o recorte de silver.matches.")

    print(f"Total de lineups brutas encontradas no recorte: {len(raw_lineups)}")

    processed = 0
    errors = 0
    total_players = 0

    for idx, row in enumerate(raw_lineups, start=1):
        payload = row["payload"]
        source_file = row["source_file"]

        try:
            match_id, players_processed = process_raw_lineup(payload, source_file)
            processed += 1
            total_players += players_processed

            if idx % 100 == 0:
                print(f"[{idx}/{len(raw_lineups)}] match_id={match_id}, players={players_processed}")

        except Exception as e:
            errors += 1
            print(f"Erro ao processar lineup {source_file}: {e}")

    print("\nNormalização de lineups finalizada.")
    print(f"Lineups processadas com sucesso: {processed}")
    print(f"Jogadores processados: {total_players}")
    print(f"Erros: {errors}")


if __name__ == "__main__":
    main()