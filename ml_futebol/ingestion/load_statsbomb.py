from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from database.db import get_db_pool


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def get_nested(data: dict[str, Any], *keys: str, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_source_match_id_from_source_file(source_file: str) -> str:
    return source_file.split("\\")[-1].split("/")[-1].replace(".json", "")


def fetch_target_source_match_ids() -> set[str]:
    db_pool = get_db_pool()

    query = """
        SELECT source_match_id
        FROM core.matches
        WHERE source_name = 'statsbomb'
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return {str(row[0]) for row in rows}


def fetch_raw_events() -> list[dict[str, Any]]:
    db_pool = get_db_pool()

    query = """
        SELECT payload, source_file
        FROM raw.statsbomb_events
        ORDER BY raw_event_id
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


def get_core_player_id(source_player_id: str | None, player_name: str | None) -> int | None:
    db_pool = get_db_pool()

    normalized_player_name = normalize_text(player_name)

    if normalized_player_name:
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
        if row:
            return row[0]

    return None


def extract_location(payload: dict[str, Any]) -> tuple[float | None, float | None]:
    location = payload.get("location")

    if isinstance(location, list) and len(location) >= 2:
        try:
            x = float(location[0]) if location[0] is not None else None
            y = float(location[1]) if location[1] is not None else None
            return x, y
        except Exception:
            return None, None

    return None, None


def extract_end_location_from_pass(payload: dict[str, Any]) -> tuple[float | None, float | None]:
    pass_data = payload.get("pass", {})
    end_location = pass_data.get("end_location")

    if isinstance(end_location, list) and len(end_location) >= 2:
        try:
            end_x = float(end_location[0]) if end_location[0] is not None else None
            end_y = float(end_location[1]) if end_location[1] is not None else None
            return end_x, end_y
        except Exception:
            return None, None

    return None, None


def upsert_event(
    match_id: int,
    team_id: int | None,
    player_id: int | None,
    payload: dict[str, Any],
) -> None:
    db_pool = get_db_pool()

    source_event_id = payload.get("id")
    event_index = payload.get("index")
    period = payload.get("period")
    minute = payload.get("minute")
    second = payload.get("second")
    possession = payload.get("possession")

    event_type = payload.get("type", {})
    event_type_name = event_type.get("name")

    play_pattern = payload.get("play_pattern", {})
    play_pattern_name = play_pattern.get("name")

    possession_team = payload.get("possession_team", {})
    possession_team_name = possession_team.get("name")

    position = payload.get("position", {})
    position_name = position.get("name") if isinstance(position, dict) else None

    under_pressure = payload.get("under_pressure")
    off_camera = payload.get("off_camera")
    out_of_play = payload.get("out")
    timestamp = payload.get("timestamp")

    start_x, start_y = extract_location(payload)
    end_x, end_y = extract_end_location_from_pass(payload)

    query = """
        INSERT INTO core.match_events (
            match_id,
            source_name,
            source_event_id,
            team_id,
            player_id,
            event_index,
            period,
            minute,
            second,
            possession,
            event_type_name,
            play_pattern_name,
            possession_team_name,
            position_name,
            event_timestamp,
            under_pressure,
            off_camera,
            out_of_play,
            start_x,
            start_y,
            end_x,
            end_y,
            raw_payload
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s::jsonb
        )
        ON CONFLICT (source_name, source_event_id)
        DO UPDATE SET
            match_id = EXCLUDED.match_id,
            team_id = EXCLUDED.team_id,
            player_id = EXCLUDED.player_id,
            event_index = EXCLUDED.event_index,
            period = EXCLUDED.period,
            minute = EXCLUDED.minute,
            second = EXCLUDED.second,
            possession = EXCLUDED.possession,
            event_type_name = EXCLUDED.event_type_name,
            play_pattern_name = EXCLUDED.play_pattern_name,
            possession_team_name = EXCLUDED.possession_team_name,
            position_name = EXCLUDED.position_name,
            event_timestamp = EXCLUDED.event_timestamp,
            under_pressure = EXCLUDED.under_pressure,
            off_camera = EXCLUDED.off_camera,
            out_of_play = EXCLUDED.out_of_play,
            start_x = EXCLUDED.start_x,
            start_y = EXCLUDED.start_y,
            end_x = EXCLUDED.end_x,
            end_y = EXCLUDED.end_y,
            raw_payload = EXCLUDED.raw_payload,
            updated_at = NOW()
    """

    import json

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    match_id,
                    "statsbomb",
                    str(source_event_id),
                    team_id,
                    player_id,
                    event_index,
                    period,
                    minute,
                    second,
                    possession,
                    event_type_name,
                    play_pattern_name,
                    possession_team_name,
                    position_name,
                    timestamp,
                    under_pressure,
                    off_camera,
                    out_of_play,
                    start_x,
                    start_y,
                    end_x,
                    end_y,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        conn.commit()


def process_raw_event(payload: dict[str, Any], source_file: str) -> int:
    source_match_id = extract_source_match_id_from_source_file(source_file)
    match_id = get_core_match_id(source_match_id)

    if not match_id:
        raise ValueError(f"Match não encontrado em core.matches para source_match_id={source_match_id}")

    team = payload.get("team", {})
    team_id_raw = team.get("id") if isinstance(team, dict) else None
    team_name = team.get("name") if isinstance(team, dict) else None
    team_id = get_core_team_id(str(team_id_raw) if team_id_raw is not None else None, team_name)

    player = payload.get("player", {})
    player_id_raw = player.get("id") if isinstance(player, dict) else None
    player_name = player.get("name") if isinstance(player, dict) else None
    player_id = get_core_player_id(str(player_id_raw) if player_id_raw is not None else None, player_name)

    upsert_event(
        match_id=match_id,
        team_id=team_id,
        player_id=player_id,
        payload=payload,
    )

    return match_id


def main():
    raw_events = fetch_raw_events()

    if not raw_events:
        raise ValueError("Nenhum registro encontrado em raw.statsbomb_events para o recorte alvo.")

    print(f"Total de events brutos encontrados no recorte: {len(raw_events)}")

    processed = 0
    errors = 0

    for idx, row in enumerate(raw_events, start=1):
        payload = row["payload"]
        source_file = row["source_file"]

        try:
            match_id = process_raw_event(payload, source_file)
            processed += 1

            if idx % 10000 == 0:
                print(f"[{idx}/{len(raw_events)}] match_id={match_id}")
        except Exception as e:
            errors += 1
            event_id = payload.get("id")
            print(f"Erro ao processar event {event_id} ({source_file}): {e}")

    print("\nNormalização de events finalizada.")
    print(f"Events processados com sucesso: {processed}")
    print(f"Erros: {errors}")


if __name__ == "__main__":
    main()