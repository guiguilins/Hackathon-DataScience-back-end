from __future__ import annotations

from datetime import datetime
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


def build_match_datetime(match_date: str | None, kick_off: str | None) -> datetime | None:
    if not match_date or not kick_off:
        return None

    try:
        clean_kick_off = kick_off.split(".")[0]
        return datetime.strptime(f"{match_date} {clean_kick_off}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def fetch_raw_matches() -> list[dict[str, Any]]:
    db_pool = get_db_pool()

    query = """
        SELECT payload
        FROM raw.statsbomb_matches
        ORDER BY raw_match_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [row["payload"] for row in rows]


def upsert_competition(payload: dict[str, Any]) -> int:
    db_pool = get_db_pool()

    competition = payload.get("competition", {})
    source_name = "statsbomb"
    source_competition_id = str(competition.get("competition_id"))
    competition_name = competition.get("competition_name")
    country_name = competition.get("country_name")
    gender = competition.get("competition_gender")

    query = """
        INSERT INTO core.competitions (
            source_name,
            source_competition_id,
            competition_name,
            country_name,
            gender
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source_name, source_competition_id)
        DO UPDATE SET
            competition_name = EXCLUDED.competition_name,
            country_name = EXCLUDED.country_name,
            gender = EXCLUDED.gender,
            updated_at = NOW()
        RETURNING competition_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    source_name,
                    source_competition_id,
                    competition_name,
                    country_name,
                    gender,
                ),
            )
            competition_id = cur.fetchone()[0]
        conn.commit()

    return competition_id


def upsert_season(payload: dict[str, Any], competition_id: int) -> int:
    db_pool = get_db_pool()

    season = payload.get("season", {})
    source_name = "statsbomb"
    source_season_id = str(season.get("season_id"))
    season_name = season.get("season_name")

    query = """
        INSERT INTO core.seasons (
            competition_id,
            source_name,
            source_season_id,
            season_name
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (competition_id, season_name)
        DO UPDATE SET
            source_name = EXCLUDED.source_name,
            source_season_id = EXCLUDED.source_season_id,
            updated_at = NOW()
        RETURNING season_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    competition_id,
                    source_name,
                    source_season_id,
                    season_name,
                ),
            )
            season_id = cur.fetchone()[0]
        conn.commit()

    return season_id


def upsert_team(team_data: dict[str, Any]) -> int:
    db_pool = get_db_pool()

    source_name = "statsbomb"
    source_team_id = str(
        team_data.get("home_team_id")
        or team_data.get("away_team_id")
        or team_data.get("team_id")
    )
    team_name = (
        team_data.get("home_team_name")
        or team_data.get("away_team_name")
        or team_data.get("team_name")
    )
    normalized_team_name = normalize_text(team_name)
    country_name = (
        team_data.get("country", {}).get("name")
        if isinstance(team_data.get("country"), dict)
        else None
    )

    query = """
        INSERT INTO core.teams (
            source_name,
            source_team_id,
            team_name,
            normalized_team_name,
            country_name
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (normalized_team_name)
        DO UPDATE SET
            source_name = EXCLUDED.source_name,
            source_team_id = EXCLUDED.source_team_id,
            team_name = EXCLUDED.team_name,
            country_name = EXCLUDED.country_name,
            updated_at = NOW()
        RETURNING team_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    source_name,
                    source_team_id,
                    team_name,
                    normalized_team_name,
                    country_name,
                ),
            )
            team_id = cur.fetchone()[0]
        conn.commit()

    return team_id


def upsert_coach(coach_name: str | None) -> int | None:
    if not coach_name:
        return None

    db_pool = get_db_pool()
    normalized_coach_name = normalize_text(coach_name)

    query = """
        INSERT INTO core.coaches (
            coach_name,
            normalized_coach_name
        )
        VALUES (%s, %s)
        ON CONFLICT (normalized_coach_name)
        DO UPDATE SET
            coach_name = EXCLUDED.coach_name,
            updated_at = NOW()
        RETURNING coach_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    coach_name,
                    normalized_coach_name,
                ),
            )
            coach_id = cur.fetchone()[0]
        conn.commit()

    return coach_id


def upsert_match(
    payload: dict[str, Any],
    competition_id: int,
    season_id: int,
    home_team_id: int,
    away_team_id: int,
    home_coach_id: int | None,
    away_coach_id: int | None,
) -> int:
    db_pool = get_db_pool()

    source_name = "statsbomb"
    source_match_id = str(payload.get("match_id"))

    match_date = payload.get("match_date")
    kick_off = payload.get("kick_off")
    match_datetime = build_match_datetime(match_date, kick_off)

    round_name = get_nested(payload, "competition_stage", "name")
    stadium_name = get_nested(payload, "stadium", "name")
    referee_name = get_nested(payload, "referee", "name")

    home_score = payload.get("home_score")
    away_score = payload.get("away_score")

    query = """
        INSERT INTO core.matches (
            source_name,
            source_match_id,
            competition_id,
            season_id,
            match_date,
            match_datetime,
            round_name,
            stadium_name,
            referee_name,
            home_team_id,
            away_team_id,
            home_score,
            away_score,
            home_coach_id,
            away_coach_id
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (source_name, source_match_id)
        DO UPDATE SET
            competition_id = EXCLUDED.competition_id,
            season_id = EXCLUDED.season_id,
            match_date = EXCLUDED.match_date,
            match_datetime = EXCLUDED.match_datetime,
            round_name = EXCLUDED.round_name,
            stadium_name = EXCLUDED.stadium_name,
            referee_name = EXCLUDED.referee_name,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            home_coach_id = EXCLUDED.home_coach_id,
            away_coach_id = EXCLUDED.away_coach_id,
            updated_at = NOW()
        RETURNING match_id
    """

    with db_pool.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    source_name,
                    source_match_id,
                    competition_id,
                    season_id,
                    match_date,
                    match_datetime,
                    round_name,
                    stadium_name,
                    referee_name,
                    home_team_id,
                    away_team_id,
                    home_score,
                    away_score,
                    home_coach_id,
                    away_coach_id,
                ),
            )
            match_id = cur.fetchone()[0]
        conn.commit()

    return match_id


def process_match(payload: dict[str, Any]) -> int:
    competition_id = upsert_competition(payload)
    season_id = upsert_season(payload, competition_id)

    home_team = payload.get("home_team", {})
    away_team = payload.get("away_team", {})

    home_team_id = upsert_team(home_team)
    away_team_id = upsert_team(away_team)

    home_managers = get_nested(home_team, "managers", default=None)
    away_managers = get_nested(away_team, "managers", default=None)

    home_coach_name = None
    away_coach_name = None

    if isinstance(home_managers, list) and home_managers:
        home_coach_name = home_managers[0].get("name")

    if isinstance(away_managers, list) and away_managers:
        away_coach_name = away_managers[0].get("name")

    home_coach_id = upsert_coach(home_coach_name)
    away_coach_id = upsert_coach(away_coach_name)

    match_id = upsert_match(
        payload=payload,
        competition_id=competition_id,
        season_id=season_id,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_coach_id=home_coach_id,
        away_coach_id=away_coach_id,
    )

    return match_id


def main():
    raw_matches = fetch_raw_matches()

    if not raw_matches:
        raise ValueError("Nenhum registro encontrado em raw.statsbomb_matches.")

    print(f"Total de partidas brutas encontradas: {len(raw_matches)}")

    processed = 0
    errors = 0

    for idx, payload in enumerate(raw_matches, start=1):
        try:
            match_id = process_match(payload)
            processed += 1

            if idx % 100 == 0:
                print(f"[{idx}/{len(raw_matches)}] match_id core={match_id}")
        except Exception as e:
            errors += 1
            raw_match_id = payload.get("match_id")
            print(f"Erro ao processar partida StatsBomb {raw_match_id}: {e}")

    print("\nNormalização de matches finalizada.")
    print(f"Processados com sucesso: {processed}")
    print(f"Erros: {errors}")


if __name__ == "__main__":
    main()