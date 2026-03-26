from __future__ import annotations

import time
from textwrap import dedent

from database.db import get_db_pool

TARGET_COMPETITIONS = (
    "Bundesliga",
    "La Liga",
    "Ligue 1",
    "Premier League",
    "Serie A",
)

TARGET_SEASON = "2015/2016"


class StatsBombSqlNormalizer:
    """
    Centraliza expressões SQL para normalizar source_file e extrair match_id
    com robustez, independentemente de slash/backslash.
    """

    @staticmethod
    def basename_expr(column_name: str) -> str:
        return (
            f"regexp_replace("
            f"replace({column_name}, E'\\\\\\\\', '/'), "
            f"'^.*/', "
            f"''"
            f")"
        )

    @staticmethod
    def match_id_from_source_file_expr(column_name: str) -> str:
        basename = StatsBombSqlNormalizer.basename_expr(column_name)
        return (
            f"NULLIF(substring({basename} from '([0-9]+)\\.json$'), '')::bigint"
        )


def log(message: str) -> None:
    print(f"[INFO] {message}")


def execute_sql(cur, sql: str, label: str) -> None:
    log(f"Iniciando: {label}")
    started_at = time.time()
    cur.execute(sql)
    elapsed = time.time() - started_at
    log(f"Finalizado: {label} ({elapsed:.2f}s)")


def fetch_scalar(cur, sql: str, label: str):
    cur.execute(sql)
    row = cur.fetchone()
    value = row[0] if row else None
    log(f"{label}: {value}")
    return value


def fetch_rows(cur, sql: str, label: str) -> list[tuple]:
    cur.execute(sql)
    rows = cur.fetchall()
    log(f"{label}: {len(rows)} linha(s)")
    return rows


def commit(conn, label: str) -> None:
    conn.commit()
    log(f"Commit realizado: {label}")


def set_session_settings(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            SET statement_timeout = 0;
            SET lock_timeout = 0;
            SET idle_in_transaction_session_timeout = 0;
        """),
        "Configurar timeouts da sessão",
    )


def create_schemas(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            CREATE SCHEMA IF NOT EXISTS curated;
            CREATE SCHEMA IF NOT EXISTS silver;
        """),
        "Criar schemas curated e silver",
    )


def create_raw_indexes(cur) -> None:
    raw_events_match_id_expr = StatsBombSqlNormalizer.match_id_from_source_file_expr("source_file")
    raw_lineups_match_id_expr = StatsBombSqlNormalizer.match_id_from_source_file_expr("source_file")

    execute_sql(
        cur,
        dedent(f"""
            CREATE INDEX IF NOT EXISTS idx_raw_matches_competition_name
            ON raw.statsbomb_matches ((payload -> 'competition' ->> 'competition_name'));

            CREATE INDEX IF NOT EXISTS idx_raw_matches_season_name
            ON raw.statsbomb_matches ((payload -> 'season' ->> 'season_name'));

            CREATE INDEX IF NOT EXISTS idx_raw_matches_match_id_text
            ON raw.statsbomb_matches ((payload ->> 'match_id'));

            CREATE INDEX IF NOT EXISTS idx_raw_events_source_file
            ON raw.statsbomb_events (source_file);

            CREATE INDEX IF NOT EXISTS idx_raw_lineups_source_file
            ON raw.statsbomb_lineups (source_file);

            CREATE INDEX IF NOT EXISTS idx_raw_events_match_id_from_source_file
            ON raw.statsbomb_events (({raw_events_match_id_expr}));

            CREATE INDEX IF NOT EXISTS idx_raw_lineups_match_id_from_source_file
            ON raw.statsbomb_lineups (({raw_lineups_match_id_expr}));
        """),
        "Criar índices nas tabelas raw",
    )


def drop_curated_tables(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            DROP TABLE IF EXISTS curated.statsbomb_matches_2015_top5 CASCADE;
            DROP TABLE IF EXISTS curated.target_matches_2015_top5 CASCADE;
            DROP TABLE IF EXISTS curated.statsbomb_events_2015_top5 CASCADE;
            DROP TABLE IF EXISTS curated.statsbomb_lineups_2015_top5 CASCADE;
        """),
        "Remover tabelas antigas de curated",
    )


def drop_silver_tables(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            DROP TABLE IF EXISTS silver.matches CASCADE;
            DROP TABLE IF EXISTS silver.events CASCADE;
            DROP TABLE IF EXISTS silver.lineups CASCADE;
            DROP TABLE IF EXISTS silver.lineup_players CASCADE;
        """),
        "Remover tabelas antigas de silver",
    )


def build_curated_matches(cur) -> None:
    competitions_sql = ", ".join(f"'{competition}'" for competition in TARGET_COMPETITIONS)

    execute_sql(
        cur,
        dedent(f"""
            CREATE TABLE curated.statsbomb_matches_2015_top5 AS
            SELECT
                r.*
            FROM raw.statsbomb_matches r
            WHERE
                (r.payload -> 'competition' ->> 'competition_name') IN ({competitions_sql})
                AND (r.payload -> 'season' ->> 'season_name') = '{TARGET_SEASON}';
        """),
        "Criar curated.statsbomb_matches_2015_top5",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE INDEX IF NOT EXISTS idx_curated_matches_2015_top5_match_id_text
            ON curated.statsbomb_matches_2015_top5 ((payload ->> 'match_id'));

            CREATE INDEX IF NOT EXISTS idx_curated_matches_2015_top5_competition_name
            ON curated.statsbomb_matches_2015_top5 ((payload -> 'competition' ->> 'competition_name'));

            CREATE INDEX IF NOT EXISTS idx_curated_matches_2015_top5_season_name
            ON curated.statsbomb_matches_2015_top5 ((payload -> 'season' ->> 'season_name'));
        """),
        "Criar índices de curated.statsbomb_matches_2015_top5",
    )


def build_curated_target_matches(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            CREATE TABLE curated.target_matches_2015_top5 AS
            SELECT
                payload ->> 'match_id' AS match_id_text,
                (payload ->> 'match_id')::bigint AS match_id,
                (payload ->> 'match_id') || '.json' AS match_file_name,
                NULLIF(payload -> 'competition' ->> 'competition_id', '')::int AS competition_id,
                payload -> 'competition' ->> 'competition_name' AS competition_name,
                NULLIF(payload -> 'season' ->> 'season_id', '')::int AS season_id,
                payload -> 'season' ->> 'season_name' AS season_name,
                TO_DATE(payload ->> 'match_date', 'YYYY-MM-DD') AS match_date,
                payload ->> 'kick_off' AS kick_off,
                NULLIF(payload -> 'home_team' ->> 'home_team_id', '')::bigint AS home_team_id,
                payload -> 'home_team' ->> 'home_team_name' AS home_team_name,
                NULLIF(payload -> 'away_team' ->> 'away_team_id', '')::bigint AS away_team_id,
                payload -> 'away_team' ->> 'away_team_name' AS away_team_name,
                NULLIF(payload ->> 'home_score', '')::int AS home_score,
                NULLIF(payload ->> 'away_score', '')::int AS away_score
            FROM curated.statsbomb_matches_2015_top5;
        """),
        "Criar curated.target_matches_2015_top5",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_curated_target_matches_2015_top5_match_id
            ON curated.target_matches_2015_top5 (match_id);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_curated_target_matches_2015_top5_match_id_text
            ON curated.target_matches_2015_top5 (match_id_text);

            CREATE INDEX IF NOT EXISTS idx_curated_target_matches_2015_top5_match_file_name
            ON curated.target_matches_2015_top5 (match_file_name);

            CREATE INDEX IF NOT EXISTS idx_curated_target_matches_2015_top5_competition_name
            ON curated.target_matches_2015_top5 (competition_name);

            CREATE INDEX IF NOT EXISTS idx_curated_target_matches_2015_top5_match_date
            ON curated.target_matches_2015_top5 (match_date);
        """),
        "Criar índices de curated.target_matches_2015_top5",
    )


def build_curated_events(cur) -> None:
    extracted_match_id_expr = StatsBombSqlNormalizer.match_id_from_source_file_expr("e.source_file")
    basename_expr = StatsBombSqlNormalizer.basename_expr("e.source_file")

    execute_sql(
        cur,
        dedent(f"""
            CREATE TABLE curated.statsbomb_events_2015_top5 AS
            SELECT
                e.*,
                {basename_expr} AS normalized_source_file,
                {extracted_match_id_expr} AS match_id
            FROM raw.statsbomb_events e
            JOIN curated.target_matches_2015_top5 tm
              ON {extracted_match_id_expr} = tm.match_id
            WHERE {extracted_match_id_expr} IS NOT NULL;
        """),
        "Criar curated.statsbomb_events_2015_top5",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE INDEX IF NOT EXISTS idx_curated_events_2015_top5_match_id
            ON curated.statsbomb_events_2015_top5 (match_id);

            CREATE INDEX IF NOT EXISTS idx_curated_events_2015_top5_source_file
            ON curated.statsbomb_events_2015_top5 (source_file);

            CREATE INDEX IF NOT EXISTS idx_curated_events_2015_top5_type_name
            ON curated.statsbomb_events_2015_top5 ((payload -> 'type' ->> 'name'));
        """),
        "Criar índices de curated.statsbomb_events_2015_top5",
    )


def build_curated_lineups(cur) -> None:
    extracted_match_id_expr = StatsBombSqlNormalizer.match_id_from_source_file_expr("l.source_file")
    basename_expr = StatsBombSqlNormalizer.basename_expr("l.source_file")

    execute_sql(
        cur,
        dedent(f"""
            CREATE TABLE curated.statsbomb_lineups_2015_top5 AS
            SELECT
                l.*,
                {basename_expr} AS normalized_source_file,
                {extracted_match_id_expr} AS match_id
            FROM raw.statsbomb_lineups l
            JOIN curated.target_matches_2015_top5 tm
              ON {extracted_match_id_expr} = tm.match_id
            WHERE {extracted_match_id_expr} IS NOT NULL;
        """),
        "Criar curated.statsbomb_lineups_2015_top5",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE INDEX IF NOT EXISTS idx_curated_lineups_2015_top5_match_id
            ON curated.statsbomb_lineups_2015_top5 (match_id);

            CREATE INDEX IF NOT EXISTS idx_curated_lineups_2015_top5_source_file
            ON curated.statsbomb_lineups_2015_top5 (source_file);
        """),
        "Criar índices de curated.statsbomb_lineups_2015_top5",
    )


def build_silver_matches(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            CREATE TABLE silver.matches AS
            SELECT
                (payload ->> 'match_id')::bigint AS match_id,
                NULLIF(payload -> 'competition' ->> 'competition_id', '')::int AS competition_id,
                payload -> 'competition' ->> 'competition_name' AS competition_name,
                NULLIF(payload -> 'season' ->> 'season_id', '')::int AS season_id,
                payload -> 'season' ->> 'season_name' AS season_name,
                TO_DATE(payload ->> 'match_date', 'YYYY-MM-DD') AS match_date,
                payload ->> 'kick_off' AS kick_off,
                NULLIF(payload -> 'home_team' ->> 'home_team_id', '')::bigint AS home_team_id,
                payload -> 'home_team' ->> 'home_team_name' AS home_team_name,
                NULLIF(payload -> 'away_team' ->> 'away_team_id', '')::bigint AS away_team_id,
                payload -> 'away_team' ->> 'away_team_name' AS away_team_name,
                NULLIF(payload ->> 'home_score', '')::int AS home_score,
                NULLIF(payload ->> 'away_score', '')::int AS away_score,
                CASE
                    WHEN NULLIF(payload ->> 'home_score', '')::int > NULLIF(payload ->> 'away_score', '')::int THEN 'H'
                    WHEN NULLIF(payload ->> 'home_score', '')::int < NULLIF(payload ->> 'away_score', '')::int THEN 'A'
                    ELSE 'D'
                END AS match_result,
                NULLIF(payload -> 'stadium' ->> 'id', '')::bigint AS stadium_id,
                payload -> 'stadium' ->> 'name' AS stadium_name,
                NULLIF(payload -> 'referee' ->> 'id', '')::bigint AS referee_id,
                payload -> 'referee' ->> 'name' AS referee_name,
                payload -> 'metadata' ->> 'data_version' AS data_version,
                payload -> 'metadata' ->> 'shot_fidelity_version' AS shot_fidelity_version,
                payload -> 'metadata' ->> 'xy_fidelity_version' AS xy_fidelity_version,
                source_dataset,
                source_file,
                payload AS raw_payload
            FROM curated.statsbomb_matches_2015_top5;
        """),
        "Criar silver.matches",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_matches_match_id
            ON silver.matches (match_id);

            CREATE INDEX IF NOT EXISTS idx_silver_matches_competition_name
            ON silver.matches (competition_name);

            CREATE INDEX IF NOT EXISTS idx_silver_matches_season_name
            ON silver.matches (season_name);

            CREATE INDEX IF NOT EXISTS idx_silver_matches_match_date
            ON silver.matches (match_date);

            CREATE INDEX IF NOT EXISTS idx_silver_matches_home_team_id
            ON silver.matches (home_team_id);

            CREATE INDEX IF NOT EXISTS idx_silver_matches_away_team_id
            ON silver.matches (away_team_id);
        """),
        "Criar índices de silver.matches",
    )


def build_silver_events(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            CREATE TABLE silver.events AS
            SELECT
                payload ->> 'id' AS event_id,
                match_id::bigint AS match_id,
                NULLIF(payload ->> 'index', '')::int AS event_index,
                NULLIF(payload ->> 'period', '')::int AS period,
                NULLIF(payload ->> 'timestamp', '')::text AS event_timestamp,
                NULLIF(payload ->> 'minute', '')::int AS minute,
                NULLIF(payload ->> 'second', '')::numeric AS second,
                NULLIF(payload ->> 'possession', '')::int AS possession,
                NULLIF(payload -> 'possession_team' ->> 'id', '')::bigint AS possession_team_id,
                payload -> 'possession_team' ->> 'name' AS possession_team_name,
                NULLIF(payload -> 'play_pattern' ->> 'id', '')::int AS play_pattern_id,
                payload -> 'play_pattern' ->> 'name' AS play_pattern_name,
                NULLIF(payload -> 'team' ->> 'id', '')::bigint AS team_id,
                payload -> 'team' ->> 'name' AS team_name,
                NULLIF(payload -> 'player' ->> 'id', '')::bigint AS player_id,
                payload -> 'player' ->> 'name' AS player_name,
                NULLIF(payload -> 'position' ->> 'id', '')::int AS position_id,
                payload -> 'position' ->> 'name' AS position_name,
                NULLIF(payload -> 'type' ->> 'id', '')::int AS event_type_id,
                payload -> 'type' ->> 'name' AS event_type_name,
                NULLIF(payload -> 'duration' #>> '{}', '')::numeric AS duration,
                NULLIF(payload ->> 'under_pressure', '')::boolean AS under_pressure,
                NULLIF(payload ->> 'off_camera', '')::boolean AS off_camera,
                NULLIF(payload ->> 'out', '')::boolean AS out_of_play,
                payload -> 'location' AS location,
                payload -> 'pass' AS pass_payload,
                payload -> 'shot' AS shot_payload,
                payload -> 'dribble' AS dribble_payload,
                payload -> 'carry' AS carry_payload,
                payload -> 'duel' AS duel_payload,
                payload -> 'interception' AS interception_payload,
                payload -> 'clearance' AS clearance_payload,
                payload -> 'goalkeeper' AS goalkeeper_payload,
                payload -> 'tactics' AS tactics_payload,
                source_dataset,
                source_file,
                payload AS raw_payload
            FROM curated.statsbomb_events_2015_top5
            WHERE match_id IS NOT NULL;
        """),
        "Criar silver.events",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE INDEX IF NOT EXISTS idx_silver_events_match_id
            ON silver.events (match_id);

            CREATE INDEX IF NOT EXISTS idx_silver_events_event_type_name
            ON silver.events (event_type_name);

            CREATE INDEX IF NOT EXISTS idx_silver_events_team_id
            ON silver.events (team_id);

            CREATE INDEX IF NOT EXISTS idx_silver_events_player_id
            ON silver.events (player_id);

            CREATE INDEX IF NOT EXISTS idx_silver_events_minute
            ON silver.events (minute);
        """),
        "Criar índices de silver.events",
    )


def build_silver_lineups(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            CREATE TABLE silver.lineups AS
            SELECT
                match_id::bigint AS match_id,
                NULLIF(payload ->> 'team_id', '')::bigint AS team_id,
                payload ->> 'team_name' AS team_name,
                source_dataset,
                source_file,
                payload AS raw_payload
            FROM curated.statsbomb_lineups_2015_top5
            WHERE match_id IS NOT NULL;
        """),
        "Criar silver.lineups",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE INDEX IF NOT EXISTS idx_silver_lineups_match_id
            ON silver.lineups (match_id);

            CREATE INDEX IF NOT EXISTS idx_silver_lineups_team_id
            ON silver.lineups (team_id);
        """),
        "Criar índices de silver.lineups",
    )


def build_silver_lineup_players(cur) -> None:
    execute_sql(
        cur,
        dedent("""
            CREATE TABLE silver.lineup_players AS
            SELECT
                l.match_id::bigint AS match_id,
                NULLIF(l.payload ->> 'team_id', '')::bigint AS team_id,
                l.payload ->> 'team_name' AS team_name,
                NULLIF(p.value ->> 'player_id', '')::bigint AS player_id,
                p.value ->> 'player_name' AS player_name,
                NULLIF(p.value ->> 'player_nickname', '')::text AS player_nickname,
                NULLIF(p.value ->> 'jersey_number', '')::int AS jersey_number,
                NULLIF(p.value -> 'country' ->> 'id', '')::bigint AS country_id,
                p.value -> 'country' ->> 'name' AS country_name,
                p.value -> 'positions' AS positions_payload,
                p.value -> 'cards' AS cards_payload,
                p.value AS raw_player_payload
            FROM curated.statsbomb_lineups_2015_top5 l
            CROSS JOIN LATERAL jsonb_array_elements(l.payload -> 'lineup') AS p(value)
            WHERE l.match_id IS NOT NULL;
        """),
        "Criar silver.lineup_players",
    )

    execute_sql(
        cur,
        dedent("""
            CREATE INDEX IF NOT EXISTS idx_silver_lineup_players_match_id
            ON silver.lineup_players (match_id);

            CREATE INDEX IF NOT EXISTS idx_silver_lineup_players_team_id
            ON silver.lineup_players (team_id);

            CREATE INDEX IF NOT EXISTS idx_silver_lineup_players_player_id
            ON silver.lineup_players (player_id);
        """),
        "Criar índices de silver.lineup_players",
    )


def validate_results(cur) -> None:
    total_matches = fetch_scalar(cur, "SELECT COUNT(*) FROM silver.matches;", "Total de matches em silver.matches")
    total_events = fetch_scalar(cur, "SELECT COUNT(*) FROM silver.events;", "Total de events em silver.events")
    total_lineups = fetch_scalar(cur, "SELECT COUNT(*) FROM silver.lineups;", "Total de lineups em silver.lineups")
    total_lineup_players = fetch_scalar(cur, "SELECT COUNT(*) FROM silver.lineup_players;", "Total de lineup_players em silver.lineup_players")

    total_raw_shots = fetch_scalar(
        cur,
        "SELECT COUNT(*) FROM raw.statsbomb_events WHERE payload -> 'type' ->> 'name' = 'Shot';",
        "Total de shots no raw",
    )
    total_curated_shots = fetch_scalar(
        cur,
        "SELECT COUNT(*) FROM curated.statsbomb_events_2015_top5 WHERE payload -> 'type' ->> 'name' = 'Shot';",
        "Total de shots no curated",
    )
    total_silver_shots = fetch_scalar(
        cur,
        "SELECT COUNT(*) FROM silver.events WHERE event_type_name = 'Shot';",
        "Total de shots no silver",
    )

    rows = fetch_rows(
        cur,
        dedent("""
            SELECT
                competition_name,
                season_name,
                COUNT(*) AS total_matches
            FROM silver.matches
            GROUP BY 1, 2
            ORDER BY 1, 2;
        """),
        "Resumo por competição",
    )

    print("\n=== RESUMO FINAL ===")
    print(f"Matches: {total_matches}")
    print(f"Events: {total_events}")
    print(f"Lineups: {total_lineups}")
    print(f"Lineup players: {total_lineup_players}")
    print(f"Shots raw: {total_raw_shots}")
    print(f"Shots curated: {total_curated_shots}")
    print(f"Shots silver: {total_silver_shots}")

    print("\n=== PARTIDAS POR LIGA ===")
    for competition_name, season_name, total in rows:
        print(f"{competition_name} | {season_name} | {total}")


def main() -> None:
    db_pool = get_db_pool()

    with db_pool.get_cursor() as (conn, cur):
        try:
            set_session_settings(cur)
            commit(conn, "Configuração da sessão")

            create_schemas(cur)
            commit(conn, "Criação de schemas")

            create_raw_indexes(cur)
            commit(conn, "Índices raw")

            drop_curated_tables(cur)
            commit(conn, "Limpeza curated")

            build_curated_matches(cur)
            commit(conn, "curated.matches")

            build_curated_target_matches(cur)
            commit(conn, "curated.target_matches")

            build_curated_events(cur)
            commit(conn, "curated.events")

            build_curated_lineups(cur)
            commit(conn, "curated.lineups")

            drop_silver_tables(cur)
            commit(conn, "Limpeza silver")

            build_silver_matches(cur)
            commit(conn, "silver.matches")

            build_silver_events(cur)
            commit(conn, "silver.events")

            build_silver_lineups(cur)
            commit(conn, "silver.lineups")

            build_silver_lineup_players(cur)
            commit(conn, "silver.lineup_players")

            validate_results(cur)
            conn.commit()

            print("\nPipeline concluída com sucesso.")

        except Exception as exc:
            conn.rollback()
            print("\n[ERRO] A pipeline falhou. Rollback executado.")
            raise exc


if __name__ == "__main__":
    main()