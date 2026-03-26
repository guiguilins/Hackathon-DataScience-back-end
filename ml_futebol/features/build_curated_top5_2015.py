from __future__ import annotations

import time
from textwrap import dedent

from database.db import get_db_pool


def execute_sql(cur, sql: str, label: str) -> None:
    print(f"\n[START] {label}")
    started_at = time.time()
    cur.execute(sql)
    elapsed = time.time() - started_at
    print(f"[OK] {label} ({elapsed:.2f}s)")


def fetch_one_value(cur, sql: str, label: str):
    print(f"\n[CHECK] {label}")
    cur.execute(sql)
    row = cur.fetchone()
    value = row[0] if row else None
    print(f"[RESULT] {label}: {value}")
    return value


def main() -> None:
    db_pool = get_db_pool()

    with db_pool.get_cursor() as (conn, cur):
        # Timeout da sessão
        execute_sql(
            cur,
            """
            SET statement_timeout = 0;
            SET lock_timeout = 0;
            SET idle_in_transaction_session_timeout = 0;
            """,
            "Configurar timeouts da sessão",
        )
        conn.commit()

        # Schemas
        execute_sql(
            cur,
            dedent("""
                CREATE SCHEMA IF NOT EXISTS curated;
                CREATE SCHEMA IF NOT EXISTS silver;
            """),
            "Criar schemas curated e silver",
        )
        conn.commit()

        # Índices nas tabelas raw
        execute_sql(
            cur,
            dedent(r"""
                CREATE INDEX IF NOT EXISTS idx_raw_matches_competition
                ON raw.statsbomb_matches ((payload -> 'competition' ->> 'competition_name'));

                CREATE INDEX IF NOT EXISTS idx_raw_matches_season
                ON raw.statsbomb_matches ((payload -> 'season' ->> 'season_name'));

                CREATE INDEX IF NOT EXISTS idx_raw_matches_match_id
                ON raw.statsbomb_matches (((payload ->> 'match_id')));

                CREATE INDEX IF NOT EXISTS idx_raw_events_source_file
                ON raw.statsbomb_events (source_file);

                CREATE INDEX IF NOT EXISTS idx_raw_lineups_source_file
                ON raw.statsbomb_lineups (source_file);

                CREATE INDEX IF NOT EXISTS idx_raw_events_type_name
                ON raw.statsbomb_events ((payload -> 'type' ->> 'name'));
            """),
            "Criar índices nas tabelas raw",
        )
        conn.commit()

        # Limpeza
        execute_sql(
            cur,
            dedent("""
                DROP TABLE IF EXISTS curated.statsbomb_matches_2015_top5 CASCADE;
                DROP TABLE IF EXISTS curated.target_matches_2015_top5 CASCADE;
                DROP TABLE IF EXISTS curated.statsbomb_events_2015_top5 CASCADE;
                DROP TABLE IF EXISTS curated.statsbomb_lineups_2015_top5 CASCADE;

                DROP TABLE IF EXISTS silver.matches CASCADE;
                DROP TABLE IF EXISTS silver.events CASCADE;
                DROP TABLE IF EXISTS silver.lineups CASCADE;
                DROP TABLE IF EXISTS silver.lineup_players CASCADE;
            """),
            "Remover tabelas antigas",
        )
        conn.commit()

        # Curated matches
        execute_sql(
            cur,
            dedent("""
                CREATE TABLE curated.statsbomb_matches_2015_top5 AS
                SELECT
                    r.*
                FROM raw.statsbomb_matches r
                WHERE
                    (r.payload -> 'competition' ->> 'competition_name') IN (
                        'Bundesliga',
                        'La Liga',
                        'Ligue 1',
                        'Premier League',
                        'Serie A'
                    )
                    AND (r.payload -> 'season' ->> 'season_name') = '2015/2016';
            """),
            "Criar curated.statsbomb_matches_2015_top5",
        )
        conn.commit()

        fetch_one_value(
            cur,
            "SELECT COUNT(*) FROM curated.statsbomb_matches_2015_top5;",
            "Total de matches filtrados",
        )

        # Target matches
        execute_sql(
            cur,
            dedent("""
                CREATE TABLE curated.target_matches_2015_top5 AS
                SELECT
                    payload ->> 'match_id' AS match_id_text,
                    (payload ->> 'match_id')::bigint AS match_id,
                    payload -> 'competition' ->> 'competition_id' AS competition_id,
                    payload -> 'competition' ->> 'competition_name' AS competition_name,
                    payload -> 'season' ->> 'season_id' AS season_id,
                    payload -> 'season' ->> 'season_name' AS season_name,
                    TO_DATE(payload ->> 'match_date', 'YYYY-MM-DD') AS match_date,
                    payload ->> 'kick_off' AS kick_off,
                    payload -> 'home_team' ->> 'home_team_name' AS home_team_name,
                    payload -> 'away_team' ->> 'away_team_name' AS away_team_name,
                    (payload ->> 'match_id') || '.json' AS match_file_name
                FROM curated.statsbomb_matches_2015_top5;
            """),
            "Criar curated.target_matches_2015_top5",
        )
        conn.commit()

        execute_sql(
            cur,
            dedent("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_curated_target_matches_match_id
                ON curated.target_matches_2015_top5 (match_id);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_curated_target_matches_match_id_text
                ON curated.target_matches_2015_top5 (match_id_text);
            """),
            "Criar índices de curated.target_matches_2015_top5",
        )
        conn.commit()

        fetch_one_value(
            cur,
            "SELECT COUNT(*) FROM curated.target_matches_2015_top5;",
            "Total de target matches",
        )

        # Curated events
        execute_sql(
            cur,
            dedent(r"""
                CREATE TABLE curated.statsbomb_events_2015_top5 AS
                WITH normalized_events AS (
                    SELECT
                        e.*,
                        regexp_replace(replace(e.source_file, E'\\', '/'), '^.*/', '') AS normalized_source_file,
                        NULLIF(
                            substring(
                                regexp_replace(replace(e.source_file, E'\\', '/'), '^.*/', '')
                                from '([0-9]+)\.json$'
                            ),
                            ''
                        )::bigint AS extracted_match_id
                    FROM raw.statsbomb_events e
                )
                SELECT
                    e.*,
                    tm.match_id
                FROM normalized_events e
                JOIN curated.target_matches_2015_top5 tm
                    ON e.extracted_match_id = tm.match_id
                WHERE e.extracted_match_id IS NOT NULL;
            """),
            "Criar curated.statsbomb_events_2015_top5",
        )
        conn.commit()

        fetch_one_value(
            cur,
            "SELECT COUNT(*) FROM curated.statsbomb_events_2015_top5;",
            "Total de events filtrados",
        )

        fetch_one_value(
            cur,
            """
            SELECT COUNT(*)
            FROM curated.statsbomb_events_2015_top5
            WHERE payload -> 'type' ->> 'name' = 'Shot';
            """,
            "Total de shots no curated.events",
        )

        # Curated lineups
        execute_sql(
            cur,
            dedent(r"""
                CREATE TABLE curated.statsbomb_lineups_2015_top5 AS
                WITH normalized_lineups AS (
                    SELECT
                        l.*,
                        regexp_replace(replace(l.source_file, E'\\', '/'), '^.*/', '') AS normalized_source_file,
                        NULLIF(
                            substring(
                                regexp_replace(replace(l.source_file, E'\\', '/'), '^.*/', '')
                                from '([0-9]+)\.json$'
                            ),
                            ''
                        )::bigint AS extracted_match_id
                    FROM raw.statsbomb_lineups l
                )
                SELECT
                    l.*,
                    tm.match_id
                FROM normalized_lineups l
                JOIN curated.target_matches_2015_top5 tm
                    ON l.extracted_match_id = tm.match_id
                WHERE l.extracted_match_id IS NOT NULL;
            """),
            "Criar curated.statsbomb_lineups_2015_top5",
        )
        conn.commit()

        fetch_one_value(
            cur,
            "SELECT COUNT(*) FROM curated.statsbomb_lineups_2015_top5;",
            "Total de lineups filtrados",
        )

        # Índices curated
        execute_sql(
            cur,
            dedent("""
                CREATE INDEX IF NOT EXISTS idx_curated_events_match_id
                ON curated.statsbomb_events_2015_top5 (match_id);

                CREATE INDEX IF NOT EXISTS idx_curated_events_type_name
                ON curated.statsbomb_events_2015_top5 ((payload -> 'type' ->> 'name'));

                CREATE INDEX IF NOT EXISTS idx_curated_lineups_match_id
                ON curated.statsbomb_lineups_2015_top5 (match_id);
            """),
            "Criar índices nas tabelas curated",
        )
        conn.commit()

        # Silver matches
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
                    source_dataset,
                    source_file,
                    payload AS raw_payload
                FROM curated.statsbomb_matches_2015_top5;
            """),
            "Criar silver.matches",
        )
        conn.commit()

        execute_sql(
            cur,
            dedent("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_matches_match_id
                ON silver.matches (match_id);

                CREATE INDEX IF NOT EXISTS idx_silver_matches_competition_name
                ON silver.matches (competition_name);

                CREATE INDEX IF NOT EXISTS idx_silver_matches_match_date
                ON silver.matches (match_date);
            """),
            "Criar índices de silver.matches",
        )
        conn.commit()

        # Silver events
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
                    NULLIF(payload -> 'team' ->> 'id', '')::bigint AS team_id,
                    payload -> 'team' ->> 'name' AS team_name,
                    NULLIF(payload -> 'player' ->> 'id', '')::bigint AS player_id,
                    payload -> 'player' ->> 'name' AS player_name,
                    NULLIF(payload -> 'type' ->> 'id', '')::int AS event_type_id,
                    payload -> 'type' ->> 'name' AS event_type_name,
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
        conn.commit()

        execute_sql(
            cur,
            dedent("""
                CREATE INDEX IF NOT EXISTS idx_silver_events_match_id
                ON silver.events (match_id);

                CREATE INDEX IF NOT EXISTS idx_silver_events_event_type_name
                ON silver.events (event_type_name);

                CREATE INDEX IF NOT EXISTS idx_silver_events_team_id
                ON silver.events (team_id);
            """),
            "Criar índices de silver.events",
        )
        conn.commit()

        fetch_one_value(
            cur,
            """
            SELECT COUNT(*)
            FROM silver.events
            WHERE event_type_name = 'Shot';
            """,
            "Total de shots no silver.events",
        )

        # Silver lineups
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
        conn.commit()

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
        conn.commit()

        # Silver lineup_players
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
                    NULLIF(p.value ->> 'jersey_number', '')::int AS jersey_number,
                    p.value -> 'positions' AS positions_payload,
                    p.value -> 'cards' AS cards_payload,
                    p.value AS raw_player_payload
                FROM curated.statsbomb_lineups_2015_top5 l
                CROSS JOIN LATERAL jsonb_array_elements(l.payload -> 'lineup') AS p(value)
                WHERE l.match_id IS NOT NULL;
            """),
            "Criar silver.lineup_players",
        )
        conn.commit()

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
        conn.commit()

        print("\nPipeline concluída com sucesso.")

        print("\nResumo final:")
        cur.execute("""
            SELECT competition_name, COUNT(*)
            FROM silver.matches
            GROUP BY competition_name
            ORDER BY competition_name;
        """)
        for row in cur.fetchall():
            print(row)

        print("\nValidação de shots por amostra:")
        cur.execute("""
            SELECT
                team_name,
                COUNT(*) AS total_shots
            FROM silver.events
            WHERE match_id = 3825846
              AND event_type_name = 'Shot'
            GROUP BY team_name
            ORDER BY total_shots DESC;
        """)
        for row in cur.fetchall():
            print(row)


if __name__ == "__main__":
    main()