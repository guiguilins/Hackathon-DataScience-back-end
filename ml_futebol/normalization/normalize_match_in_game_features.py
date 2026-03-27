from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence, Any

from psycopg.rows import dict_row

from database.db import get_db_pool


@dataclass
class InGameFeatureStoreBuilder:
    target_competitions: Sequence[str]
    target_season: str
    statement_timeout_ms: int | None = None

    def build(self) -> None:
        db_pool = get_db_pool()

        with db_pool.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                self._configure_session(cur)
                self._create_schema(cur)
                self._create_indexes(cur)
                conn.commit()

                self._create_filtered_matches(cur)
                conn.commit()

                self._create_event_by_minute(cur)
                conn.commit()

                self._create_team_minute_rollups(cur)
                conn.commit()

                self._create_final_table(cur)
                conn.commit()

                self._analyze_tables(cur)
                conn.commit()

    def _configure_session(self, cur: Any) -> None:
        if self.statement_timeout_ms is None:
            cur.execute("SET statement_timeout = 0;")
        else:
            cur.execute("SET statement_timeout = %s;", (int(self.statement_timeout_ms),))

        cur.execute("SET lock_timeout = 0;")
        cur.execute("SET idle_in_transaction_session_timeout = 0;")

    def _create_schema(self, cur: Any) -> None:
        cur.execute("CREATE SCHEMA IF NOT EXISTS feature_store;")

    def _create_indexes(self, cur: Any) -> None:
        queries = [
            """
            CREATE INDEX IF NOT EXISTS idx_silver_matches_comp_season_match
            ON silver.matches (competition_name, season_name, match_id);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_silver_events_match_team_minute
            ON silver.events (match_id, team_id, minute);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_silver_events_match
            ON silver.events (match_id);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_silver_events_type
            ON silver.events (event_type_name);
            """,
        ]
        for query in queries:
            cur.execute(query)

    def _create_filtered_matches(self, cur: Any) -> None:
        cur.execute("DROP TABLE IF EXISTS feature_store.tmp_filtered_matches CASCADE;")

        query = """
        CREATE TABLE feature_store.tmp_filtered_matches AS
        SELECT
            m.match_id,
            m.match_date,
            m.competition_name,
            m.season_name,
            m.home_team_id,
            m.home_team_name,
            m.away_team_id,
            m.away_team_name,
            m.home_score,
            m.away_score,
            m.match_result
        FROM silver.matches m
        WHERE m.competition_name = ANY(%s)
          AND m.season_name = %s;
        """
        cur.execute(query, (list(self.target_competitions), self.target_season))

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tmp_filtered_matches_match
            ON feature_store.tmp_filtered_matches (match_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tmp_filtered_matches_home
            ON feature_store.tmp_filtered_matches (match_id, home_team_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tmp_filtered_matches_away
            ON feature_store.tmp_filtered_matches (match_id, away_team_id);
        """)

    def _create_event_by_minute(self, cur: Any) -> None:
        cur.execute("DROP TABLE IF EXISTS feature_store.tmp_event_by_minute CASCADE;")

        query = """
        CREATE TABLE feature_store.tmp_event_by_minute AS
        WITH event_base AS (
            SELECT
                e.match_id,
                e.team_id,
                COALESCE(e.minute, 0) AS minute,
                e.event_type_name,

                CASE
                    WHEN e.event_type_name = 'Shot' THEN 1
                    ELSE 0
                END AS is_shot,

                CASE
                    WHEN e.event_type_name = 'Shot'
                     AND (e.shot_payload -> 'outcome' ->> 'name') = 'Goal'
                    THEN 1
                    ELSE 0
                END AS is_goal,

                CASE
                    WHEN e.event_type_name = 'Shot'
                     AND (e.shot_payload -> 'outcome' ->> 'name') IN ('Goal', 'Saved', 'Saved To Post')
                    THEN 1
                    ELSE 0
                END AS is_shot_on_target,

                CASE
                    WHEN e.event_type_name = 'Shot'
                    THEN COALESCE((e.shot_payload ->> 'statsbomb_xg')::numeric, 0)
                    ELSE 0
                END AS shot_xg,

                CASE
                    WHEN e.event_type_name = 'Foul Committed' THEN 1
                    ELSE 0
                END AS is_foul_committed,

                CASE
                    WHEN e.event_type_name = 'Card'
                     AND (e.raw_payload -> 'bad_behaviour' -> 'card' ->> 'name') = 'Red Card'
                    THEN 1
                    ELSE 0
                END AS is_red_card,

                CASE
                    WHEN e.event_type_name = 'Pass' THEN 1
                    ELSE 0
                END AS is_pass
            FROM silver.events e
            INNER JOIN feature_store.tmp_filtered_matches fm
                ON fm.match_id = e.match_id
            WHERE COALESCE(e.minute, 0) BETWEEN 1 AND 90
        )
        SELECT
            eb.match_id,
            eb.team_id,
            eb.minute,
            SUM(eb.is_shot) AS shots_minute,
            SUM(eb.is_goal) AS goals_minute,
            SUM(eb.is_shot_on_target) AS shots_on_target_minute,
            SUM(eb.shot_xg) AS xg_minute,
            SUM(eb.is_foul_committed) AS fouls_minute,
            SUM(eb.is_red_card) AS red_cards_minute,
            SUM(eb.is_pass) AS passes_minute
        FROM event_base eb
        GROUP BY
            eb.match_id,
            eb.team_id,
            eb.minute;
        """
        cur.execute(query)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tmp_event_by_minute_match_team_min
            ON feature_store.tmp_event_by_minute (match_id, team_id, minute);
        """)

    def _create_team_minute_rollups(self, cur: Any) -> None:
        cur.execute("DROP TABLE IF EXISTS feature_store.tmp_team_minute_rollups CASCADE;")

        query = """
        CREATE TABLE feature_store.tmp_team_minute_rollups AS
        WITH team_minute_grid AS (
            SELECT
                fm.match_id,
                fm.home_team_id AS team_id,
                gs.minute
            FROM feature_store.tmp_filtered_matches fm
            CROSS JOIN generate_series(1, 90) AS gs(minute)

            UNION ALL

            SELECT
                fm.match_id,
                fm.away_team_id AS team_id,
                gs.minute
            FROM feature_store.tmp_filtered_matches fm
            CROSS JOIN generate_series(1, 90) AS gs(minute)
        ),
        team_minute_features AS (
            SELECT
                tmg.match_id,
                tmg.team_id,
                tmg.minute,
                COALESCE(ebm.shots_minute, 0) AS shots_minute,
                COALESCE(ebm.goals_minute, 0) AS goals_minute,
                COALESCE(ebm.shots_on_target_minute, 0) AS shots_on_target_minute,
                COALESCE(ebm.xg_minute, 0) AS xg_minute,
                COALESCE(ebm.fouls_minute, 0) AS fouls_minute,
                COALESCE(ebm.red_cards_minute, 0) AS red_cards_minute,
                COALESCE(ebm.passes_minute, 0) AS passes_minute
            FROM team_minute_grid tmg
            LEFT JOIN feature_store.tmp_event_by_minute ebm
                ON ebm.match_id = tmg.match_id
               AND ebm.team_id = tmg.team_id
               AND ebm.minute = tmg.minute
        )
        SELECT
            tmf.match_id,
            tmf.team_id,
            tmf.minute,

            SUM(tmf.goals_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS score_now,

            SUM(tmf.shots_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS shots_cum,

            SUM(tmf.shots_on_target_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS shots_on_target_cum,

            SUM(tmf.xg_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS xg_cum,

            SUM(tmf.fouls_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS fouls_cum,

            SUM(tmf.red_cards_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS red_cards_cum,

            SUM(tmf.passes_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS passes_cum,

            SUM(tmf.shots_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) AS shots_last_10,

            SUM(tmf.shots_on_target_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) AS shots_on_target_last_10,

            SUM(tmf.xg_minute) OVER (
                PARTITION BY tmf.match_id, tmf.team_id
                ORDER BY tmf.minute
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) AS xg_last_10
        FROM team_minute_features tmf;
        """
        cur.execute(query)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tmp_team_minute_rollups_match_team_min
            ON feature_store.tmp_team_minute_rollups (match_id, team_id, minute);
        """)

    def _create_final_table(self, cur: Any) -> None:
        cur.execute("DROP TABLE IF EXISTS feature_store.match_in_game_features CASCADE;")

        query = """
        CREATE TABLE feature_store.match_in_game_features AS
        WITH minute_grid AS (
            SELECT
                fm.match_id,
                fm.match_date,
                fm.competition_name,
                fm.season_name,
                fm.home_team_id,
                fm.home_team_name,
                fm.away_team_id,
                fm.away_team_name,
                fm.match_result,
                gs.minute
            FROM feature_store.tmp_filtered_matches fm
            CROSS JOIN generate_series(1, 90) AS gs(minute)
        ),
        final_join AS (
            SELECT
                mg.match_id,
                mg.minute,
                mg.match_date,
                mg.competition_name,
                mg.season_name,
                mg.home_team_id,
                mg.home_team_name,
                mg.away_team_id,
                mg.away_team_name,
                mg.match_result,

                COALESCE(home.score_now, 0) AS home_score_now,
                COALESCE(away.score_now, 0) AS away_score_now,

                COALESCE(home.shots_cum, 0) AS home_shots_cum,
                COALESCE(away.shots_cum, 0) AS away_shots_cum,

                COALESCE(home.shots_on_target_cum, 0) AS home_shots_on_target_cum,
                COALESCE(away.shots_on_target_cum, 0) AS away_shots_on_target_cum,

                COALESCE(home.xg_cum, 0) AS home_xg_cum,
                COALESCE(away.xg_cum, 0) AS away_xg_cum,

                COALESCE(home.shots_last_10, 0) AS home_shots_last_10,
                COALESCE(away.shots_last_10, 0) AS away_shots_last_10,

                COALESCE(home.shots_on_target_last_10, 0) AS home_shots_on_target_last_10,
                COALESCE(away.shots_on_target_last_10, 0) AS away_shots_on_target_last_10,

                COALESCE(home.xg_last_10, 0) AS home_xg_last_10,
                COALESCE(away.xg_last_10, 0) AS away_xg_last_10,

                COALESCE(home.red_cards_cum, 0) AS home_red_cards,
                COALESCE(away.red_cards_cum, 0) AS away_red_cards,

                COALESCE(home.fouls_cum, 0) AS home_fouls_cum,
                COALESCE(away.fouls_cum, 0) AS away_fouls_cum,

                COALESCE(home.passes_cum, 0) AS home_passes_cum,
                COALESCE(away.passes_cum, 0) AS away_passes_cum
            FROM minute_grid mg
            LEFT JOIN feature_store.tmp_team_minute_rollups home
                ON home.match_id = mg.match_id
               AND home.team_id = mg.home_team_id
               AND home.minute = mg.minute
            LEFT JOIN feature_store.tmp_team_minute_rollups away
                ON away.match_id = mg.match_id
               AND away.team_id = mg.away_team_id
               AND away.minute = mg.minute
        )
        SELECT
            match_id,
            minute,
            match_date,
            competition_name,
            season_name,
            home_team_id,
            home_team_name,
            away_team_id,
            away_team_name,

            home_score_now,
            away_score_now,
            (home_score_now - away_score_now) AS goal_diff_now,

            home_shots_cum,
            away_shots_cum,
            home_shots_on_target_cum,
            away_shots_on_target_cum,
            home_xg_cum,
            away_xg_cum,

            home_shots_last_10,
            away_shots_last_10,
            home_shots_on_target_last_10,
            away_shots_on_target_last_10,
            home_xg_last_10,
            away_xg_last_10,

            home_red_cards,
            away_red_cards,
            home_fouls_cum,
            away_fouls_cum,
            home_passes_cum,
            away_passes_cum,

            (home_shots_cum - away_shots_cum) AS diff_shots_cum,
            (home_shots_on_target_cum - away_shots_on_target_cum) AS diff_shots_on_target_cum,
            (home_xg_cum - away_xg_cum) AS diff_xg_cum,

            (home_shots_last_10 - away_shots_last_10) AS diff_shots_last_10,
            (home_shots_on_target_last_10 - away_shots_on_target_last_10) AS diff_shots_on_target_last_10,
            (home_xg_last_10 - away_xg_last_10) AS diff_xg_last_10,

            (home_red_cards - away_red_cards) AS diff_red_cards,
            (home_fouls_cum - away_fouls_cum) AS diff_fouls_cum,
            (home_passes_cum - away_passes_cum) AS diff_passes_cum,

            (90 - minute) AS remaining_minutes,
            match_result AS target_result_final
        FROM final_join;
        """
        cur.execute(query)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_match_in_game_features_match_min
            ON feature_store.match_in_game_features (match_id, minute);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_match_in_game_features_comp_season
            ON feature_store.match_in_game_features (competition_name, season_name);
        """)

    def _analyze_tables(self, cur: Any) -> None:
        queries = [
            "ANALYZE silver.matches;",
            "ANALYZE silver.events;",
            "ANALYZE feature_store.tmp_filtered_matches;",
            "ANALYZE feature_store.tmp_event_by_minute;",
            "ANALYZE feature_store.tmp_team_minute_rollups;",
            "ANALYZE feature_store.match_in_game_features;",
        ]
        for query in queries:
            cur.execute(query)


if __name__ == "__main__":
    TARGET_COMPETITIONS = (
        "Bundesliga",
        "La Liga",
        "Ligue 1",
        "Premier League",
        "Serie A",
    )
    TARGET_SEASON = "2015/2016"

    builder = InGameFeatureStoreBuilder(
        target_competitions=TARGET_COMPETITIONS,
        target_season=TARGET_SEASON,
        statement_timeout_ms=None,
    )
    builder.build()
    print("Tabela feature_store.match_in_game_features criada com sucesso.")