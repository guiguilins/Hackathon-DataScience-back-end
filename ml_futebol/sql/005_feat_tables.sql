CREATE SCHEMA IF NOT EXISTS feat;

CREATE TABLE IF NOT EXISTS feat.match_pre_game_features (
    feature_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL REFERENCES core.matches(match_id) ON DELETE CASCADE,
    competition_id BIGINT NOT NULL REFERENCES core.competitions(competition_id),
    season_id BIGINT NOT NULL REFERENCES core.seasons(season_id),
    match_date DATE NOT NULL,

    home_team_id BIGINT NOT NULL REFERENCES core.teams(team_id),
    away_team_id BIGINT NOT NULL REFERENCES core.teams(team_id),

    home_last5_points_avg NUMERIC(10,4),
    home_last5_goals_for_avg NUMERIC(10,4),
    home_last5_goals_against_avg NUMERIC(10,4),
    home_last5_win_rate NUMERIC(10,4),

    away_last5_points_avg NUMERIC(10,4),
    away_last5_goals_for_avg NUMERIC(10,4),
    away_last5_goals_against_avg NUMERIC(10,4),
    away_last5_win_rate NUMERIC(10,4),

    form_diff NUMERIC(10,4),
    goals_diff NUMERIC(10,4),
    defense_diff NUMERIC(10,4),

    home_days_rest INTEGER,
    away_days_rest INTEGER,
    days_rest_diff INTEGER,

    target_home_win BOOLEAN,
    target_draw BOOLEAN,
    target_away_win BOOLEAN,
    target_over_25 BOOLEAN,
    target_btts BOOLEAN,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (match_id)
);