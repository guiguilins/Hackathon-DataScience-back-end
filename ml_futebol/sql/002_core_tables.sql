CREATE TABLE core.competitions (
    competition_id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(50),
    source_competition_id VARCHAR(100),
    competition_name VARCHAR(150) NOT NULL,
    country_name VARCHAR(100),
    gender VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_name, source_competition_id)
);

CREATE TABLE core.seasons (
    season_id BIGSERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES core.competitions(competition_id),
    source_name VARCHAR(50),
    source_season_id VARCHAR(100),
    season_name VARCHAR(100) NOT NULL,
    season_start_date DATE,
    season_end_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (competition_id, season_name)
);

CREATE TABLE core.teams (
    team_id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(50),
    source_team_id VARCHAR(100),
    team_name VARCHAR(150) NOT NULL,
    normalized_team_name VARCHAR(150) NOT NULL,
    country_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (normalized_team_name)
);

CREATE TABLE core.coaches (
    coach_id BIGSERIAL PRIMARY KEY,
    coach_name VARCHAR(150) NOT NULL,
    normalized_coach_name VARCHAR(150) NOT NULL,
    birth_date DATE,
    nationality VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (normalized_coach_name)
);

CREATE TABLE core.players (
    player_id BIGSERIAL PRIMARY KEY,
    player_name VARCHAR(150) NOT NULL,
    normalized_player_name VARCHAR(150) NOT NULL,
    birth_date DATE,
    age INTEGER,
    nationality VARCHAR(100),
    preferred_position VARCHAR(50),
    foot VARCHAR(20),
    height_cm NUMERIC(5,2),
    weight_kg NUMERIC(5,2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_players_normalized_name
ON core.players(normalized_player_name);


CREATE TABLE core.matches (
    match_id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL,
    source_match_id VARCHAR(100) NOT NULL,
    competition_id BIGINT NOT NULL REFERENCES core.competitions(competition_id),
    season_id BIGINT NOT NULL REFERENCES core.seasons(season_id),
    match_date DATE NOT NULL,
    match_datetime TIMESTAMP,
    round_name VARCHAR(100),
    stadium_name VARCHAR(150),
    referee_name VARCHAR(150),

    home_team_id BIGINT NOT NULL REFERENCES core.teams(team_id),
    away_team_id BIGINT NOT NULL REFERENCES core.teams(team_id),

    home_score INTEGER,
    away_score INTEGER,

    home_coach_id BIGINT REFERENCES core.coaches(coach_id),
    away_coach_id BIGINT REFERENCES core.coaches(coach_id),

    status VARCHAR(30) DEFAULT 'played',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (source_name, source_match_id)
);

CREATE INDEX idx_matches_season_date
ON core.matches(season_id, match_date);

CREATE INDEX idx_matches_home_team
ON core.matches(home_team_id);

CREATE INDEX idx_matches_away_team
ON core.matches(away_team_id);

CREATE TABLE core.lineups (
    lineup_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL REFERENCES core.matches(match_id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES core.teams(team_id),
    player_id BIGINT NOT NULL REFERENCES core.players(player_id),

    position_name VARCHAR(50),
    starter BOOLEAN NOT NULL DEFAULT FALSE,
    jersey_number INTEGER,
    minutes_played INTEGER,
    is_captain BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (match_id, team_id, player_id)
);

CREATE INDEX idx_lineups_match_team
ON core.lineups(match_id, team_id);

CREATE INDEX idx_lineups_player
ON core.lineups(player_id);

CREATE TABLE core.match_team_stats (
    match_team_stats_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL REFERENCES core.matches(match_id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES core.teams(team_id),
    opponent_team_id BIGINT NOT NULL REFERENCES core.teams(team_id),

    is_home BOOLEAN NOT NULL,

    goals_scored INTEGER DEFAULT 0,
    goals_conceded INTEGER DEFAULT 0,

    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    xg NUMERIC(10,4) DEFAULT 0,
    possession_pct NUMERIC(5,2),
    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    progressive_passes INTEGER DEFAULT 0,
    progressive_carries INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    corners INTEGER DEFAULT 0,
    fouls_committed INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    offsides INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    aerial_duels_won INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (match_id, team_id)
);

CREATE INDEX idx_match_team_stats_match
ON core.match_team_stats(match_id);

CREATE INDEX idx_match_team_stats_team
ON core.match_team_stats(team_id);

CREATE TABLE core.match_player_stats (
    match_player_stats_id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL REFERENCES core.matches(match_id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES core.teams(team_id),
    player_id BIGINT NOT NULL REFERENCES core.players(player_id),

    minutes_played INTEGER DEFAULT 0,
    started BOOLEAN DEFAULT FALSE,

    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    xg NUMERIC(10,4) DEFAULT 0,
    xa NUMERIC(10,4) DEFAULT 0,

    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    progressive_passes INTEGER DEFAULT 0,
    progressive_carries INTEGER DEFAULT 0,
    dribbles_completed INTEGER DEFAULT 0,

    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    aerial_duels_won INTEGER DEFAULT 0,

    fouls_committed INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (match_id, team_id, player_id)
);

CREATE INDEX idx_match_player_stats_match
ON core.match_player_stats(match_id);

CREATE INDEX idx_match_player_stats_player
ON core.match_player_stats(player_id);


CREATE TABLE core.player_season_stats (
    player_season_stats_id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES core.players(player_id),
    team_id BIGINT NOT NULL REFERENCES core.teams(team_id),
    competition_id BIGINT REFERENCES core.competitions(competition_id),
    season_id BIGINT NOT NULL REFERENCES core.seasons(season_id),

    matches_played INTEGER DEFAULT 0,
    starts INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,

    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    xg NUMERIC(10,4) DEFAULT 0,
    xa NUMERIC(10,4) DEFAULT 0,

    progressive_passes INTEGER DEFAULT 0,
    progressive_carries INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,

    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    aerial_duels_won INTEGER DEFAULT 0,

    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (player_id, team_id, season_id)
);

CREATE INDEX idx_player_season_stats_player
ON core.player_season_stats(player_id);

CREATE INDEX idx_player_season_stats_team_season
ON core.player_season_stats(team_id, season_id);


CREATE TABLE map.player_source_mapping (
    player_source_mapping_id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES core.players(player_id),

    statsbomb_player_id VARCHAR(100),
    statsbomb_player_name VARCHAR(150),
    fbref_player_id VARCHAR(100),
    fbref_player_name VARCHAR(150),

    statsbomb_team_name VARCHAR(150),
    fbref_team_name VARCHAR(150),

    season_name VARCHAR(100),
    confidence_score NUMERIC(5,2),
    mapping_status VARCHAR(30) NOT NULL DEFAULT 'pending',
    notes TEXT,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_player_source_mapping_player
ON map.player_source_mapping(player_id);


CREATE TABLE map.team_source_mapping (
    team_source_mapping_id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL REFERENCES core.teams(team_id),

    statsbomb_team_id VARCHAR(100),
    statsbomb_team_name VARCHAR(150),
    fbref_team_id VARCHAR(100),
    fbref_team_name VARCHAR(150),

    competition_name VARCHAR(150),
    season_name VARCHAR(100),
    confidence_score NUMERIC(5,2),
    mapping_status VARCHAR(30) NOT NULL DEFAULT 'pending',

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_team_source_mapping_team
ON map.team_source_mapping(team_id);


