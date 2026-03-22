CREATE TABLE raw.statsbomb_matches (
    raw_match_id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    payload JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_raw_statsbomb_matches_payload
ON raw.statsbomb_matches USING GIN(payload);

CREATE TABLE raw.statsbomb_events (
    raw_event_id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    source_match_id VARCHAR(100),
    payload JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_raw_statsbomb_events_payload
ON raw.statsbomb_events USING GIN(payload);

CREATE TABLE raw.statsbomb_lineups (
    raw_lineup_id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    source_match_id VARCHAR(100),
    payload JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_raw_statsbomb_lineups_payload
ON raw.statsbomb_lineups USING GIN(payload);

CREATE TABLE raw.fbref_players (
    raw_fbref_player_id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    payload JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_raw_fbref_players_payload
ON raw.fbref_players USING GIN(payload);

