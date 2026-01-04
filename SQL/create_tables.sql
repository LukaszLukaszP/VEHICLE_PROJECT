CREATE TABLE IF NOT EXISTS makes (
    make_id BIGINT PRIMARY KEY,
    make_name TEXT,
    load_timestamp TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_makes_make_id
    ON makes (make_id);

CREATE TABLE IF NOT EXISTS models (
    model_id BIGINT PRIMARY KEY,
    model_name TEXT,
    make_id BIGINT,
    make_name TEXT,
    load_timestamp TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_models_model_id
    ON models (model_id);
