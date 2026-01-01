CREATE TABLE IF NOT EXISTS makes(
    Make_ID BIGINT PRIMARY KEY,
    Make_Name TEXT,
    Load_Timestamp TIMESTAMP NOT NULL
)

CREATE INDEX idx makes_make_id ON makes(Make_ID)

CREATE TABLE IF NOT EXISTS models(
    Model_ID BIGINT PRIMARY KEY,
    Model_Name TEXT,
    Make_ID BIGINT,
    Make_Name TEXT,
    Load_Timestamp TIMESTAMP NOT NULL
)

CREATE INDEX idx models_model_id ON models(Model_ID)

CREATE INDEX idx models_model_id_make_id ON models(Model_ID, Make_ID)