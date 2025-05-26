CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0)
);