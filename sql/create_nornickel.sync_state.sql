DROP TABLE IF EXISTS nornickel.sync_state;
CREATE TABLE nornickel.sync_state (
    ticker VARCHAR(20) PRIMARY KEY,
    last_date TIMESTAMPTZ,  -- with time zone!
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);