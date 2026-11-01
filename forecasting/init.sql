CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS occupancy_readings (
    lot_id        TEXT        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    occupancy_pct FLOAT       NOT NULL,
    free          INTEGER,
    capacity      INTEGER,
    PRIMARY KEY (lot_id, ts)
);

SELECT create_hypertable('occupancy_readings', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_occ_lot ON occupancy_readings (lot_id, ts DESC);

-- Saatlik continuous aggregate. materialized_only = false sayesinde henüz
-- materyalize edilmemiş son kova da ham tablodan canlı okunur.
CREATE MATERIALIZED VIEW IF NOT EXISTS lot_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT lot_id,
       time_bucket('1 hour', ts) AS bucket,
       AVG(occupancy_pct)        AS avg_occupancy_pct,
       MAX(occupancy_pct)        AS max_occupancy_pct,
       MIN(occupancy_pct)        AS min_occupancy_pct,
       COUNT(*)                  AS reading_count
FROM occupancy_readings
GROUP BY lot_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('lot_hourly',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);

-- 30 saniyelik poll'da günde ~43k satır birikiyor; ham veri 30 günde düşer,
-- saatlik özet lot_hourly içinde kalır.
SELECT add_retention_policy('occupancy_readings', INTERVAL '30 days',
    if_not_exists => TRUE);
