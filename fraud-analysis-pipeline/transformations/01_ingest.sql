-- =============================================================
-- 01_ingest.sql  –  Bronze streaming tables
-- Catalog: de_workshop  |  Schema: labuser15231327_1781170922
-- =============================================================

-- Bronze 1: raw CDC events from the wanderbricks booking system
CREATE OR REFRESH STREAMING TABLE booking_updates_raw
COMMENT 'Raw CDC booking-update events streamed from samples.wanderbricks.booking_updates'
AS
SELECT * FROM STREAM(samples.wanderbricks.booking_updates);

-- Bronze 2: raw payment events
CREATE OR REFRESH STREAMING TABLE payments_raw
COMMENT 'Raw payment events streamed from samples.wanderbricks.payments'
AS
SELECT * FROM STREAM(samples.wanderbricks.payments);

-- Bronze 3: fraud flag files arriving in the landing-zone volume
CREATE OR REFRESH STREAMING TABLE booking_fraud_flags
COMMENT 'Fraud-flag events loaded incrementally from landing-zone JSON files'
AS
SELECT
    CAST(booking_id  AS BIGINT)    AS booking_id,
    flag,
    CAST(confidence  AS DOUBLE)    AS confidence,
    CAST(flagged_at  AS TIMESTAMP) AS flagged_at,
    reason,
    _metadata.file_name            AS source_file
FROM STREAM read_files(
    '/Volumes/ops_data/shared/landing/booking_fraud_flags/',
    format => 'json',
    schema  => 'booking_id BIGINT, flag STRING, confidence DOUBLE, flagged_at STRING, reason STRING'
);
