-- =============================================================
-- 02_cdc.sql  –  Apply CDC to materialise latest booking state
-- =============================================================

-- Declare the CDC target table first
CREATE OR REFRESH STREAMING TABLE bookings_latest
COMMENT 'Latest state of every booking, kept current via SCD Type-1 CDC merge';

-- Merge incoming update events into bookings_latest
APPLY CHANGES INTO bookings_latest
FROM   STREAM(booking_updates_raw)
KEYS   (booking_id)
SEQUENCE BY booking_update_id
STORED AS SCD TYPE 1;
