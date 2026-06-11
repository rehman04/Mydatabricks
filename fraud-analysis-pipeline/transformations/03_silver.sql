-- =============================================================
-- 03_silver.sql  –  Silver: bookings enriched with fraud flags
-- =============================================================

CREATE OR REFRESH MATERIALIZED VIEW bookings_with_fraud
COMMENT 'Latest booking state left-joined with fraud flags; is_fraud=TRUE when a flag exists'
AS
SELECT
    b.booking_id,
    b.property_id,
    b.user_id,
    b.guests_count,
    b.status,
    b.total_amount,
    b.check_in,
    b.check_out,
    b.created_at,
    b.updated_at,
    -- fraud enrichment
    CASE WHEN f.flag IS NOT NULL THEN TRUE ELSE FALSE END AS is_fraud,
    f.confidence  AS fraud_confidence,
    f.reason      AS fraud_reason,
    f.flagged_at  AS fraud_flagged_at
FROM      bookings_latest       b
LEFT JOIN booking_fraud_flags   f  ON b.booking_id = f.booking_id;
