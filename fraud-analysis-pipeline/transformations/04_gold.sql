-- =============================================================
-- 04_gold.sql  –  Gold: fraud rate by party size & payment method
-- Answers: "Is fraud risk related to party size and payment method?"
-- =============================================================

CREATE OR REFRESH MATERIALIZED VIEW fraud_by_party_and_method
COMMENT 'Fraud rate aggregated by guest-count bucket and payment method'
AS
SELECT
    CASE
        WHEN b.guests_count = 1                   THEN '1 - solo'
        WHEN b.guests_count = 2                   THEN '2 - couple'
        WHEN b.guests_count BETWEEN 3 AND 4       THEN '3-4 - small group'
        WHEN b.guests_count BETWEEN 5 AND 8       THEN '5-8 - large group'
        ELSE                                           '9+ - very large'
    END                                                  AS party_size_bucket,
    p.payment_method,
    COUNT(DISTINCT b.booking_id)                         AS total_bookings,
    SUM(CAST(b.is_fraud AS INT))                         AS fraud_bookings,
    ROUND(
        100.0 * SUM(CAST(b.is_fraud AS INT))
        / NULLIF(COUNT(DISTINCT b.booking_id), 0), 2
    )                                                    AS fraud_rate_pct,
    ROUND(AVG(CASE WHEN b.is_fraud
                   THEN CAST(p.amount AS DOUBLE) END), 2) AS avg_fraud_amount,
    ROUND(AVG(CAST(p.amount AS DOUBLE)), 2)              AS avg_booking_amount
FROM      bookings_with_fraud  b
INNER JOIN payments_raw        p  ON b.booking_id = p.booking_id
GROUP BY  party_size_bucket, p.payment_method
ORDER BY  fraud_rate_pct DESC;
