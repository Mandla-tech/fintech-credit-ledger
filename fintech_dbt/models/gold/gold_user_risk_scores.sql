{{ config(
    materialized='table',
    schema='gold'
) }}

WITH user_metrics AS (
    SELECT
        user_id,

        -- Volume metrics
        COUNT(txn_id)               AS total_transactions,
        SUM(amount)                 AS total_spend,
        AVG(amount)                 AS avg_transaction_amount,
        MAX(amount)                 AS max_transaction_amount,

        -- Risk indicators
        COUNTIF(status = 'failed')  AS failed_transactions,
        COUNTIF(status = 'pending') AS pending_transactions,
        COUNTIF(status = 'success') AS successful_transactions,

        --Failure rate shown as a percentage
        ROUND(
            SAFE_DIVIDE(
                COUNTIF(status = 'failed'),
                COUNT(txn_id)
            ) * 100, 2
        )                             AS failure_rate_pct,

        --Time metric
        MIN(timestamp)                 AS first_transaction_at,
        MAX(timestamp)                 AS last_transaction_at

    FROM{{ref('silver_transactions')}}
    GROUP BY user_id
),

risk_scored AS (
    SELECT
        *,

        ROUND(
            (failure_rate_pct * 0.40) +
            (SAFE_DIVIDE(max_transaction_amount, NULLIF(avg_transaction_amount, 0)) * 0.40) +
            (SAFE_DIVIDE(pending_transactions, NULLIF(total_transactions, 0)) * 100 * 0.20) , 2

        )                               AS risk_score,

        CURRENT_TIMESTAMP()             AS dbt_loaded_at

    FROM user_metrics  
)

SELECT
    user_id,
    total_transactions,
    total_spend,
    avg_transaction_amount,
    max_transaction_amount,
    failed_transactions,
    pending_transactions,
    successful_transactions,
    failure_rate_pct,
    risk_score,

    CASE
        WHEN risk_score >= 25 THEN 'HIGH'
        WHEN risk_score >= 15 THEN 'MEDIUM'
        ELSE                        'LOW'
    END                                 AS risk_tier,

    first_transaction_at,
    last_transaction_at,
    dbt_loaded_at

FROM risk_scored
ORDER BY risk_score DESC