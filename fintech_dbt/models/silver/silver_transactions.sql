{{ config(
    materialized='table',
    schema='silver'
) }}

WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY txn_id
            ORDER BY timestamp DESC
        ) AS row_num
    FROM {{ source('bronze', 'raw_transactions') }}
),

cleaned AS (
    SELECT
        txn_id,
        user_id,
        currency,
        COALESCE(amount, 0.00)      AS amount,     -- Replacing NULL amounts with 0
        CAST(timestamp AS TIMESTAMP) AS timestamp,  -- Converting timestamp into a proper TIMESTAMP datatype
        status,
        CURRENT_TIMESTAMP()          AS dbt_loaded_at
    FROM deduplicated
    WHERE row_num = 1                          -- removing duplicates
      AND currency != 'INVALID'                -- removing junk currencies
      AND amount >= 0                          -- removing negative refund errors
)

SELECT * FROM cleaned