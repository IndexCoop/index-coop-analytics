-- https://duneanalytics.com/queries/89088

WITH bed_mint AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
        GROUP BY 1

),

bed_days AS (
    
    SELECT generate_series('2021-07-21'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

bed_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM bed_days d
    LEFT JOIN bed_mint m ON d.day = m.day
    
)

SELECT 
    *,
    AVG(amount) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
FROM bed_units