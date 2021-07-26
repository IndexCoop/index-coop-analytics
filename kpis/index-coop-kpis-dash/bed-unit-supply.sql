-- https://duneanalytics.com/queries/89078

WITH mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
    GROUP BY 1
),

days AS (
    
    SELECT generate_series('2021-07-13'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM days d
    LEFT JOIN mint_burn m ON d.day = m.day
    
)

SELECT 
    DISTINCT
    day, 
    SUM(amount) OVER (ORDER BY day) AS bed
FROM units
WHERE day >= '07-21-2021'
ORDER BY 1