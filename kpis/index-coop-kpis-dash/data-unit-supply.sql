-- https://dune.xyz/queries/163030

WITH mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'
    GROUP BY 1
),

days AS (
    
    SELECT generate_series('2021-09-21'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
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
    SUM(amount) OVER (ORDER BY day) AS data
FROM units
WHERE day >= '9-23-2021'
ORDER BY 1