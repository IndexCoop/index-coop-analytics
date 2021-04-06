-- https://duneanalytics.com/queries/25369/52769

WITH cgi_mint AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\xada0a1202462085999652dc5310a7a9e2bf3ed42'
        GROUP BY 1

),

cgi_days AS (
    
    SELECT generate_series('2021-02-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

cgi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM cgi_days d
    LEFT JOIN cgi_mint m ON d.day = m.day
    
)

SELECT 
    *,
    AVG(amount) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
FROM cgi_units
