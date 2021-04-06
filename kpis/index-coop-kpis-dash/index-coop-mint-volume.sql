-- https://duneanalytics.com/queries/25370/51980

WITH dpi_mint AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        GROUP BY 1

),

dpi_days AS (
    
    SELECT generate_series('2020-09-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

dpi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM dpi_days d
    LEFT JOIN dpi_mint m ON d.day = m.day
    
),

dpi AS (

SELECT 
    *,
    'DPI' AS product
FROM dpi_units

),

fli_mint AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        GROUP BY 1
        
),

fli_days AS (
    
    SELECT generate_series('2021-03-13'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

fli_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM fli_days d
    LEFT JOIN fli_mint m ON d.day = m.day
    
),

fli AS (

SELECT 
    *,
    'ETH2X-FLI' AS product
FROM fli_units

),

cgi_mint AS (

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
    
),

cgi AS (

SELECT 
    *,
    'CGI' AS product
FROM cgi_units

)

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM cgi
