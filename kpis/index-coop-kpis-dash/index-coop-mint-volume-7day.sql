-- https://duneanalytics.com/queries/32152/64785

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

--ETH2x-FLI
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
    'ETH2x-FLI' AS product
FROM fli_units

),

-- BTC2x-FLI
btc2x_mint AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        GROUP BY 1
        
),

btc2x_days AS (
    
    SELECT generate_series('2021-05-11'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

btc2x_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM btc2x_days d
    LEFT JOIN btc2x_mint m ON d.day = m.day
    
),

btc2x AS (

    SELECT 
        *,
        'BTC2x-FLI' AS product
    FROM btc2x_units

),

mvi_mint AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        GROUP BY 1

),

mvi_days AS (
    
    SELECT generate_series('2021-04-06'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

mvi_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM mvi_days d
    LEFT JOIN mvi_mint m ON d.day = m.day
    
),

mvi AS (

SELECT 
    *,
    'MVI' AS product
FROM mvi_units

),

bed_mint AS (

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
    
),

bed AS (

    SELECT
        *,
        'BED' AS product
    FROM bed_units

),

agg AS (

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM btc2x

UNION ALL

SELECT * FROM mvi

UNION ALL

SELECT * FROM bed

)

SELECT
    day,
    SUM(amount) AS amount,
    AVG(SUM(amount)) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
FROM agg
GROUP BY 1
ORDER BY 1
