WITH dpi_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
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
    LEFT JOIN dpi_mint_burn m ON d.day = m.day
    
),

dpi AS (

SELECT 
    day,
    'DPI' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM dpi_units

),

-- ETH2x-FLI
fli_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
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
    LEFT JOIN fli_mint_burn m ON d.day = m.day
    
),

fli AS (

SELECT 
    day,
    'ETH2x-FLI' AS product,
    SUM(amount) OVER (ORDER BY day) AS fli
FROM fli_units

),

--BTC2x-FLI
btc2x_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
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
    LEFT JOIN btc2x_mint_burn m ON d.day = m.day
    
),

btc2x AS (

    SELECT 
        day, 
        'BTC2x-FLI' AS product,
        SUM(amount) OVER (ORDER BY day) AS btc2x
    FROM btc2x_units

),

mvi_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day, 
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed" 
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
    LEFT JOIN mvi_mint_burn m ON d.day = m.day
    
),

mvi AS (

SELECT 
    day, 
    'MVI' AS product,
    SUM(amount) OVER (ORDER BY day) AS mvi
FROM mvi_units

),

supply AS (

SELECT DISTINCT * FROM dpi

UNION ALL

SELECT DISTINCT * FROM fli

UNION ALL

SELECT DISTINCT * FROM btc2x

UNION ALL

SELECT DISTINCT * FROM mvi

),


daily_unit_supply as (
SELECT
    day,
    SUM(units) AS supply
FROM supply
GROUP BY 1
ORDER BY 1

),

lagged_daily_unit_supply as (

SELECT day,
       supply,
       lag(supply, 7) over (order by day) as supply_last_week,
       lag(supply, 30) over (order by day) as supply_last_month
FROM daily_unit_supply

),

daily_unit_supply_wow as (

SELECT day,
       supply,
       supply_last_week,
       ((supply - supply_last_week) / supply_last_week)::numeric as week_over_week_change, 
       ((supply - supply_last_month) / supply_last_month)::numeric as month_over_month_change 
FROM lagged_daily_unit_supply

),

daily_unit_supply_wow_rolling as (

SELECT day,
       supply,
       supply_last_week,
       week_over_week_change,
       month_over_month_change,
       avg(week_over_week_change) over 
       (order by day rows between 7 preceding and current row) as week_over_week_change_7d_ma,
       avg(month_over_month_change) over 
       (order by day rows between 7 preceding and current row) as month_over_month_change_7d_ma
FROM daily_unit_supply_wow

)

select day, 
       round(week_over_week_change, 4) as "WoW %",
       round(month_over_month_change, 4) as "MoM %"
from daily_unit_supply_wow_rolling
where week_over_week_change is not null 
and month_over_month_change is not null
and day >= '2020-12-01'::date

