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

mint_volume_7d_ma as (

SELECT 
    *,
    AVG(amount) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_mint_volume
FROM dpi_units

),

lagged_avg_mint_volume as (

SELECT day,
       avg_mint_volume,
       lag(avg_mint_volume, 7) over (order by day) as avg_mint_volume_last_week,
       lag(avg_mint_volume, 30) over (order by day) as avg_mint_volume_last_month
FROM mint_volume_7d_ma

),

lagged_avg_mint_volume_wow as (

SELECT day,
       avg_mint_volume,
       avg_mint_volume_last_week,
       ((avg_mint_volume - avg_mint_volume_last_week) / avg_mint_volume_last_week)::numeric as week_over_week_change, 
       ((avg_mint_volume - avg_mint_volume_last_month) / avg_mint_volume_last_month)::numeric as month_over_month_change 
FROM lagged_avg_mint_volume

),

avg_mint_volume_wow_rolling as (

SELECT day,
       avg_mint_volume,
       avg_mint_volume_last_week,
       week_over_week_change,
       month_over_month_change,
       avg(week_over_week_change) over 
       (order by day rows between 7 preceding and current row) as week_over_week_change_7d_ma,
       avg(month_over_month_change) over 
       (order by day rows between 7 preceding and current row) as month_over_month_change_7d_ma
FROM lagged_avg_mint_volume_wow

)

select day, 
       round(week_over_week_change, 4) as "WoW %",
       round(month_over_month_change, 4) as "MoM %"
from avg_mint_volume_wow_rolling
where week_over_week_change is not null 
and month_over_month_change is not null
and day >= '2020-12-01'::date