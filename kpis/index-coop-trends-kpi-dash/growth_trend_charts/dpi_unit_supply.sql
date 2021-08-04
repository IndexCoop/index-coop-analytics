WITH mint_burn AS (

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

days AS (
    
    SELECT generate_series('2020-09-10'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM days d
    LEFT JOIN mint_burn m ON d.day = m.day
    
),

daily_unit_supply as (
SELECT 
    DISTINCT
    day, 
    SUM(amount) OVER (ORDER BY day) AS supply
FROM units
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
       ((supply - supply_last_week) / nullif(supply_last_week, 0))::numeric as week_over_week_change, 
       ((supply - supply_last_month) / nullif(supply_last_month, 0))::numeric as month_over_month_change 
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

