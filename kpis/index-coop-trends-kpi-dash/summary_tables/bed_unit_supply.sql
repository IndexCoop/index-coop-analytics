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

)


,

daily_unit_supply_wow as (

SELECT day,
      supply,
      supply_last_week,
      supply_last_month,
      ((supply - supply_last_week) / nullif(supply_last_week, 0))::numeric as week_over_week_change, 
      ((supply - supply_last_month) / nullif(supply_last_month, 0))::numeric as month_over_month_change 
FROM lagged_daily_unit_supply

),



daily_unit_supply_wow_rolling as (

SELECT day::date,
      supply::int,
      supply_last_week::int,
      supply_last_month::int,
      week_over_week_change,
      month_over_month_change,
      avg(week_over_week_change) over 
      (order by day rows between 7 preceding and current row) as week_over_week_change_7d_ma,
      avg(month_over_month_change) over 
      (order by day rows between 7 preceding and current row) as month_over_month_change_7d_ma
FROM daily_unit_supply_wow

)

select day, 
      supply,
      supply_last_week,
      supply_last_month,
      round(week_over_week_change, 4) as "WoW %",
      round(month_over_month_change, 4) as "MoM %"
from daily_unit_supply_wow_rolling
where week_over_week_change is not null 
--and month_over_month_change is not null
order by day desc limit 7