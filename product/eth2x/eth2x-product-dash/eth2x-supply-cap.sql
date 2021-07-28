-- https://duneanalytics.com/queries/81380

WITH supply_caps AS (

SELECT 
    evt_block_time AS dt,
    "_newCap" / 1e18 AS supply_cap
FROM setprotocol_v2."SupplyCapIssuanceHook_evt_SupplyCapUpdated"

UNION ALL

SELECT
    '2021-03-14 00:00'::timestamp AS dt,
    50000 AS supply_cap
    
),

eth2x_days AS (
    
    SELECT generate_series('2021-03-14'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
)

SELECT
    day,
    FIRST_VALUE(supply_cap) OVER (PARTITION BY tmp) AS supply_cap
FROM (
SELECT 
    d.day,
    s.supply_cap,
    SUM(CASE WHEN supply_cap IS NOT NULL THEN 1 END) OVER (ORDER BY day) AS tmp
FROM eth2x_days d
LEFT JOIN supply_caps s ON date_trunc('day', s.dt) = d.day
) t