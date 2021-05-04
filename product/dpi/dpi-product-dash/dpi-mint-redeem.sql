-- https://duneanalytics.com/queries/35663

WITH mint_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
GROUP BY 1

),

redeem_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    -SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN ('\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
GROUP BY 1

)

SELECT
    m.day,
    m.quantity AS mint_volume,
    r.quantity AS redeem_volume,
    m.quantity + r.quantity AS net_volume,
    AVG(m.quantity + r.quantity) OVER (ORDER BY m.day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
FROM mint_volume m
JOIN redeem_volume r ON m.day = r.day