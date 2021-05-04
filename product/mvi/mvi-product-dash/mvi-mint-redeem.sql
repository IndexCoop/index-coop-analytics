-- https://duneanalytics.com/queries/42181

WITH mint_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
WHERE "_setToken" IN ('\x72e364f2abdc788b7e918bc238b21f109cd634d7')
GROUP BY 1

),

redeem_volume AS (

SELECT
    date_trunc('day', evt_block_time) AS day,
    -SUM("_quantity"/1e18) as quantity
FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
WHERE "_setToken" IN ('\x72e364f2abdc788b7e918bc238b21f109cd634d7')
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