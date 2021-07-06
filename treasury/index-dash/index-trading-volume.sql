-- Query here: https://duneanalytics.com/queries/76637

WITH index_token AS (

SELECT
    'INDEX' AS product,
    date_trunc('day', block_time) as day,
    SUM(
        CASE WHEN token_a_address = price_address
        THEN token_a_amount * price
        ELSE token_b_amount * price END
        ) AS usd_volume
FROM (
    SELECT DISTINCT ON (tx_hash, trace_address, evt_index)
        project ||
        version as project,
        token_a_address,
        token_a_amount,
        token_b_address,
        token_b_amount,
        p.contract_address AS price_address,
        price,
        block_time
    FROM dex.trades t
    INNER JOIN prices.usd p
    ON date_trunc('minute', block_time) = p.minute AND (token_a_address = p.contract_address OR token_b_address = p.contract_address)
    WHERE  (token_a_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
        or token_b_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab')
        and block_time  > now() - interval '6 months'
) t
GROUP BY 1,2

)
SELECT day
    , SUM(usd_volume) AS volume
    , AVG(SUM(usd_volume)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS "7 Day Avg"
    , AVG(SUM(usd_volume)) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS "30 Day Avg"
FROM index_token
GROUP BY 1
ORDER BY 1