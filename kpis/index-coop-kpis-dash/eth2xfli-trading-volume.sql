-- https://duneanalytics.com/queries/25373/51986

SELECT
    'ETH2x-FLI' AS product,
    date_trunc('day', block_time) as day,
    SUM(
        CASE WHEN token_a_address = price_address
        THEN token_a_amount * price
        ELSE token_b_amount * price END
        ) AS usd_volume,
    AVG(SUM(
        CASE WHEN token_a_address = price_address
        THEN token_a_amount * price
        ELSE token_b_amount * price END
        )) OVER (ORDER BY date_trunc('day', block_time)ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
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
    WHERE  (token_a_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        or token_b_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd')
        -- and block_time  > now() - interval '3 months'
) t
GROUP BY 1,2