-- https://duneanalytics.com/queries/32154/64794

WITH dpi AS (

SELECT
    'DPI' AS product,
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
    WHERE  (token_a_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
        or token_b_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b')
        -- and block_time  > now() - interval '3 months'
) t
GROUP BY 1,2

),

-- ETH2x-FLI
fli AS (

SELECT
    'ETH2x-FLI' AS product,
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
    WHERE  (token_a_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        or token_b_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd')
        -- and block_time  > now() - interval '3 months'
) t
GROUP BY 1,2

),

-- BTC2x-FLI
btc2x AS (

SELECT
    'BTC2x-FLI' AS product,
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
    WHERE  (token_a_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        or token_b_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b')
        -- and block_time  > now() - interval '3 months'
) t
WHERE date_trunc('day', block_time) >= '2021-05-11'
GROUP BY 1,2

),

mvi AS (

SELECT
    'MVI' AS product,
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
    WHERE  (token_a_address = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
        or token_b_address = '\x72e364f2abdc788b7e918bc238b21f109cd634d7')
        -- and block_time  > now() - interval '3 months'
) t
GROUP BY 1,2

),

bed AS (

SELECT
    'BED' AS product,
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
    WHERE  (token_a_address = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'
        or token_b_address = '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6')
        -- and block_time  > now() - interval '3 months'
) t
WHERE date_trunc('day', block_time) >= '2021-07-21'
GROUP BY 1,2

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
    SUM(usd_volume) AS volume,
    AVG(SUM(usd_volume)) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS av
FROM agg
GROUP BY 1
ORDER BY 1