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

agg AS (

SELECT * FROM dpi

UNION ALL

SELECT * FROM fli

UNION ALL

SELECT * FROM btc2x

UNION ALL

SELECT * FROM mvi

),

daily_trading_volume as (

SELECT
    day,
    SUM(usd_volume) AS volume
FROM agg
GROUP BY 1
ORDER BY 1

),

trading_volume_7d_ma as (

SELECT 
    *,
    AVG(volume) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_trading_volume
FROM daily_trading_volume

),

lagged_avg_trading_volume as (

SELECT day,
       avg_trading_volume,
       lag(avg_trading_volume, 7) over (order by day) as avg_trading_volume_last_week,
       lag(avg_trading_volume, 30) over (order by day) as avg_trading_volume_last_month
FROM trading_volume_7d_ma

),

lagged_avg_trading_volume_wow as (

SELECT day,
       avg_trading_volume,
       avg_trading_volume_last_week,
       avg_trading_volume_last_month,
       ((avg_trading_volume - avg_trading_volume_last_week) / avg_trading_volume_last_week)::numeric as week_over_week_change, 
       ((avg_trading_volume - avg_trading_volume_last_month) / avg_trading_volume_last_month)::numeric as month_over_month_change 
FROM lagged_avg_trading_volume

),

avg_trading_volume_wow_rolling as (

SELECT day,
       avg_trading_volume,
       avg_trading_volume_last_week,
       avg_trading_volume_last_month,
       week_over_week_change,
       avg(week_over_week_change) over 
       (order by day rows between 7 preceding and current row) as week_over_week_change_7d_ma,
       avg(month_over_month_change) over 
       (order by day rows between 7 preceding and current row) as month_over_month_change_7d_ma
FROM lagged_avg_trading_volume_wow

)

select day, 
       avg_trading_volume,
       avg_trading_volume_last_week,
       avg_trading_volume_last_month,
       round(week_over_week_change_7d_ma, 4) as "WoW %",
       round(month_over_month_change_7d_ma, 4) as "MoM %"
from avg_trading_volume_wow_rolling
where week_over_week_change_7d_ma is not null 
and month_over_month_change_7d_ma is not null
and day >= '2020-12-01'::date
order by day desc limit 7