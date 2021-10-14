-- https://duneanalytics.com/queries/105743

WITH balances AS (
SELECT date_trunc('hour', tb.timestamp) AS hour,
    sum(CASE WHEN token_address = '\xccf4429db6322d5c611ee964527d42e5d685dd6a' THEN amount_raw / 1e8 END) AS cwbtc2,
    sum(CASE WHEN token_address = '\xc00e94cb662c3520282e6f5717214004a7f26888' THEN amount_raw / 1e18 END) AS comp
FROM erc20.token_balances tb -- TODO: this table is not suitable as a base since it is not stricly hourly
WHERE wallet_address = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
GROUP BY 1
ORDER BY 1
),
borrow_balance AS (
    SELECT date_trunc('hour', evt_block_time) AS hour,
        avg(usdc) AS usdc -- TODO: this is not the proper way to do this, should be latest value of the hour
    FROM (
        SELECT evt_block_time, "accountBorrows" / 1e6 AS usdc
        FROM compound_v2."cErc20_evt_RepayBorrow"
        WHERE borrower = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        UNION ALL
        SELECT evt_block_time, "accountBorrows" / 1e6 AS usdc
        FROM compound_v2."cErc20_evt_Borrow"
        WHERE borrower = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
    ) alias0
    GROUP BY 1
),
px_dex AS (
    SELECT *
    FROM (
        SELECT
            date_trunc('hour', evt_block_time) AS hour,
            avg(price / ((e."mintTokens" / 1e8) / (e."mintAmount" / 1e8))) AS cwbtc2_usd
        FROM compound_v2."CErc20Delegator_evt_Mint" e
        JOIN prices.usd p
            ON date_trunc('hour', p.minute) = date_trunc('hour', e.evt_block_time)
            AND p.contract_address = '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHERE e."mintAmount" > 10
            AND e.contract_address = '\xccf4429db6322d5c611ee964527d42e5d685dd6a'
        GROUP BY 1
    ) alias0
    RIGHT JOIN (
        SELECT hour::timestamptz
        FROM generate_series('2021-03-19', now(), '1 hour') AS hour
    ) alias1 USING (hour)
),
px_cex AS (
    SELECT date_trunc('hour', px.minute) AS hour,
        avg(CASE WHEN px.contract_address = '\xc00e94cb662c3520282e6f5717214004a7f26888' THEN price END) AS comp_usd,
        avg(CASE WHEN px.contract_address = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN price END) AS usdc_usd
    FROM prices.usd px
    WHERE px.contract_address = '\xc00e94cb662c3520282e6f5717214004a7f26888' OR
        px.contract_address = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    GROUP BY 1
)

SELECT
    hour,
    cwbtc2 * cwbtc2_usd AS ceth_collateral_usd,
    usdc * usdc_usd AS usdc_borrow_usd,
    (cwbtc2 * cwbtc2_usd) / (usdc * usdc_usd) AS leverage_ratio,
    2 AS leverage_par
FROM (
    SELECT
        balances.hour,
        coalesce(cwbtc2, (
            SELECT cwbtc2 FROM balances AS balances_ WHERE balances_.hour < balances.hour AND cwbtc2 IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS cwbtc2,
        coalesce(comp, (
            SELECT comp FROM balances AS balances_ WHERE balances_.hour < balances.hour AND comp IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS comp,
        coalesce(usdc, (
            SELECT usdc FROM borrow_balance AS borrow_balance_ WHERE borrow_balance_.hour < borrow_balance.hour AND usdc IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS usdc,
        coalesce(cwbtc2_usd, (
            SELECT cwbtc2_usd FROM px_dex AS px_dex_ WHERE px_dex_.hour < px_dex.hour AND cwbtc2_usd IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS cwbtc2_usd,
        coalesce(comp_usd, (
            SELECT comp_usd FROM px_cex AS px_cex_ WHERE px_cex_.hour < px_cex.hour AND comp_usd IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS comp_usd,
        coalesce(usdc_usd, (
            SELECT usdc_usd FROM px_cex AS px_cex_ WHERE px_cex_.hour < px_cex.hour AND usdc_usd IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS usdc_usd
    FROM balances
    LEFT JOIN px_dex USING (hour)
    LEFT JOIN px_cex USING (hour)
    LEFT JOIN borrow_balance USING (hour)
    ORDER BY 1 DESC
) final
