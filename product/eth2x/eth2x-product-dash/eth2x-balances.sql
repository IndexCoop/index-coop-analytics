-- https://duneanalytics.com/queries/51832

WITH balances AS (
SELECT date_trunc('hour', tb.timestamp) AS hour,
    sum(CASE WHEN token_address = '\x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' THEN amount_raw / 1e8 END) AS ceth,
    sum(CASE WHEN token_address = '\xc00e94cb662c3520282e6f5717214004a7f26888' THEN amount_raw / 1e18 END) AS comp
FROM erc20.token_balances tb -- TODO: this table is not suitable as a base since it is not stricly hourly
WHERE wallet_address = '\xAa6E8127831c9DE45ae56bB1b0d4D4Da6e5665BD'
GROUP BY 1
ORDER BY 1
),
borrow_balance AS (
    SELECT date_trunc('hour', evt_block_time) AS hour,
        avg(usdc) AS usdc -- TODO: this is not the proper way to do this, should be latest value of the hour
    FROM (
        SELECT evt_block_time, "accountBorrows" / 1e6 AS usdc
        FROM compound_v2."cErc20_evt_RepayBorrow"
        WHERE borrower = '\xAa6E8127831c9DE45ae56bB1b0d4D4Da6e5665BD'
        UNION ALL
        SELECT evt_block_time, "accountBorrows" / 1e6 AS usdc
        FROM compound_v2."cErc20_evt_Borrow"
        WHERE borrower = '\xAa6E8127831c9DE45ae56bB1b0d4D4Da6e5665BD'
    ) alias0
    GROUP BY 1
),
px_dex AS (
    SELECT *
    FROM (
        SELECT date_trunc('hour', evt_block_time) AS hour,
            avg(price / ((e."mintTokens" / 1e8) / (e."mintAmount" / 1e18))) AS ceth_usd
        FROM compound_v2."cEther_evt_Mint" e
        JOIN prices.layer1_usd_eth p ON date_trunc('hour', p.minute) = date_trunc('hour', e.evt_block_time)
        WHERE e."mintAmount" > 10000000000000
        GROUP BY 1
    ) alias0
    RIGHT JOIN (
        SELECT hour::timestamptz
        FROM generate_series('2021-03-18', now(), '1 hour') AS hour -- dropping first couple of days due to outliers
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
SELECT *,
    ceth * ceth_usd AS ceth_collateral_usd,
    usdc * usdc_usd AS usdc_borrow_usd,
    (ceth * ceth_usd) / (usdc * usdc_usd) AS leverage_ratio,
    2 AS leverage_par
FROM (
    SELECT balances.hour,
        coalesce(ceth, (
            SELECT ceth FROM balances AS balances_ WHERE balances_.hour < balances.hour AND ceth IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS ceth,
        coalesce(comp, (
            SELECT comp FROM balances AS balances_ WHERE balances_.hour < balances.hour AND comp IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS comp,
        coalesce(usdc, (
            SELECT usdc FROM borrow_balance AS borrow_balance_ WHERE borrow_balance_.hour < borrow_balance.hour AND usdc IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS usdc,
        coalesce(ceth_usd, (
            SELECT ceth_usd FROM px_dex AS px_dex_ WHERE px_dex_.hour < px_dex.hour AND ceth_usd IS NOT NULL ORDER BY hour DESC LIMIT 1
        )) AS ceth_usd,
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
) alias0

-- NOTES

-- uint256 collateralPrice;     // Price of underlying in precise units (10e18)
-- uint256 borrowPrice;         // Price of underlying in precise units (10e18)
-- uint256 collateralBalance;   // Balance of underlying held in Compound in base units (e.g. USDC 10e6)
-- uint256 borrowBalance;       // Balance of underlying borrowed from Compound in base units
-- uint256 collateralValue;     // Valuation in USD adjusted for decimals in precise units (10e18)
-- uint256 borrowValue;         // Valuation in USD adjusted for decimals in precise units (10e18)
-- collateralValue = rebalanceInfo.collateralPrice.preciseMul(rebalanceInfo.collateralBalance)
-- borrowValue = rebalanceInfo.borrowPrice.preciseMul(rebalanceInfo.borrowBalance)
-- _calculateCurrentLeverageRatio = _collateralValue.preciseDiv(_collateralValue.sub(_borrowValue))

-- alternative way to get borrowBalanceStored
-- however, this might not always be up to date
-- (but a good sanity check nonetheless)
-- SELECT call_block_time,
--     output_0 / 1e6 AS usdc,
--     contract_address
-- FROM compound_v2."cErc20_call_borrowBalanceStored"
-- WHERE account = '\xAa6E8127831c9DE45ae56bB1b0d4D4Da6e5665BD' AND
--     call_success
