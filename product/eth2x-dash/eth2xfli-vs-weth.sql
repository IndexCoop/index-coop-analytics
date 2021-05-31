--https://duneanalytics.com/queries/51187

SELECT hour,
    (coalesce(eth2xfli_weth_v2, 0) + coalesce(eth2xfli_weth_v3, 0)) / (coalesce(v2_count, 0) + coalesce(v3_count, 0)) AS eth2xfli_weth,
    eth_usd
FROM (
    SELECT date_trunc('hour', minute) AS hour,
        1 / avg(("amount0In" + "amount0Out") / ("amount1In" + "amount1Out")) * avg(weth_usd) AS eth2xfli_weth_v2,
        CASE WHEN 1 / avg(("amount0In" + "amount0Out") / ("amount1In" + "amount1Out")) * avg(weth_usd) IS NOT NULL THEN 1 END AS v2_count,
        1 / -avg(amount0 / amount1) * avg(weth_usd) AS eth2xfli_weth_v3,
        CASE WHEN 1 / -avg(amount0 / amount1) * avg(weth_usd) IS NOT NULL THEN 1 END AS v3_count,
        avg(weth_usd) AS weth_usd,
        avg(eth_usd) AS eth_usd
    FROM (
        SELECT minute,
            price AS weth_usd
        FROM prices.usd
        WHERE symbol = 'WETH' AND
        minute > '2021-03-16 13:00' -- removing weird spike in first 48 hours
    ) AS px
    JOIN (
        SELECT minute,
            price AS eth_usd
        FROM prices.layer1_usd
        WHERE symbol = 'ETH'
    ) AS px_l1 USING (minute)
    JOIN (
        SELECT date_trunc('minute', evt_block_time) AS minute,
            "amount0In",
            "amount1In",
            "amount0Out",
            "amount1Out"
        FROM uniswap_v2."Pair_evt_Swap"
        WHERE contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' AND
            ("amount1In" + "amount1Out") > 0 -- cannot divide by 0
    ) AS v2 USING (minute)
    LEFT JOIN (
        SELECT date_trunc('minute', evt_block_time) AS minute,
            amount0,
            amount1
        FROM uniswap_v3."Pair_evt_Swap"
        WHERE contract_address = '\x151ccb92bc1ed5c6d0f9adb5cec4763ceb66ac7f'
    ) AS v3 USING (minute)
    GROUP BY 1
) AS agg
