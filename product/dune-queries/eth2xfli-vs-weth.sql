--https://duneanalytics.com/queries/51187

SELECT date_trunc('hour', minute) AS hour,
    1 / avg(("amount0In" + "amount0Out") / ("amount1In" + "amount1Out")) * avg(weth_usd) AS eth2xfli_eth_v2,
    1 / -avg(amount0 / amount1) * avg(weth_usd) AS eth2xfli_eth_v3,
    avg(weth_usd) AS weth_usd
FROM (
    SELECT minute,
        price AS weth_usd
    FROM prices.usd
    WHERE symbol = 'WETH' AND
    minute > '2021-03-17' -- removing first 48 hours due to weird price spike
) AS px
LEFT JOIN (
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
