--https://dune.xyz/queries/105805

SELECT
    date_trunc('hour', minute) AS hour,
    avg(wbtc_usd) / avg((("amount0In" + "amount0Out") / ("amount1In" + "amount1Out")) / 1e10) AS btc2xfli_wbtc,
    avg(btc_usd) AS btc_usd
FROM (
    SELECT minute,
        price AS wbtc_usd
    FROM prices.usd
    WHERE symbol = 'WBTC'
        AND minute > '2021-05-13 00:00' -- removing weird spike in first 48 hours
) AS px
JOIN (
    SELECT minute,
        price AS btc_usd
    FROM prices.layer1_usd
    WHERE symbol = 'BTC'
) AS px_l1 USING (minute)
JOIN (
    SELECT date_trunc('minute', evt_block_time) AS minute,
        "amount0In",
        "amount1In",
        "amount0Out",
        "amount1Out"
    FROM sushi."Pair_evt_Swap"
    WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' AND
        ("amount1In" + "amount1Out") > 0 -- cannot divide by 0
) AS dex USING (minute)
GROUP BY 1
