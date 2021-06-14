/*
    query here: https://duneanalytics.com/queries/54062

    --- INDEX Treasury ---

    Wallet / Address
    ('\x9467cfadc9de245010df95ec6a585a506a8ad5fc', -- Treasury Wallet
    '\xe2250424378b6a6dC912f5714cfd308a8D593986', -- Treasury Committee Wallet
    '\x26e316f5b3819264DF013Ccf47989Fb8C891b088' -- Community Treasury Year 1 Vesting
    )    
    
    INDEX from 
    Growth Working Group: 0xd4bcc2b5d21fe67c8be351cdb47ec1b2cd7e84a7
    Analytics Working Group: 0xe83de75eb3e84f3cbca3576351d81dbeda5645d4
    Centralised Exchange Listing: 0x154c154c589b4aeccbf186fb8bc668cd7c213762 (DPI, USDT & INDEX )

*/

-- Start Generalized Price Feed block - see generalized_price_feed.sql
WITH prices_usd AS (

    SELECT
        date_trunc('day', minute) AS dt
        , symbol
        , AVG(price) AS price
    FROM prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI')
    GROUP BY 1,2
)
    
, swaps AS (
    -- Uniswap price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , case 
            when contract_address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' then 'INDEX'
            when contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' then 'DPI'
            when contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' then 'MVI'
            when contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' then 'ETH2x-FLI'
        end as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address in ( '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' -- liq pair addresses I am searching the price for
                                , '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
                                , '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' 
                                , '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' )
        AND sw.evt_block_time >= '2020-09-10'

    union all
    
    -- Sushi price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , 'BTC2x-FLI' as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e8 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-05-11'

)
, swap_a1_prcs AS (
    SELECT 
        avg(price) a1_prc
        , date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2               
)
, uni_hours AS (
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
)
, uni_temp AS (
    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
        -- a1_prcs."minute" AS minute
    FROM uni_hours h
    LEFT JOIN swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2
) 
, swap_feed AS (
    SELECT
        hour
        , symbol
        , (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(usd_price) OVER (PARTITION BY symbol ORDER BY hour)] AS usd_price
        , (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(eth_price) OVER (PARTITION BY symbol ORDER BY hour)] AS eth_price
    FROM uni_temp
)
, swap_price_feed AS ( -- only include the uni feed when there's no corresponding price in prices_usd

    SELECT
        date_trunc('day', hour) AS dt
        , u.symbol
        , AVG(usd_price) AS price
    FROM swap_feed u
    left join prices_usd p on date_trunc('day', u.hour) = p.dt
        and u.symbol = p.symbol
    WHERE p.dt is null
        AND usd_price IS NOT NULL
    GROUP BY 1, 2

)
, prices AS (
    SELECT *
    FROM prices_usd
    where dt > '2020-10-06'
    UNION ALL
    SELECT *
    FROM swap_price_feed
    where dt > '2020-10-06'
)
-- End price feed block - output is CTE "prices"
, wallets AS (
    SELECT '\xe83de75eb3e84f3cbca3576351d81dbeda5645d4'::bytea as address
        , 'Analytics Working Group' as address_alias
    /*
    union all
    select '\xd4bcc2b5d21fe67c8be351cdb47ec1b2cd7e84a7'::bytea as address
        , 'Growth Working Group' as address_alias
    */
)
, addresses as (
    
    select '\x154c154c589b4aeccbf186fb8bc668cd7c213762'::bytea as address
        , 'Centralised Exchange Listing' as address_alias
    union all
    select '\xe83de75eb3e84f3cbca3576351d81dbeda5645d4'::bytea as address
        , 'Analytics Working Group' as address_alias
    union all
    select '\xd4bcc2b5d21fe67c8be351cdb47ec1b2cd7e84a7'::bytea as address
        , 'Growth Working Group' as address_alias
    union all
    select '\x0dea6d942a2d8f594844f973366859616dd5ea50'::bytea as address
        , 'DPI Manager' as address_alias
    union all
    select '\x25100726b25a6ddb8f8e68988272e1883733966e'::bytea as address
        , 'DPI Rebalancer' as address_alias
    union all
    select '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'::bytea as address
        , 'ETH2x-FLI Token' as address_alias
    union all
    select '\x445307De5279cD4B1BcBf38853f81b190A806075'::bytea as address
        , 'ETH2x-FLI Manager' as address_alias
    union all
    select '\x1335D01a4B572C37f800f45D9a4b36A53a898a9b'::bytea as address
        , 'ETH2x-FLI Strategy Adapter' as address_alias
    union all
    select '\x26F81381018543eCa9353bd081387F68fAE15CeD'::bytea as address
        , 'ETH2x-FLI Fee Adapter' as address_alias
    union all
    select '\x0F1171C24B06ADed18d2d23178019A3B256401D3'::bytea as address
        , 'ETH2x-FLI SupplyCapIssuanceHook' as address_alias
    union all
    select '\x0b498ff89709d3838a063f1dfa463091f9801c2b'::bytea as address
        , 'BTC2x-FLI Token' as address_alias
    union all
    select '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'::bytea as address
        , 'BTC2x-FLI Manager' as address_alias
    union all
    select '\x4a99733458349505A6FCbcF6CD0a0eD18666586A'::bytea as address
        , 'BTC2x-FLI Strategy Adapter' as address_alias
    union all
    select '\xA0D95095577ecDd23C8b4c9eD0421dAc3c1DaF87'::bytea as address
        , 'BTC2x-FLI Fee Adapter' as address_alias
    union all
    select '\x6c8137f2f552f569cc43bc4642afbe052a12441c'::bytea as address
        , 'BTC2x-FLI SupplyCapAllowedCallerIssuanceHook' as address_alias
    union all
    select '\x0954906da0Bf32d5479e25f46056d22f08464cab'::bytea as address
        , 'INDEX Token Address' as address_alias
    union all
    select '\xDD111F0fc07F4D89ED6ff96DBAB19a61450b8435'::bytea as address
        , 'INDEX Initial Airdrop Address' as address_alias
    union all
    select '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'::bytea as address
        , 'INDEX DPI Farming Contract 1 (Oct - Dec)' as address_alias
    union all
    select '\xB93b505Ed567982E2b6756177ddD23ab5745f309'::bytea as address
        , 'INDEX DPI Farming Contract 2 (Dec. 2020 - March 2021)' as address_alias
    union all
    select '\x66a7d781828B03Ee1Ae678Cd3Fe2D595ba3B6000'::bytea as address
        , 'Index Methodologist Bounty (18 months vesting)' as address_alias
    union all
    select '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea as address
        , 'Community Treasury Year 1 Vesting' as address_alias
    union all
    select '\xd89C642e52bD9c72bCC0778bCf4dE307cc48e75A'::bytea as address
        , 'Community Treasury Year 2 Vesting' as address_alias
    union all
    select '\x71F2b246F270c6AF49e2e514cA9F362B491Fbbe1'::bytea as address
        , 'Community Treasury Year 3 Vesting' as address_alias
    union all
    select '\xf64d061106054Fe63B0Aca68916266182E77e9bc'::bytea as address
        , 'Set Labs Year 1 Vesting' as address_alias
    -- union all
    -- select NULL as address -- need to look this up - on the website the address is invalid
    --     , 'Set Labs Year 2 Vesting' as address_alias
    union all
    select '\x0D627ca04A97219F182DaB0Dc2a23FB4a5B02A9D'::bytea as address
        , 'Set Labs Year 3 Vesting' as address_alias
    union all
    select '\x0D627ca04A97219F182DaB0Dc2a23FB4a5B02A9D'::bytea as address
        , 'Set Labs Year 3 Vesting' as address_alias
    union all
    select '\x319b852cd28b1cbeb029a3017e787b98e62fd4e2'::bytea as address
        , 'January 2021 Merkle Rewards Account' as address_alias
    union all
    select '\xeb1cbc809b21dddc71f0f9edc234eee6fb29acee'::bytea as address
        , 'December 2020 Merkle Rewards Account' as address_alias
    union all
    select '\x209f012602669c88bbda687fbbfe6a0d67477a5d'::bytea as address
        , 	'October 2020 Merkle Rewards Account' as address_alias
    union all
    select '\xa6bb7b6b2c5c3477f20686b98ea09796f8f93184'::bytea as address
        ,	'November 2020 Merkle Rewards Account' as address_alias
    union all
    select '\xCa3C3570beb35E5d3D85BCd8ad8F88BefaccFF10'::bytea as address
        , 'February 2021 Merkle Rewards Account' as address_alias
    union all
    select '\xa87fbb413f8de11e47037c5d697cc03de29e4e4b'::bytea as address
        , 'March 2021 Merkle Rewards Account' as address_alias
    union all
    select '\x973a526a633313b2d32b9a96ed16e212303d6905'::bytea as address
        ,	'April 2021 Merkle Rewards Account' as address_alias
    union all
    select '\x10F87409E405c5e44e581A4C3F2eECF36AAf1f92'::bytea as address
        , 'INDEX Sale 2 of 3 Multisig - Dylan, Greg, Punia' as address_alias
    
)

, creation_days AS (
    SELECT
        date_trunc('day', block_time) AS day
    FROM ethereum.traces
    WHERE address IN (SELECT address FROM wallets)
    AND TYPE = 'create'
)
, weeks AS (
    SELECT 
        generate_series(date_trunc('week', MIN(day))
                        , date_trunc('week', NOW())
                        , '1 week') AS week -- Generate all weeks since the first contract
    FROM creation_days
)
, transfers AS (
    --ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day
        , "from" AS sender_address
        , contract_address
        , "to" as recipient_address
        , sum(value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3,4
)
, transfers_month AS (
    SELECT
        date_trunc('month', tr.day) as month
        -- , tr.sender_address
        , coalesce(a.address_alias, 'unknown') as recipient_address_alias
        , tr.recipient_address
        , tok.symbol
        , avg(p.price) as avg_price
        , sum(tr.amount/10^(tok.decimals)) as amount_token
        , sum(tr.amount/10^(tok.decimals) * coalesce(p.price,0)) AS amount_usd
    FROM transfers tr
    inner join erc20.tokens tok on tr.contract_address = tok.contract_address
    left join prices p on tok.symbol = p.symbol and p.dt = tr.day
    left join addresses a on tr.recipient_address = a.address
    GROUP BY 1,2,3,4
)
select month
    , recipient_address as "Recipient Address"
    , amount_usd as "USD Value"
    , amount_token as "Qty Token"
    , symbol as "token"
    , avg_price as "INDEX Price"
    , recipient_address_alias
from transfers_month
order by 1 desc