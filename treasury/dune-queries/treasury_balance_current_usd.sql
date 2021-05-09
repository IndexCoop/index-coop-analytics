/*
    query here: https://duneanalytics.com/queries/44425

    forked from https://duneanalytics.com/queries/22041/46378

    --- INDEX Treasury ---

    Wallet / Address
    ('\x9467cfadc9de245010df95ec6a585a506a8ad5fc', -- Treasury Wallet
    '\xe2250424378b6a6dC912f5714cfd308a8D593986', -- Treasury Committee Wallet
    '\x26e316f5b3819264DF013Ccf47989Fb8C891b088' -- Community Treasury Year 1 Vesting
    )    
    
    -- Target tokens:
    WHEN contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab' THEN 'INDEX'
    WHEN contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN 'DPI'
    when contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' then 'FLI-ETH2X'

*/

WITH wallets AS (
    SELECT 'INDEX' AS org
        , '\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea AS address
        , 'Treasury Wallet' AS wallet
    UNION
    SELECT 'INDEX' AS org
        , '\xe2250424378b6a6dC912f5714cfd308a8D593986'::bytea AS address
        , 'Treasury Committee' AS wallet
    union
    select 'INDEX' AS org
    , '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea AS address
    , 'Community Treasury Year 1 Vesting' AS wallet
)

, creation_days AS (
    SELECT
        date_trunc('day', block_time) AS day
    FROM ethereum.traces
    WHERE address IN (SELECT address FROM wallets)
    AND TYPE = 'create'
)

, days AS (
    SELECT 
        generate_series(MIN(day), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    FROM creation_days
)

, prices as ( 
    -- using dex price approximations table only containes FLI-ETH2X
    select date_trunc('day',hour) as day
        , contract_address
        , case when contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab' THEN 'INDEX'
            WHEN contract_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' THEN 'DPI'
            when contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' then 'FLI-ETH2X'
            end as symbol
        , avg(median_price) as price
    from dex.view_token_prices
    where contract_address in ('\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd') -- FLI-ETH2X,
    group by 1,2,3
    union
    -- prices.usd only contains price data for INDEX and DPI
    select date_trunc('day', minute)
        , contract_address
        , symbol
        , avg(price) as price
    from prices.usd
    where contract_address in ('\x0954906da0bf32d5479e25f46056d22f08464cab' -- INDEX
                        , '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b') -- DPI
    group by 1,2,3

)

, transfers AS (
--ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day,
        "from" AS address,
        contract_address,
        sum(-value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
    
UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day,
        "to" AS address,
        contract_address,
        sum(value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "to" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
)

, transfers_day AS (
    SELECT
        t.day,
        t.address,
        t.contract_address,
        sum(t.amount/10^18) AS change -- all target contracts have decimals of 18
    FROM transfers t
    GROUP BY 1,2,3
)

, balances_w_gap_days AS (
    SELECT
        day,
        address,
        contract_address,
        sum(change) OVER (PARTITION BY address, contract_address ORDER BY day) AS "balance",
        lead(day, 1, now()) OVER (PARTITION BY address, contract_address ORDER BY day) AS next_day
    FROM transfers_day
)

, balances_all_days AS (
    SELECT
        d.day,
--        b.address,
        b.contract_address,
        sum(b.balance) AS "balance"
    FROM balances_w_gap_days b
    INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1,2 --,3
    ORDER BY 1,2 --,3
)

, usd_value_all_days as (
SELECT
    b.day,
--    b.address,
--    w.wallet,
--    w.org,
    b.contract_address,
    p.symbol AS token,
    b.balance,
    p.price,
    b.balance * coalesce(p.price,0) AS usd_value
    , rank() over (order by b.day desc)
FROM balances_all_days b
LEFT OUTER JOIN prices p ON b.contract_address = p.contract_address AND b.day = p.day
-- LEFT OUTER JOIN wallets w ON b.address = w.address
ORDER BY usd_value DESC
LIMIT 10000
)
select contract_address
    , token
    , balance
    , usd_value
from usd_value_all_days
where rank = 1
;
