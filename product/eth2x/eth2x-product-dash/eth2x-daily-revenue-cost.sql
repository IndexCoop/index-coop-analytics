-- https://duneanalytics.com/queries/53279/105647

-- V2 query here - https://duneanalytics.com/queries/93652
-- CONTRACTS
-- BaseManager --> 0x445307De5279cD4B1BcBf38853f81b190A806075
-- SupplyCapIssuanceHook --> 0x0F1171C24B06ADed18d2d23178019A3B256401D3
-- FeeSplitAdapter --> 0x26F81381018543eCa9353bd081387F68fAE15CeD
-- FlexibleLeverageStrategyAdapter v1 --> 0x1335D01a4B572C37f800f45D9a4b36A53a898a9b
-- FlexibleLeverageStrategyAdapter v2 --> 0x90A17826C80Ea4917BBD64b281d92aAF2bBb0024
-- FlexibleLeverageStrategyAdapter v3 --> 0xF6ba6441D4DAc647898F4083483AA44Da8B1446f

-- COSTS

-- Flexible Leverage Strategy Adapter
    -- Contract Creation
    -- Update Caller Status
    -- Transfer
    -- Engage
    -- Rebalance
    -- iterateRebalance
    -- Ripcord
WITH flsa_transactions AS (

    SELECT
        hash,
        block_time,
        'contract creation' AS transaction
    FROM ethereum.transactions
    WHERE hash IN ('\x441098a773606776ed6a6ae6eba8ef0f2cf6ebf034fe8e10c3f57c666548541b',
        '\xded787d7c49b298e3d589c2899345e46953dbd725d9330c37a2a36a4728eca67', 
        '\xc4ff83f29d40fcc477ddb78e4905a2317c418e3cb8379e53f3cbd9c3d6f33edf')
    
    UNION ALL
    
    SELECT
        hash,
        block_time,
        'transfer' AS transaction
    FROM ethereum."transactions"
    WHERE "to" IN ('\x1335D01a4B572C37f800f45D9a4b36A53a898a9b', '\x90A17826C80Ea4917BBD64b281d92aAF2bBb0024', '\xF6ba6441D4DAc647898F4083483AA44Da8B1446f')
        AND "from" = '\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55'
    
    UNION ALL
    -- sorting by ETH2x-Contract address in V2 of this query
    -- v1 ripcord
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'ripcord' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_RipcordCalled"
    where contract_address in ('\x1335D01a4B572C37f800f45D9a4b36A53a898a9b', '\x90A17826C80Ea4917BBD64b281d92aAF2bBb0024')
    -- UNION ALL

    -- v2 ripcord    
    -- SELECT
    --     evt_tx_hash AS hash,
    --     evt_block_time AS block_time,
    --     'ripcord' AS transaction
    -- FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_RipcordCalled"
    
    UNION ALL
    
    -- v3 ripcord
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'ripcord' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyExtension_evt_RipcordCalled"
    where contract_address in ('\xF6ba6441D4DAc647898F4083483AA44Da8B1446f')
    UNION ALL
    
    -- v1 rebalance iteration
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'rebalance iteration' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_RebalanceIterated"
    where contract_address in ('\x1335D01a4B572C37f800f45D9a4b36A53a898a9b', '\x90A17826C80Ea4917BBD64b281d92aAF2bBb0024')
    -- UNION ALL
    
    -- -- v2 rebalance iteration
    -- SELECT
    --     evt_tx_hash AS hash,
    --     evt_block_time AS block_time,
    --     'rebalance iteration' AS transaction
    -- FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_RebalanceIterated"
    
    UNION ALL 
    
    -- v3 rebalance iteration
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'rebalance iteration' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyExtension_evt_RebalanceIterated"
    where contract_address in ('\xF6ba6441D4DAc647898F4083483AA44Da8B1446f')

    UNION ALL
    
    -- v1 rebalance
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'rebalance' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_Rebalanced"
    where contract_address in ('\x1335D01a4B572C37f800f45D9a4b36A53a898a9b', '\x90A17826C80Ea4917BBD64b281d92aAF2bBb0024')
    -- UNION ALL
    
    -- v2 rebalance
    -- SELECT
    --     evt_tx_hash AS hash,
    --     evt_block_time AS block_time,
    --     'rebalance' AS transaction
    -- FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_Rebalanced"
    
    UNION ALL
    
    -- v3 rebalance
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'rebalance' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyExtension_evt_Rebalanced"
    where contract_address in ('\xF6ba6441D4DAc647898F4083483AA44Da8B1446f')
    
    UNION ALL 
    
    -- v1 update caller status
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update caller status' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_CallerStatusUpdated"
    where contract_address in ('\x1335D01a4B572C37f800f45D9a4b36A53a898a9b', '\x90A17826C80Ea4917BBD64b281d92aAF2bBb0024')
    -- UNION ALL
    
    -- -- v2 update caller status
    -- SELECT
    --     evt_tx_hash AS hash,
    --     evt_block_time AS block_time,
    --     'update caller status' AS transaction
    -- FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_CallerStatusUpdated"
    
    UNION ALL
    
    -- v3 update caller status
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update caller status' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyExtension_evt_CallerStatusUpdated"
    where contract_address in ('\xF6ba6441D4DAc647898F4083483AA44Da8B1446f')
    
    UNION ALL
    
    -- v1 engage
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'engage' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_Engaged"
    where contract_address in ('\x1335D01a4B572C37f800f45D9a4b36A53a898a9b', '\x90A17826C80Ea4917BBD64b281d92aAF2bBb0024')
    -- UNION ALL
    
    -- v2 engage
    -- SELECT
    --     evt_tx_hash AS hash,
    --     evt_block_time AS block_time,
    --     'engage' AS transaction
    -- FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_Engaged"
    
    UNION ALL
    
    -- v3 engage
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'engage' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyExtension_evt_Engaged"
    where contract_address in ('\xF6ba6441D4DAc647898F4083483AA44Da8B1446f')

),

-- Supply Cap Issuance Hook
    -- Contract Creation
    -- Supply Cap Updated
    -- Ownership Transferred
scih_transactions AS (

    -- contract creation
    SELECT
        hash,
        block_time,
        'contract creation' AS transaction
    FROM ethereum.transactions
    WHERE hash IN ('\x0a8293124630c713014c51a5265a579cf7db4d4bc9f10c5cd0756597c3eddb1c')
    
    UNION ALL
    
    -- supply cap update
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update supply cap' AS transaction
    FROM setprotocol_v2."SupplyCapIssuanceHook_evt_SupplyCapUpdated"
    where contract_address = '\x0F1171C24B06ADed18d2d23178019A3B256401D3'
    
    UNION ALL
    
    -- ownership transfer
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'transfer ownership' AS transaction
    FROM setprotocol_v2."SupplyCapIssuanceHook_evt_OwnershipTransferred"
    where contract_address = '\x0F1171C24B06ADed18d2d23178019A3B256401D3'
),

-- Fee Adapter
    -- Contract Creation
    -- Update Fee Recipient
    -- Ownership Transferred
    -- Accrue Fees
    -- Register Upgrade
    -- Update Caller Status
    -- Update Anyone Callable
    -- Query V2 - Fee Adaper doesn't need filtering yet as BTC2x Fee Adapter data is
    -- under different project name on Dune(by mistake). Still adding it in so it all works when 
    -- we have new FLI products. 
fa_transactions AS (

    -- contract creation
    SELECT
        hash,
        block_time,
        'contract creation' AS transaction
    FROM ethereum.transactions
    WHERE hash IN ('\x2a3f34729a8a39d613d2e977f32cec2cbca02e98039005e85cd49846bca14cab')
    
    UNION ALL
    
    -- update fee recipient
    SELECT
        call_tx_hash AS hash,
        call_block_time AS block_time,
        'update fee recipient' AS transaction
    FROM indexcoop."FeeSplitAdapter_call_updateFeeRecipient"
    where contract_address = '\x26F81381018543eCa9353bd081387F68fAE15CeD'
    
    UNION ALL 
    
    -- ownership transfer
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'transfer ownership' AS transaction
    FROM indexcoop."FeeSplitAdapter_evt_OwnershipTransferred"
    where contract_address = '\x26F81381018543eCa9353bd081387F68fAE15CeD'
    
    UNION ALL 
    
    -- accrue fees
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'accrue fees' AS transaction
    FROM indexcoop."FeeSplitAdapter_evt_FeesAccrued"
    where contract_address = '\x26F81381018543eCa9353bd081387F68fAE15CeD'

    UNION ALL 
    
    -- register upgrade
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'register upgrade' AS transaction
    FROM indexcoop."FeeSplitAdapter_evt_UpgradeRegistered"
    where contract_address = '\x26F81381018543eCa9353bd081387F68fAE15CeD'

    UNION ALL
    
    -- update caller status
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update caller status' AS transaction
    FROM indexcoop."FeeSplitAdapter_evt_CallerStatusUpdated"
    where contract_address = '\x26F81381018543eCa9353bd081387F68fAE15CeD'

    UNION ALL
    
    -- update anyone callable
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update anyone callable' AS transaction
    FROM indexcoop."FeeSplitAdapter_evt_AnyoneCallableUpdated"
    where contract_address = '\x26F81381018543eCa9353bd081387F68fAE15CeD'
),

-- Manager
    -- Contract Creation
    -- Set Manager
    -- Set Operator
    -- Add Adapter
    -- Remove Adapter
    -- Change Methodologist
mngr_transactions AS (

    -- contract creation
    SELECT
        hash,
        block_time,
        'contract creation' AS transaction
    FROM ethereum.transactions
    WHERE hash IN ('\x4cc4a235a1a9d99712fc0072d73d58dd44e754ed97728d5574464b0f7d8499c7')
    
    UNION ALL
    
    -- set manager
    SELECT
        call_tx_hash AS hash,
        call_block_time AS block_time,
        'set manager' AS transaction
    FROM setprotocol_v2."BaseManager_call_setManager"
    where contract_address = '\x445307De5279cD4B1BcBf38853f81b190A806075'
    
    UNION ALL
    
    -- set operator
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'set operator' AS transaction
    FROM setprotocol_v2."BaseManager_evt_OperatorChanged"
    where contract_address = '\x445307De5279cD4B1BcBf38853f81b190A806075'

    UNION ALL
    
    -- add adapter
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'add adapter' AS transaction
    FROM setprotocol_v2."BaseManager_evt_AdapterAdded"
    where contract_address = '\x445307De5279cD4B1BcBf38853f81b190A806075'
    
    UNION ALL
    
    -- remove adapter
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'remove adapter' AS transaction
    FROM setprotocol_v2."BaseManager_evt_AdapterRemoved"
    where contract_address = '\x445307De5279cD4B1BcBf38853f81b190A806075'

    UNION ALL
    
    -- change methodologist
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'change methodologist' AS transaction
    FROM setprotocol_v2."BaseManager_evt_MethodologistChanged"
    where contract_address = '\x445307De5279cD4B1BcBf38853f81b190A806075'
),

transaction_costs_temp AS (

    SELECT * FROM flsa_transactions
    
    UNION ALL 
    
    SELECT * FROM scih_transactions
    
    UNION ALL
    
    SELECT * FROM fa_transactions
    
    UNION ALL
    
    SELECT * FROM mngr_transactions

),

transaction_costs AS (

SELECT
    a.*,
    (gas_price * gas_used) / 10^18 AS eth_used_for_gas,
    ((gas_price * gas_used) / 10^18) * p.price AS usd_gas_cost
FROM transaction_costs_temp a
LEFT JOIN ethereum.transactions b ON a.hash = b.hash
LEFT JOIN prices."layer1_usd_eth" p ON p.minute = date_trunc('minute', a.block_time) 

),

-- REVENUE
fli_mint_burn AS (

    SELECT 
        date_trunc('day', evt_block_time) AS day,
        'mint' AS action,
        SUM("_quantity"/1e18) AS amount 
        FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"
        WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day,
        'redeem' AS action,
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    GROUP BY 1
    
),

fli_days AS (
    
    SELECT generate_series('2021-03-15'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    
),

fli_units AS (

    SELECT
        d.day,
        COALESCE(m.amount, 0) AS amount
    FROM fli_days d
    LEFT JOIN fli_mint_burn m ON d.day = m.day
    
),

fli AS (

SELECT 
    day,
    'ETH2X-FLI' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM fli_units

),

fli_swap AS (

--eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
    
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-03-14'

),

fli_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-03-12'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2
                
),

fli_hours AS (
    
    SELECT generate_series('2021-03-12 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

fli_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as eth_price
    -- a1_prcs."minute" AS minute
FROM fli_hours h
LEFT JOIN fli_swap s ON s."hour" = h.hour 
LEFT JOIN fli_a1_prcs a ON h."hour" = a."hour"
GROUP BY 1
ORDER BY 1

),

fli_feed AS (

SELECT
    hour,
    'ETH2X-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(eth_price) OVER (ORDER BY hour), NULL))[COUNT(eth_price) OVER (ORDER BY hour)] AS eth_price
FROM fli_temp

),

fli_aum AS (

SELECT
    d.*,
    f.usd_price AS price,
    f.usd_price * d.units AS aum
FROM fli d
LEFT JOIN fli_feed f ON f.product = d.product AND d.day = f.hour

),

fli_mint_burn_amount AS (

SELECT
    day,
    SUM(ABS(amount)) AS amount
FROM fli_mint_burn
GROUP BY 1

),

fli_mint_burn_revenue AS (

    SELECT
        a.*,
        a.amount * b.usd_price * .0006 AS revenue
    FROM fli_mint_burn_amount a
    LEFT JOIN fli_feed b ON a.day = b.hour

),

fli_revenue AS (

    SELECT
        DISTINCT
        a.day,
        'revenue' AS detail,
        (a.aum * .0117/365) + COALESCE(b.revenue, 0) AS revenue
    FROM fli_aum a
    LEFT JOIN fli_mint_burn_revenue b ON a.day = b.day
    
),

revenue AS (

SELECT 
    *
FROM fli_revenue
ORDER BY 1

),

cost AS (

SELECT
    date_trunc('day', block_time) AS day,
    'cost' AS detail,
    -SUM(usd_gas_cost) AS cost
FROM transaction_costs
GROUP BY 1, 2
ORDER BY 1

),

agg AS (

    SELECT
        COALESCE(r.day, c.day) AS day,
        COALESCE(r.revenue, 0) AS revenue,
        COALESCE(c.cost, 0) AS cost,
        COALESCE(r.revenue, 0) + COALESCE(c.cost, 0) AS profit
    FROM revenue r
    FULL OUTER JOIN cost c ON r.day = c.day 

)

SELECT * FROM agg


