-- CONTRACTS
-- BaseManager --> 0xc7aede3b12daad3ffa48fc96ccb65659ff8d261a
-- SupplyCapIssuanceHook(SupplyCapAllowedCallerIssuanceHook) --> 0x6C8137F2F552F569CC43BC4642afbe052a12441C
-- FeeSplitAdapter --> 0xA0D95095577ecDd23C8b4c9eD0421dAc3c1DaF87
-- FlexibleLeverageStrategyAdapter v1 --> 0x4a99733458349505A6FCbcF6CD0a0eD18666586A
-- Hash V1 - 0x1bb07f4900b568041a8ae60012b9b1e613363fab7778794a0d76c7ddb373ebe1
-- FlexibleLeverageStrategyAdapter v2 --> 0x6B351cdd65704D86134c183aa4BBfFb0833e4A8c
-- Hash V2 - 0xaef6bfd06e4e43ebb3422e969b486617a36895daf1d49ded734111a66cdf9ea2
-- FlexibleLeverageStrategyAdapter v3 --> 0x2612fA1E336cb248ee00eFD02f1C941a7A015e76

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
    WHERE hash IN ('\x1bb07f4900b568041a8ae60012b9b1e613363fab7778794a0d76c7ddb373ebe1',
        '\xaef6bfd06e4e43ebb3422e969b486617a36895daf1d49ded734111a66cdf9ea2', 
        '\x8c8676c6ae8cf3f34cf4efa36fcf9a49ea2d53c44e8e5af69eec52d3b8694812')
    
    UNION ALL
    
    SELECT
        hash,
        block_time,
        'transfer' AS transaction
    FROM ethereum."transactions"
    WHERE "to" IN ('\x4a99733458349505A6FCbcF6CD0a0eD18666586A', '\x6B351cdd65704D86134c183aa4BBfFb0833e4A8c', '0x2612fA1E336cb248ee00eFD02f1C941a7A015e76')
        AND "from" = '\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55'
    
    UNION ALL
    -- sorted by BTC2x-contract addresses 
    -- v1 ripcord
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'ripcord' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_RipcordCalled"
    where contract_address in ('\x4a99733458349505A6FCbcF6CD0a0eD18666586A', '\x6B351cdd65704D86134c183aa4BBfFb0833e4A8c')
    
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
    -- adding BTC2x FLI v3 Extension Address 
    where contract_address = '\x2612fA1E336cb248ee00eFD02f1C941a7A015e76'
    
    UNION ALL
    
    -- v1 rebalance iteration
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'rebalance iteration' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_RebalanceIterated"
    where contract_address in ('\x4a99733458349505A6FCbcF6CD0a0eD18666586A', '\x6B351cdd65704D86134c183aa4BBfFb0833e4A8c')
    
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
    where contract_address = '\x2612fA1E336cb248ee00eFD02f1C941a7A015e76'
    
    UNION ALL
    
    -- v1 rebalance
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'rebalance' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_Rebalanced"
    where contract_address in ('\x4a99733458349505A6FCbcF6CD0a0eD18666586A', '\x6B351cdd65704D86134c183aa4BBfFb0833e4A8c')
    
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
    where contract_address = '\x2612fA1E336cb248ee00eFD02f1C941a7A015e76'
    
    UNION ALL 
    
    -- v1 update caller status
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update caller status' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_CallerStatusUpdated"
    where contract_address in ('\x4a99733458349505A6FCbcF6CD0a0eD18666586A', '\x6B351cdd65704D86134c183aa4BBfFb0833e4A8c')
    
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
    where contract_address = '\x2612fA1E336cb248ee00eFD02f1C941a7A015e76'

    
    UNION ALL
    
    -- v1 engage
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'engage' AS transaction
    FROM setprotocol_v2."FlexibleLeverageStrategyAdapter_evt_Engaged"
    where contract_address in ('\x4a99733458349505A6FCbcF6CD0a0eD18666586A', '\x6B351cdd65704D86134c183aa4BBfFb0833e4A8c')

    
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
    where contract_address = '\x2612fA1E336cb248ee00eFD02f1C941a7A015e76'
    

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
    WHERE hash IN ('\x27c73aadbb455043e27cd827756b06f3f840044a407f5f2a1acbd5e1e25123ac')
    
    UNION ALL
    
    -- supply cap update
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update supply cap' AS transaction
    FROM setprotocol_v2."SupplyCapIssuanceHook_evt_SupplyCapUpdated"
    where contract_address = '\x6c8137f2f552f569cc43bc4642afbe052a12441c'
    
    UNION ALL
    
    -- ownership transfer
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'transfer ownership' AS transaction
    FROM setprotocol_v2."SupplyCapIssuanceHook_evt_OwnershipTransferred"
    where contract_address = '\x6c8137f2f552f569cc43bc4642afbe052a12441c'

),

-- Fee Adapter
    -- Contract Creation
    -- Update Fee Recipient
    -- Ownership Transferred
    -- Accrue Fees
    -- Register Upgrade
    -- Update Caller Status
    -- Update Anyone Callable
 --- Fee Adapter for BTC2x FLI was decoded under the project name SetProtocol_v2 instead of Index Coop due to a submission mistake   
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
    FROM setprotocol_v2. "FeeSplitAdapter_call_updateFeeRecipient"
    
    UNION ALL 
    
    -- ownership transfer
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'transfer ownership' AS transaction
    FROM setprotocol_v2. "FeeSplitAdapter_evt_OwnershipTransferred"
    
    UNION ALL 
    
    -- accrue fees
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'accrue fees' AS transaction
    FROM setprotocol_v2. "FeeSplitAdapter_evt_FeesAccrued"
    
    UNION ALL 
    
    -- register upgrade
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'register upgrade' AS transaction
    FROM setprotocol_v2. "FeeSplitAdapter_evt_UpgradeRegistered"
    
    UNION ALL
    
    -- update caller status
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update caller status' AS transaction
    FROM setprotocol_v2. "FeeSplitAdapter_evt_CallerStatusUpdated"
    
    UNION ALL
    
    -- update anyone callable
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'update anyone callable' AS transaction
    FROM setprotocol_v2. "FeeSplitAdapter_evt_AnyoneCallableUpdated"

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
    where contract_address = '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'
    
    UNION ALL
    
    -- set operator
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'set operator' AS transaction
    FROM setprotocol_v2."BaseManager_evt_OperatorChanged"
    where contract_address = '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'
    
    UNION ALL
    
    -- add adapter
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'add adapter' AS transaction
    FROM setprotocol_v2."BaseManager_evt_AdapterAdded"
    where contract_address = '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'

    UNION ALL
    
    -- remove adapter
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'remove adapter' AS transaction
    FROM setprotocol_v2."BaseManager_evt_AdapterRemoved"
    where contract_address = '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'

    UNION ALL
    
    -- change methodologist
    SELECT
        evt_tx_hash AS hash,
        evt_block_time AS block_time,
        'change methodologist' AS transaction
    FROM setprotocol_v2."BaseManager_evt_MethodologistChanged"
    where contract_address = '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'

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
        WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
        GROUP BY 1

    UNION ALL

    SELECT 
        date_trunc('day', evt_block_time) AS day,
        'redeem' AS action,
        -SUM("_quantity"/1e18) AS amount 
    FROM setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed" 
    WHERE "_setToken" = '\x0b498ff89709d3838a063f1dfa463091f9801c2b'
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
    'BTC2X-FLI' AS product,
    SUM(amount) OVER (ORDER BY day) AS units
FROM fli_units

),

fli_swap AS (

--eth/fli uni        xf91c12dae1313d0be5d7a27aa559b1171cc1eac5
--btc2x/wbtc sushi 'x164FE0239d703379Bddde3c80e4d4800A1cd452B'
    
    select 
    date_trunc('hour', sw."evt_block_time") AS hour,
        ("amount0In" + "amount0Out")/1e18 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e8 AS a1_amt
    from sushi."Pair_evt_Swap" sw
    where contract_address = '\x164FE0239d703379Bddde3c80e4d4800A1cd452B'
    AND sw.evt_block_time >= '2021-03-14' -- 

),

fli_a1_prcs AS (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-03-12'
        AND contract_address ='\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599' --wbtc as base asset
    GROUP BY 2
                
),

fli_hours AS (
    
    SELECT generate_series('2021-05-01 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
),

fli_temp AS (

SELECT
    h.hour,
    COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price, 
    COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as btc_price
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
    'BTC2X-FLI' AS product,
    (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (ORDER BY hour), NULL))[COUNT(usd_price) OVER (ORDER BY hour)] AS usd_price,
    (ARRAY_REMOVE(ARRAY_AGG(btc_price) OVER (ORDER BY hour), NULL))[COUNT(btc_price) OVER (ORDER BY hour)] AS btc_price
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
    action,
    SUM(ABS(amount)) AS amount
FROM fli_mint_burn
GROUP BY 1, 2

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

SELECT
    day,
    SUM(revenue) OVER (ORDER BY day) AS revenue,
    SUM(ABS(cost)) OVER (ORDER BY day) AS cost,
    SUM(profit) OVER (ORDER BY day) AS profit
FROM agg



