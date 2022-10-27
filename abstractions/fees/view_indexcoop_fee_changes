-- https://dune.com/queries/1298781
 -- drop view if exists dune_user_generated.indexcoop_fee_changes
create or replace view dune_user_generated.indexcoop_fee_changes as (

with 

set_created as (
select
    token_address,
    evt_block_time as block_time,
    0 as priority
from setprotocol_v2."SetTokenCreator_evt_SetTokenCreated" s
inner join dune_user_generated.indexcoop_tokens t on s."_setToken" = t.token_address
),

streaming_fees as (
select 
'Streaming Fee Initialized' as event,
"call_block_time" as block_time,
symbol,
token_address,
(select "streamingFeePercentage"/1e18 from json_to_record("_settings"::json) as x(
    "feeRecipient" varchar, 
    "streamingFeePercentage" numeric, 
    "lastStreamingFeeTimestamp" numeric, 
    "maxStreamingFeePercentage" numeric
)) as streaming_fee,
1 as priority
from setprotocol_v2."StreamingFeeModule_call_initialize" s 
inner join dune_user_generated.indexcoop_tokens t on s."_setToken" = t.token_address
where "call_success" = true

union

select 
    'Streaming Fee Updated' as event,
    "evt_block_time" as block_time,
    symbol,
    token_address,
    "_newStreamingFee"/1e18 as streaming_fee,
    2 as priority
from setprotocol_v2."StreamingFeeModule_evt_StreamingFeeUpdated" s
inner join dune_user_generated.indexcoop_tokens t on s."_setToken" = t.token_address
),

issue_redeem_fees as (
select
    'Issue/Redeem Fee Initialize' as event,
    "call_block_time" as block_time,
    symbol,
    token_address,
    "_managerIssueFee"/1e18 as issue_fee,
    "_managerRedeemFee"/1e18 as redeem_fee,
    1 as priority
from    (
        select * from setprotocol_v2."DebtIssuanceModule_call_initialize" 
        union
        select * from setprotocol_v2."DebtIssuanceModuleV2_call_initialize"
        ) s 
inner join  dune_user_generated.indexcoop_tokens t on s."_setToken" = t.token_address
where       "call_success" = true    

union

select
    'Redeem Fee Updated' as event,
    "evt_block_time" as block_time,
    symbol,
    token_address,
    null::numeric as issue_fee,
    "_newRedeemFee"/1e18 as redeem_fee,
    2 as priority
from    (
        select * from setprotocol_v2."DebtIssuanceModule_evt_RedeemFeeUpdated"
        union
        select * from setprotocol_v2."DebtIssuanceModuleV2_evt_RedeemFeeUpdated"
        ) s
inner join dune_user_generated.indexcoop_tokens t on s."_setToken" = t.token_address

union

select
    'Issue Fee Updated' as event,
    "evt_block_time" as block_time,
    symbol,
    token_address,
    "_newIssueFee"/1e18 as issue_fee,
    null::numeric as redeem_fee,
    2 as priority
from    (
        select * from setprotocol_v2."DebtIssuanceModule_evt_IssueFeeUpdated"
        union
        select * from setprotocol_v2."DebtIssuanceModuleV2_evt_IssueFeeUpdated"
        ) s
inner join dune_user_generated.indexcoop_tokens t on s."_setToken" = t.token_address
),

all_fees as (
select
    token_address,
    block_time,
    priority,
    first_value(streaming_fee) over (partition by token_address, sf_part order by block_time asc, priority asc) as streaming_fee,
    first_value(issue_fee) over (partition by token_address, if_part order by block_time asc, priority asc) as issue_fee,
    first_value(redeem_fee) over (partition by token_address, rf_part order by block_time asc, priority asc) as redeem_fee
from    (
        select
            token_address,
            block_time,
            streaming_fee,
            issue_fee,
            redeem_fee,
            priority,
            sum(case when streaming_fee is null then 0 else 1 end) over (partition by token_address order by block_time asc, priority asc) as sf_part,
            sum(case when issue_fee is null then 0 else 1 end) over (partition by token_address order by block_time asc, priority asc) as if_part,
            sum(case when redeem_fee is null then 0 else 1 end) over (partition by token_address order by block_time asc, priority asc) as rf_part
        from    (
                select token_address, block_time, priority, null::numeric as streaming_fee, null::numeric  as issue_fee, null::numeric as redeem_fee from set_created
                union
                select token_address, block_time, priority, streaming_fee, null::numeric as issue_fee, null::numeric as redeem_fee from streaming_fees
                union
                select token_address, block_time, priority, null::numeric as streaming_fee, issue_fee, redeem_fee from issue_redeem_fees
                ) t0
        ) t1
)

select * from all_fees

)
