--https://dune.com/queries/1555356

create or replace view dune_user_generated.seth2_updates as

select
    pd_start,
    pd_length,
    365 * (rate_increase / pd_length) as pd_apr,
    pd_start_nav
from    (
        select 
            time as pd_start,
            extract(epoch from lead(time,1) over (order by time) - time)/60/60/24 as pd_length,
            (1+ lead("rewardpertoken",1) over (order by time)) / (1 + "rewardpertoken") - 1 as rate_increase,
            1+"rewardpertoken" as pd_start_nav
        from    (
                select 
                    block_time as time,
                    bytea2numeric(substring(text(data) from 1 for 66)::bytea)/1e18 as periodRewards,
                    bytea2numeric(('\x' || (right(substring(text(data) from 67 for 64), 40)))::bytea)/1e18 as totalRewards,
                    bytea2numeric(('\x' || (right(substring(text(data) from 131 for 64), 40)))::bytea)/1e18 as rewardPerToken,
                    bytea2numeric(('\x' || (right(substring(text(data) from 195 for 64), 40)))::bytea)/1e18 as distributorReward,
                    bytea2numeric(('\x' || (right(substring(text(data) from 259 for 64), 40)))::bytea)/1e18 as protocolReward
                from        ethereum.logs
                where       "contract_address" = '\x20bc832ca081b91433ff6c17f85701b6e92486c5'
                and         "topic1" = '\xb9c8611ba2eb0880a25df0ebde630048817ebee5f33710af0da51958c621ffd7'
                ) t0
        ) t1
