/*
query here: https://duneanalytics.com/queries/92921

The purpose of this query is to forecast out how much INDEX is available to the Treasury and plot that
alongside the amount that been already claimed

Key parameters and hard coding values

Vesting Contract 1: '\x26e316f5b3819264df013ccf47989fb8c891b088'
Vesting Contract 2: '\xd89c642e52bd9c72bcc0778bcf4de307cc48e75a'
Vesting Contract 3: '\x71f2b246f270c6af49e2e514ca9f362b491fbbe1'

Index Treasury: '\x9467cfadc9de245010df95ec6a585a506a8ad5fc'

Assuming 365 days a year for all years
Vesting Contract 1 Total INDEX: 2375000, Daily INDEX: 6506.8493
Vesting Contract 2 Total INDEX: 1425000, Daily INDEX: 3904.1096
Vesting Contract 3 Total INDEX: 950000, Daily INDEX: 2602.7397

Assumed vesting start timestamp UTC: '2020-10-06 06:46:00' 
Vesting Contract 1 validity dates: '2020-10-06' to '2021-10-05'
Vesting Contract 2 validity dates: '2021-10-06' to '2022-10-05'
Vesting Contract 3 validity dates: '2022-10-06' to '2023-10-05'

*/

with days as (
    SELECT generate_series('2020-10-06'::date, '2023-10-05'::date, '1 day') AS day -- Generate all days since first contract deployment
)
, vesting_values as (
    select 'INDEX Community Treasury Year 1' as vesting_contract, 6506.8493 as daily_index_grant
    union
    select 'INDEX Community Treasury Year 2' as vesting_contract, 3904.1096 as daily_index_grant
    union
    select 'INDEX Community Treasury Year 3' as vesting_contract, 2602.7397 as daily_index_grant
)
, days_tagged as (
    select day
        , case when day between '2020-10-06' and '2021-10-05' then 'INDEX Community Treasury Year 1'
            when day between '2021-10-06' and '2022-10-05' then 'INDEX Community Treasury Year 2'
            when day between '2022-10-06' and '2023-10-05' then 'INDEX Community Treasury Year 3'
            else 'ERROR' end as vesting_contract
    from days
)
, granted_index as (
    select d.day
        , d.vesting_contract
        , v.daily_index_grant
        , sum(v.daily_index_grant) over (order by d.day) as cumulative_granted_index
    from days_tagged d
    inner join vesting_values v on d.vesting_contract = v.vesting_contract
)
, claim_index_evts as (
    SELECT
         evt_block_time::date as day
        , sum(tr.value/1e18) AS amount
     FROM erc20."ERC20_evt_Transfer" tr 
     WHERE tr."from" in (
          '\x26e316f5b3819264df013ccf47989fb8c891b088' -- Vesting Contract 1
        , '\xd89c642e52bd9c72bcc0778bcf4de307cc48e75a' -- Vesting Contract 2
        , '\x71f2b246f270c6af49e2e514ca9f362b491fbbe1' -- Vesting Contract 3
     )
     and tr."to" = '\x9467cfadc9de245010df95ec6a585a506a8ad5fc' -- Index Treasury
     and contract_address = '\x0954906da0bf32d5479e25f46056d22f08464cab' -- INDEX token
     group by 1
)
, cum_claimed_index_w_gap_days as (
    select day
        , sum(amount) OVER (ORDER BY day) AS claimed_index
        , lead(day, 1, '2023-10-06'::date) OVER (ORDER BY day) AS next_day
    from claim_index_evts cie
    
)
, cum_claimed_index_all_days as (
    select d.day
        , cci.claimed_index
    from cum_claimed_index_w_gap_days cci
    inner join days d ON cci.day <= d.day AND d.day < cci.next_day
)
select gi.day
    , gi.cumulative_granted_index
    , coalesce(cci.claimed_index,0) as claimed_index
    , gi.cumulative_granted_index - coalesce(cci.claimed_index,0) as vested_index_available
from granted_index gi
left join cum_claimed_index_all_days cci on gi.day = cci.day