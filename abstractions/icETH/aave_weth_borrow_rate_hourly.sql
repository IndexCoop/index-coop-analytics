-- https://dune.com/queries/1581433

create or replace view dune_user_generated.aave_weth_borrow_rate_hourly as (
with

borrows as (
select  
    "evt_block_time" as start_time,
    coalesce(lead("evt_block_time", 1) over (order by "evt_block_number" asc, "evt_index" asc), now()) as end_time,
    date_trunc('hour', "evt_block_time") as start_hour,
    date_trunc('hour', coalesce(lead("evt_block_time", 1) over (order by "evt_block_number" asc, "evt_index" asc), now())) as end_hour,
    extract(epoch from coalesce(lead("evt_block_time", 1) over (order by "evt_block_number" asc, "evt_index" asc), now()) - "evt_block_time") as seconds,
    "variableBorrowRate"/1e27 as rate
from        aave_v2."LendingPool_evt_ReserveDataUpdated"
where       reserve = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
and         "evt_block_time" >= '2022-03-20'
)

select
    gs.hour,
    sum(rate * seconds) / 3600 as rate
from        (select generate_series('2022-03-20 00:00', date_trunc('hour', now()), '1 hour') as hour) gs
left join   (
            -- start hour
            select
                start_hour as hour, 
                rate,
                case 
                    when start_hour = end_hour then seconds
                    when start_hour != end_hour then extract(epoch from (start_hour + interval '1 hour') - start_time)
                end as seconds
            from borrows
            
            union
            
            select
                end_hour as hour,
                rate,
                extract(epoch from end_time - end_hour) as seconds
            from    borrows
            where   start_hour != end_hour
            
            union
            
            select
                gs.hour,
                rate,
                60*60*24 as seconds
            from        (select generate_series('2022-03-20 00:00', date_trunc('hour', now()), '1 hour') as hour) gs
            inner join  borrows b on gs.hour > b.start_hour + interval '1 hour' and gs.hour < b.end_hour
            where       b.end_hour - b.start_hour > '1 hour'
            ) t on t.hour = gs.hour  
group by 1
order by 1
)
