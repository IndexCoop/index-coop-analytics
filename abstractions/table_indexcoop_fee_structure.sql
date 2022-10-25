-- https://dune.xyz/queries/317160
-- drop table if exists dune_user_generated.indexcoop_fee_structure cascade
CREATE OR REPLACE VIEW 
dune_user_generated.indexcoop_fee_structure as

select
    z.symbol,
    t.token_address,
    day,
    first_value(streaming_fee) over (partition by t.token_address, sf_part order by day asc) as streaming_fee,
    first_value(issue_fee) over (partition by t.token_address, if_part order by day asc) as issue_fee,
    first_value(redeem_fee) over (partition by t.token_address, rf_part order by day asc) as redeem_fee
from    (
        select
            d.day,
            t.token_address,
            x.streaming_fee,
            x.issue_fee,
            x.redeem_fee,
            sum(case when x.streaming_fee is null then 0 else 1 end) over (partition by t.token_address order by d.day asc) as sf_part,
            sum(case when x.issue_fee is null then 0 else 1 end) over (partition by t.token_address order by d.day asc) as if_part,
            sum(case when x.redeem_fee is null then 0 else 1 end) over (partition by t.token_address order by d.day asc) as rf_part
        from        (select token_address, date_trunc('day', min(block_time)) as min_day from dune_user_generated.indexcoop_fee_changes group by 1) t
        cross join  (select generate_series(date_trunc('day', (select min(block_time) from dune_user_generated.indexcoop_fee_changes)), date_trunc('day', now()), '1 day') as day) d
        left join   (
                    select 
                        token_address, 
                        date_trunc('day', block_time) as day, 
                        case when streaming_fee is null then 0 else streaming_fee end as streaming_fee , 
                        case when issue_fee is null then 0 else issue_fee end as issue_fee,
                        case when redeem_fee is null then 0 else redeem_fee end as redeem_fee,
                        row_number() over (partition by token_address, date_trunc('day', block_time) order by block_time desc, priority desc) as rnb
                    from    dune_user_generated.indexcoop_fee_changes
                    ) x on x.day = d.day and x.token_address = t.token_address and x.rnb = 1
        where       d.day >= t.min_day
        ) t
left join   dune_user_generated.indexcoop_tokens z on z.token_address = t.token_address
