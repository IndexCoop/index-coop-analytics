-- https://dune.xyz/queries/308544
CREATE OR REPLACE view dune_user_generated.indexcoop_issuance_daily as

select
    symbol,
    day,
    coalesce(issued_amount, 0) as issued_amount,
    coalesce(redeemed_amount, 0) as redeemed_amount,
    coalesce(net_amount, 0) as net_amount,
    coalesce(gross_amount, 0) as gross_amount,
    sum(net_amount) over (partition by symbol order by day asc rows between unbounded preceding and current row) as supply
from    (select
            dat.symbol,
            dat.day,
            sum(s.issued_amount) as issued_amount,
            sum(s.redeemed_amount) as redeemed_amount,
            sum(s.net_amount) as net_amount,
            sum(s.issued_amount) + sum(s.redeemed_amount) as gross_amount
        from        (select distinct
                        symbol,
                        generate_series (
                            date_trunc('day', min(evt_block_time)),    -- Start Series
                            date_trunc('day', NOW()),                  -- End Series    
                            '1 day'                                    -- Interval Length   
                            ) as day
                    from        dune_user_generated.indexcoop_issuance_events
                    group by    symbol
                    order by    symbol, day
                    ) dat
        left join   (select
                        evt_block_time,
                        symbol,
                        case
                            when evt_type = 'Issue' then amount
                            else 0
                        end as issued_amount,
                        case
                            when evt_type = 'Redeem' then amount
                            else 0
                        end as redeemed_amount,
                        case
                            when evt_type = 'Issue' then amount
                            when evt_type = 'Redeem' then - amount
                            else 0
                        end as net_amount   
                    from dune_user_generated.indexcoop_issuance_events
                    ) s on dat.day = date_trunc('day', s.evt_block_time) and dat.symbol = s.symbol
        group by    dat.day, dat.symbol
        ) x