-- https://dune.xyz/queries/308476
CREATE OR REPLACE view dune_user_generated.indexcoop_issuance_hourly as

select
    hat.symbol,
    hat.hour,
    coalesce(s.issued_amount, 0) as issued_amount,
    coalesce(s.redeemed_amount, 0) as redeemed_amount,
    coalesce(s.issued_amount, 0) - coalesce(s.redeemed_amount, 0) as net_amount,
    coalesce(s.issued_amount, 0) + coalesce(s.redeemed_amount, 0) as gross_amount,
    sum(s.issued_amount - s.redeemed_amount) over (partition by hat.symbol order by hat.hour asc rows between unbounded preceding and current row) as supply
from        (select distinct
                symbol,
                generate_series (
                    date_trunc('hour', min(evt_block_time)),    -- Start Series
                    date_trunc('hour', NOW()),                  -- End Series    
                    '1 hour'                                    -- Interval Length   
                    ) as hour 
            from        dune_user_generated.indexcoop_issuance_events
            group by    symbol
            order by    symbol, hour
            ) hat
left join   (select
                date_trunc('hour', evt_block_time) as hour,
                symbol,
                case
                    when evt_type = 'Issue' then amount
                    else 0
                end as issued_amount,
                case
                    when evt_type = 'Redeem' then amount
                    else 0
                end as redeemed_amount
            from dune_user_generated.indexcoop_issuance_events
            group by 1,2,3,4
            ) s on hat.hour = s.hour and hat.symbol = s.symbol

