-- also documented in notion
-- https://dune.xyz/queries/317169

CREATE OR REPLACE VIEW
dune_user_generated."indexcoop_revenue_daily" as

select
    a.day,
    a.symbol,
    b.methodologist,
    b.methodologist_split,
    a.aum * (b.streaming_fee/365) as streaming_revenue,
    (a.issued_volume * b.issue_fee) + (a.redeemed_volume * b.redeem_fee) as issue_redeem_revenue,
    (a.aum * (b.streaming_fee/365)) + (a.issued_volume * b.issue_fee) + (a.redeemed_volume * b.redeem_fee) as total_revenue,
    ((a.aum * (b.streaming_fee/365)) + (a.issued_volume * b.issue_fee) + (a.redeemed_volume * b.redeem_fee)) * (1 - b.methodologist_split) as net_revenue,
    ((a.aum * (b.streaming_fee/365)) + (a.issued_volume * b.issue_fee) + (a.redeemed_volume * b.redeem_fee)) * b.methodologist_split as methodologist_fee
from        (select
                a.day,
                a.symbol,
                coalesce(a.supply * b.price, 0) as aum,
                coalesce(a.issued_amount * b.price, 0) as issued_volume,
                coalesce(a.redeemed_amount * b.price, 0) as redeemed_volume
            from        dune_user_generated."indexcoop_issuance_daily" a
            left join   (select distinct
                            date_trunc('day', hour) as day,
                            symbol,
                            percentile_cont(.5) within group (order by median_price) as price
                        from        prices."prices_from_dex_data"
                        where       contract_address in (select token_address from dune_user_generated.indexcoop_tokens)
                        group by    day, symbol
                        ) b on a.day = b.day and a.symbol = b.symbol
            ) a
left join   dune_user_generated."indexcoop_fee_structure" b on a.symbol = b.symbol and a.day >= b.begin_date and a.day <= b.end_date