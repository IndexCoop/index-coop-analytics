-- https://dune.com/queries/1046446

select
    day as "Day",
    price as "Price"
from    (
        select
            date_trunc('day', hour) as day,
            median_price as price,
            row_number() over (partition by date_trunc('day', hour) order by hour desc) as rnb
        from    prices.prices_from_dex_data
        where   contract_address = (select token_address from dune_user_generated."indexcoop_tokens" where symbol = '{{Index Coop Sector Token:}}')
        and     date_trunc('day', hour) > date_trunc('day', least('{{End Date:}}', now())) - interval '{{Trailing Days:}} days'
        and     date_trunc('day', hour) <= date_trunc('day', least('{{End Date:}}', now()))
        ) t
where       t.rnb = 1
order by    day desc
