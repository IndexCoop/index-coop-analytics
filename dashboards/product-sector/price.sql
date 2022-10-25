-- https://dune.com/queries/1046446
/*

This query gets the daily closing price of Index Coop Sector Products using the median price from trades on DEXs in the last
hour of each day in the range.

*/
select
    day as "Day",
    price as "Price"
from    dune_user_generated.indexcoop_prices_daily 
where   symbol = '{{Index Coop Sector Token:}}'
and     day > date_trunc('day', least('{{End Date:}}', now())) - interval '{{Trailing Days:}} days'
and     day <= date_trunc('day', least('{{End Date:}}', now()))
order by    day desc
