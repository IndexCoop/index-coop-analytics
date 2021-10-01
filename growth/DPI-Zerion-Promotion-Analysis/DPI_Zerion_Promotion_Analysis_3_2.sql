--https://dune.xyz/queries/130302
with promotion_users_bought as
(
    select 
         "to" as holder,
         ("value"/1e18)::int as bought_amount
    from erc20."ERC20_evt_Transfer"
    where "evt_tx_hash" in (
    '\x005b48fa78bd822f630e3bac1322e48395328d9409276357b25cc1e2ae9ac184',
    '\x145190b126f2a44ccc5112a615db021b617d55f4893ccd559b34ddc04180d8ac',
    '\xaf14e7447800cf61df7b88d4ad4362c234685b4882332a702a8549fb3b0cb026'
    )
    and "from" != '\x69238af5756617e5218810057a03da509ec51fd4'
)
,
-- select * from promotion_users_bought;

dpi_transfer as 
(
    select
        holder,
        days,
        trans_out,
        sum(trans_out) over( partition by holder order by days rows between unbounded preceding and current row) as cumurative_out
    from (
    select 
         "from" as holder,
         date_trunc('day',"evt_block_time") as days,
        sum(("value"/1e18))::int as trans_out
    from erc20."ERC20_evt_Transfer"
    where  "contract_address"='\x0954906da0bf32d5479e25f46056d22f08464cab'
    and "evt_block_time" >= '2021-04-28 05:50:00'
    and "from" in (select holder from promotion_users_bought) 
    group by "from",date_trunc('day',"evt_block_time")
        ) x
    order by holder, days 
)
,
-- select * from dpi_transfer;
trans_out_by_day_temp1 as
(
select
    a.holder,
    days,
    bought_amount,
    trans_out,
    cumurative_out,
    case when cumurative_out <= bought_amount then trans_out
         else GREATEST(bought_amount-cumurative_out+trans_out,0)
    end as trans_out_real
from dpi_transfer a 
left join promotion_users_bought b
on a.holder = b.holder

)
,
-- select * from trans_out_by_day_temp1;
trans_out_by_day_temp2 as
(
select
    days,
    trans_out_real,
    sum(trans_out_real) over( order by days rows between unbounded preceding and current row) as cumurative_out_real
from (
select
    days,
    sum(trans_out_real) as trans_out_real
from trans_out_by_day_temp1
group by days
) x
)

-- select * from trans_out_by_day_temp2;
select 
    days,
    round((3114-cumurative_out_real)/3114,2) as retention_rate
from trans_out_by_day_temp2

union all

select
    '2021-04-27 00:00' as days,
    1.00 as retention_rate
    
;

