--https://dune.xyz/queries/129548
with promotion_users_bought as
(
    select 
         "to" as holder,
         sum("value"/1e18)::int as bought_amount
    from erc20."ERC20_evt_Transfer"
    where --"to" in (select address from promotion_rewards) and
    "from" = '\xd291328a6c202c5b18dcb24f279f69de1e065f70'
    and "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" >= '2021-04-13 00:00:00' 
    and "evt_block_time" < '2021-04-22 00:00:00'
    group by "to"
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
    where  "contract_address"='\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    and "evt_block_time" >= '2021-04-22 00:00:00'
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
    round((6334-cumurative_out_real)/6334,2) as retention_rate
from trans_out_by_day_temp2

union all

select
    '2021-04-21 00:00' as days,
    1.00 as retention_rate
    
;

