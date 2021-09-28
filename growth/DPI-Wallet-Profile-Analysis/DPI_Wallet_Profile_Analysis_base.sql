--https://dune.xyz/queries/90319

drop table if exists dune_user_generated.dpi_holders;
create table  dune_user_generated.dpi_holders as 
select 
    "from" as holder,
   date_trunc('day',"evt_block_time") as days, 
    -"value"/1e18 as amount
from erc20."ERC20_evt_Transfer"
where "contract_address" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
   

union all

select 
    "to" as holder,
     date_trunc('day',"evt_block_time") as days, 
    "value"/1e18 as amount
from erc20."ERC20_evt_Transfer"
where "contract_address" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' 
;

drop table if exists dune_user_generated.dpi_balance_change_by_day;
create table  dune_user_generated.dpi_balance_change_by_day as
select 
    holder,
    days,
    'DPI' as token,
    sum(amount) as amount
from dune_user_generated.dpi_holders
where holder not in ('\x0000000000000000000000000000000000000000')
group by holder,days
;

drop table if exists dune_user_generated.dpi_balance_by_day;
create table dune_user_generated.dpi_balance_by_day as
select 
    *,
    sum(amount) over(partition by holder order by days rows between unbounded preceding and current row) as dpi_balance,
    row_number() over( partition by holder order by days desc ) as rnk
from dune_user_generated.dpi_balance_change_by_day

;

select
    holder,
    last_transaction_day,
    dpi_balance::int,
    address_ever_had_dpi,
    address_has_1plus_dpi,
    total_dpi_issued::int,
    avg(dpi_balance) over()::int as avg_dpi_balance,
    (select percentile_cont(0.5) within group(order by dpi_balance)::int as median_dpi_balance from dune_user_generated.dpi_balance_by_day where dpi_balance >= 1)
from (
select
    holder,
    days as last_transaction_day,
    dpi_balance,
    count(*) over() as address_ever_had_dpi,
    sum(case when dpi_balance >= 1 then 1 else 0 end) over() as address_has_1plus_dpi,
    sum(dpi_balance) over() as total_dpi_issued
from dune_user_generated.dpi_balance_by_day
where rnk = 1
) x
where dpi_balance >= 1
order by dpi_balance desc;