--https://dune.xyz/queries/90330

drop table if exists dune_user_generated.holders_ever_had_250dpi;
create table dune_user_generated.holders_ever_had_250dpi as
select
    holder,
    min(days) as days 
from dune_user_generated.dpi_balance_by_day
where dpi_balance >= 250
group by holder
;
--select * from holders_ever_had_250dpi;
drop table if exists dune_user_generated.holders_ever_had_250dpi_now;
create table dune_user_generated.holders_ever_had_250dpi_now as

select
    holder,
    max(days) as days
from dune_user_generated.dpi_balance_by_day
where holder in (select holder from dune_user_generated.holders_ever_had_250dpi)
group by holder
;

drop table if exists dune_user_generated.holder_dpi_change;
create table dune_user_generated.holder_dpi_change as
select 
    *,
    count(*) over() as holder_had_250dpi_ever,
    sum(case when end_balance_dpi >= 250 then 1 else 0 end) over() as holders_still_have_250dpi,
    sum(case when end_balance_dpi > start_balance_dpi then 1 else 0 end) over() as adding_number
from (
select
    a.holder,
    a.days as start_date,
    c.dpi_balance as start_balance_dpi,
    b.days as end_date,
    d.dpi_balance as end_balance_dpi
from dune_user_generated.holders_ever_had_250dpi a 
left join dune_user_generated.holders_ever_had_250dpi_now b
  on a.holder = b.holder 
left join dune_user_generated.dpi_balance_by_day c
  on a.holder = c.holder and a.days = c.days
left join dune_user_generated.dpi_balance_by_day d
  on b.holder = d.holder and b.days = d.days
) x
;

select * from dune_user_generated.holder_dpi_change;