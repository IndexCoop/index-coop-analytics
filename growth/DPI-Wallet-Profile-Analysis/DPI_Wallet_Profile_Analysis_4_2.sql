--https://dune.xyz/queries/126279

with lp_position as
(
SELECT 
    holder,
    coalesce(sushi_lp, 0) as sushi_lp,
     coalesce(uni_v2_lp, 0) as uni_v2_lp,
     coalesce(uni_v3_lp, 0) as uni_v3_lp
FROM crosstab ( 
'SELECT holder, types, lp_position FROM dune_user_generated.dpi_lp_position ORDER BY 1, 2',
'SELECT DISTINCT types FROM dune_user_generated.dpi_lp_position ORDER BY 1' 
) AS ( holder bytea, sushi_lp numeric, uni_v2_lp numeric, uni_v3_lp numeric )
),
wallet_balance as
(
select
    holder,
    dpi_balance::int as wallet
from dune_user_generated.dpi_balance_by_day
where rnk = 1 
)

select
    -- count(*),
    -- count(distinct a.holder)
    a.holder,
    (coalesce(wallet, 0) + sushi_lp + uni_v2_lp + uni_v3_lp) as dpi_balance,
    coalesce(wallet, 0) as dpi_in_wallet, 
    ( sushi_lp + uni_v2_lp + uni_v3_lp) as dpi_in_lp,
    sushi_lp,
    uni_v2_lp,
    uni_v3_lp
from lp_position a 
left join wallet_balance b
on a.holder = b.holder 
order by (coalesce(wallet, 0) + sushi_lp + uni_v2_lp + uni_v3_lp) desc ;

