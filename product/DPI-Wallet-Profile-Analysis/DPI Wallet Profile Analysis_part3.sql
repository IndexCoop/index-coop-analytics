--https://duneanalytics.com/queries/92099

drop table if exists dune_user_generated.loren_token_balance;
create table dune_user_generated.loren_token_balance as 
with dpi_250plus_holder as 
(
select
    holder
from dune_user_generated.holder_dpi_change
where end_balance_dpi >= 250 
),

relevent_token_holders as 
(
select 
    "from" as holder,
    "contract_address" as tokens,
    date_trunc('day',"evt_block_time") as days, 
    -"value" as amount
from erc20."ERC20_evt_Transfer"
where "contract_address" in (
    '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
    '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984',
    '\x0954906da0Bf32d5479e25f46056d22f08464cab',
    '\x72e364f2abdc788b7e918bc238b21f109cd634d7',
    '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd',
    '\xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c',
    '\xbb0e17ef65f82ab018d8edd776e8dd940327b28b',
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b',
    '\xdac17f958d2ee523a2206206994597c13d831ec7',
    '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    '\x6b175474e89094c44da98b954eedeac495271d0f'
   )
and "from" in (select holder from dpi_250plus_holder)


union all

select 
    "to" as holder,
    "contract_address" as tokens,
    date_trunc('day',"evt_block_time") as days, 
    "value" as amount
from erc20."ERC20_evt_Transfer"
where "contract_address" in (
    '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
    '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984',
    '\x0954906da0Bf32d5479e25f46056d22f08464cab',
    '\x72e364f2abdc788b7e918bc238b21f109cd634d7',
    '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd',
    '\xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c',
    '\xbb0e17ef65f82ab018d8edd776e8dd940327b28b',
    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b',
    '\xdac17f958d2ee523a2206206994597c13d831ec7',
    '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    '\x6b175474e89094c44da98b954eedeac495271d0f'
    )
and "to" in (select holder from dpi_250plus_holder)
),

token_balance_1 as 
(
select 
    holder,
    tokens,
    sum(amount) as amount
from relevent_token_holders
where holder not in ('\x0000000000000000000000000000000000000000')
group by holder,tokens
),

token_balance_2 as 
(
select
    holder,
    --tokens,
    symbol as tokens,
   round((amount/10^decimals)::numeric,2) as amount
from token_balance_1 a 
left join erc20."tokens" b
on a.tokens = b."contract_address"
),

address_labels as 
(
select
    address,
    string_agg("name", '| ') as label
from (
select
    distinct 
    address,
    "name"

from labels."labels"
where "address" in (select distinct holder from token_balance_2)
    ) x
  --and type not in ('dapp usage','activity')
group by address
)
,
token_balance as
(
select
    a.tokens,
    b.holder,
    d.label,
    coalesce(c.amount, 0)::int as amount
from (select distinct tokens from token_balance_2 ) a 
cross join (select distinct holder from token_balance_2) b
left join token_balance_2 c
on c.tokens = a.tokens and c.holder = b.holder
left join address_labels d
on d."address" = b.holder
)
select * from token_balance;

drop table if exists dune_user_generated.loren_250plus_dpi_balance;
create table dune_user_generated.loren_250plus_dpi_balance as 
select 
    holder,
    "DPI",
    "UNI",
    "AAVE", 
    "AXS", 
    "ENJ", 
    "ETH2x-FLI", 
    "INDEX", 
    "MVI", 
    "USDT",
    "USDC",
    "DAI",
    label 
from crosstab ( 'select holder, label, tokens, amount from dune_user_generated.loren_token_balance ORDER BY 1, 2',
                'select DISTINCT tokens from dune_user_generated.loren_token_balance ORDER BY 1' ) 
            as ( holder bytea, label text, "AAVE" numeric, "AXS" numeric, "DAI" numeric, "DPI" numeric, 
                "ENJ" numeric, "ETH2x-FLI" numeric, "INDEX" numeric, "MVI" numeric,
                "UNI" numeric, "USDC" numeric, "USDT" numeric 
                );


select
    (select count(distinct holder) from dune_user_generated.loren_token_balance) as holder_number_with_250DPI,
    tokens,
    count(*) as holder_number,
    sum(amount) as total_balance,
    percentile_cont(0.5) within group(order by amount)::int as median_balance,
    avg(amount)::int as avg_balance,
    max(amount) as max_balance,
    min(amount) as min_balance
from dune_user_generated.loren_token_balance
where amount > 0 
  and tokens not in ('DPI','USDT','USDC','DAI')
group by tokens

union all


select
    (select count(distinct holder) from dune_user_generated.loren_token_balance) as holder_number_with_250DPI,
    tokens,
    count(*) as holder_number,
    sum(amount) as total_balance,
    percentile_cont(0.5) within group(order by amount)::int as median_balance,
    avg(amount)::int as avg_balance,
    max(amount) as max_balance,
    min(amount) as min_balance
from (
select
    holder,
    'StableCoin' as tokens,
    sum(amount) as amount
from dune_user_generated.loren_token_balance
where amount > 0 
  and tokens  in ('USDT','USDC','DAI')
group by holder
) x
group by tokens;
;

