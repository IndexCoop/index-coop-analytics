CREATE OR REPLACE view dune_user_generated.indexcoop_index_supply AS

(
with

total_supply as (
select
    d.day,
    sum(s.amount) over (order by d.day) as total_supply
from        (select generate_series('2020-10-06', current_date, '1 day') as day) d
left join   (
            SELECT 
                date_trunc('day',"evt_block_time") AS day,
                sum(value/1e18) AS amount
            FROM        erc20."ERC20_evt_Transfer"
            WHERE       contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
            AND         "from" = '\x0000000000000000000000000000000000000000'
            GROUP BY    day
            
            union
            
            SELECT 
                date_trunc('day',"evt_block_time") AS day,
                -SUM(value/1e18) AS amount
            FROM        erc20."ERC20_evt_Transfer"
            WHERE       contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
            AND         "to" in ('\x000000000000000000000000000000000000dead', '\x0000000000000000000000000000000000000000') 
            GROUP BY    1
            ) s on s.day = d.day
),

circulating_supply as (
select
    d.day,
    sum(s.amount) over (order by d.day) as non_circulating_supply
from        (select generate_series('2020-10-06', current_date, '1 day') as day) d
left join   (
            SELECT 
                date_trunc('day',"evt_block_time") AS day,
                sum(value/1e18) AS amount
            FROM        erc20."ERC20_evt_Transfer" t
            inner join  (select address from dune_user_generated.indexcoop_address_book where circulating = false) s on s.address = t."to"
            WHERE       t.contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
            GROUP BY    1
            
            union
            
            SELECT 
                date_trunc('day',"evt_block_time") AS day,
                -sum(value/1e18) AS amount
            FROM        erc20."ERC20_evt_Transfer" t
            inner join  (select address from dune_user_generated.indexcoop_address_book where circulating = false) s on s.address = t."from"
            WHERE       t.contract_address = '\x0954906da0Bf32d5479e25f46056d22f08464cab'
            GROUP BY    1
            ) s on s.day = d.day
)

select
    ts.day,
    ts.total_supply,
    ts.total_supply - cs.non_circulating_supply as circulating_supply
from        total_supply ts
left join   circulating_supply cs on ts.day = cs.day
)
