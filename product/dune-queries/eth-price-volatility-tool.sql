-- https://dune.xyz/queries/481002

/*
Input a list of token addresses into the parameter separated by commas
Spaces and 0x is handled by the parsing
Trades are against ETH/WETH as a pair
*/

with parsed_addresses as (
select replace(lower(trim(address)),'0x','\x')::bytea as address
from unnest(string_to_array('{{token_addresses}}',',')) address
)
, bidirectional_trades as (
SELECT date_trunc('day',block_time) as date_utc
    , pa.address
    , token_a_amount as token_1_amt
    , token_b_amount as token_2_amt
FROM dex."trades" t
inner join parsed_addresses pa on t.token_a_address = pa.address
WHERE block_time >= now() - interval '12 months'
and token_b_address in ('\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', -- WETH
        '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -- ETH
        )
union all
SELECT date_trunc('day',block_time) as date_utc
    , pa.address
    , token_b_amount as token_1_amt
    , token_a_amount as token_2_amt
FROM dex."trades" t
inner join parsed_addresses pa on t.token_b_address = pa.address
WHERE block_time >= now() - interval '12 months'
and token_a_address in ('\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', -- WETH
        '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -- ETH
        )
)
select date_utc
    , address as token_address
    , count(*) as num_trades
    , min(token_2_amt/nullif(token_1_amt,0)) as min_price
    , avg(token_2_amt/nullif(token_1_amt,0)) as avg_trade_price
    , sum(token_2_amt)/nullif(sum(token_1_amt),0) as vwa_trade_price -- volume weighted price
    , max(token_2_amt/nullif(token_1_amt,0)) as max_price
    , stddev(token_2_amt/nullif(token_1_amt,0)) as sd_price
    , avg(token_2_amt/nullif(token_1_amt,0)) - stddev(token_2_amt/nullif(token_1_amt,0)) as "1sd_below_avg"
    , avg(token_2_amt/nullif(token_1_amt,0)) + stddev(token_2_amt/nullif(token_1_amt,0)) as "1sd_above_avg"
from bidirectional_trades
group by 1,2
order by 2, 1 desc
; 
