-- https://dune.xyz/queries/359340
CREATE OR REPLACE VIEW 
dune_user_generated.indexcoop_prices as

select
    b.symbol,
    a.hour,
    a.median_price as price
from        prices.prices_from_dex_data a
left join   dune_user_generated.indexcoop_tokens b on a.contract_address = b.token_address
where       a.contract_address in (select token_address from dune_user_generated.indexcoop_tokens)
