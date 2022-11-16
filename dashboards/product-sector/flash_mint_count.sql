-- https://dune.com/queries/1110603
select 
    count(event) as "Flash Mint Count" 
from    dune_user_generated.indexcoop_flash_events 
where   symbol = '{{Index Coop Sector Token:}}'
and     event = 'Issue'
and     block_time 
        between date_trunc('day', least('{{End Date:}}', now())) - interval '{{Trailing Days:}} days' 
        and     date_trunc('day', least('{{End Date:}}', now()))
