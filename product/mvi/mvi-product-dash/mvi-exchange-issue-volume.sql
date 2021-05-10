-- https://duneanalytics.com/queries/42187

SELECT date_trunc('day', block_time) as "date",
       sum(coalesce(p.price, pweth.price) / POWER(10, coalesce(p.decimals, pweth.decimals)) * bytea2numeric(substring(DATA, 13, 20))) as volume
FROM ethereum.logs
LEFT JOIN prices.usd p
    ON p.minute = date_trunc('minute', block_time) and substring(topic4, 13, 20) = p.contract_address
LEFT JOIN prices.usd pweth
    ON pweth.minute = date_trunc('minute', block_time) and pweth.symbol = 'WETH'
WHERE ethereum.logs.contract_address = '\xc8C85A3b4d03FB3451e7248Ff94F780c92F884fD'
  AND topic1 = '\x44b3b16472a909f781f712646232271ffd156fff642d4895b700146a40462601'
  AND substring(topic3, 13, 20) = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
group by 1 order by 1