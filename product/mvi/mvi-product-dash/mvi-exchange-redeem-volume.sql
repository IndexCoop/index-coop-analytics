-- https://duneanalytics.com/queries/42188

SELECT 
    date_trunc('day', block_time) as "date",
    sum(coalesce(p.price, pweth.price) / POWER(10, coalesce(p.decimals, pweth.decimals)) * bytea2numeric(substring(DATA, 49, 50))) as volume
FROM ethereum.logs
LEFT JOIN prices.usd p
    ON p.minute = date_trunc('minute', block_time) and substring(topic4, 13, 20) = p.contract_address
LEFT JOIN prices.usd pweth
    ON pweth.minute = date_trunc('minute', block_time) and pweth.symbol = 'WETH'
WHERE ethereum.logs.contract_address = '\xc8C85A3b4d03FB3451e7248Ff94F780c92F884fD'
  AND topic1 = '\x9f8f1a845f52c0a7086f16f355a092a5acf0cca219c831409b9e5b1c39af9f9f'
  AND substring(topic3, 13, 20) = '\x72e364f2abdc788b7e918bc238b21f109cd634d7'
GROUP BY 1 ORDER BY 1