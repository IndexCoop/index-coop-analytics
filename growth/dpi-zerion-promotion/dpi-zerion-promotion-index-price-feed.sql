-- https://duneanalytics.com/queries/38284

SELECT
    symbol,
    date_trunc('hour', minute) AS hour,
    AVG(price) AS price
FROM prices.usd
WHERE symbol = 'INDEX'
    AND minute >= '2021-04-13 16:00'
    AND minute <= '2021-04-16 04:00'
GROUP BY 1, 2
ORDER BY 2