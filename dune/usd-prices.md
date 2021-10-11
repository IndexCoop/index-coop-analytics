# USD Prices

## Useful Tables

**Prices - ERC20 Tokens**

`prices.usd` _\*\*_

Note: This table does not include all prices for all tokens. You may need to supplement this table with prices from `dex.trades`

**Prices - Layer 1s**

`prices.layer1_usd`

## DEX Trades Example

Here is an example of getting the average hourly market price for DPI using `dex.trades` :

**Get the Average Hourly DPI/WETH Price**

```sql
dpi_eth_price_hourly AS (
SELECT
    date_trunc('hour', block_time) AS hour,
    (SUM(token_b_amount)/1e18) / (SUM(token_a_amount)/1e18) AS dpi_weth
FROM dex.trades
WHERE token_a_address = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b' -- DPI
AND token_b_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
GROUP BY 1
)
```

**Get the Hourly ETH/USD Price**

```sql
eth_usd_price_hourly AS (
SELECT
    date_trunc('hour', minute) AS hour,
    AVG(price) AS eth_usd
FROM prices.layer1_usd
WHERE symbol = 'ETH'
AND minute >= (SELECT MIN(hour) FROM dpi_eth_price_hourly)
GROUP BY 1
)
```

**Use DPI/WETH and ETH/USD to Find Hourly DPI/USD Price**

```sql
dpi_usd_price_hourly AS (
SELECT
    a.hour,
    a.dpi_weth * b.eth_usd AS dpi_usd
FROM dpi_eth_price_hourly a
LEFT JOIN eth_usd_price_hourly b ON a.hour = b.hour
)
```
