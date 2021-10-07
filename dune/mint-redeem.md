# Mint / Redeem

When minting new supply of a product, the minter must own the underlying assets of the product. The minter may source the required collateral from DEXs via Exchange Issuance. Note, that exchange issuance is just an additional step. If a minter uses exchange issuance, the mint will still be an event in the Basic Issuance contract.

## Useful Tables

**Simple Index - Mint**

`setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"`

**Simple Index - Redeem**

`setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"`

**Simple Index \(Exchange Issuance\) - Mint**

`setprotocol_v2."ExchangeIssuance_evt_ExchangeIssue"`

**Simple Index \(Exchange Issuance\) - Redeem**

`setprotocol_v2."ExchangeIssuance_evt_ExchangeRedeem"`

**Flexible Leverage Index - Mint**

`setprotocol_v2."DebtIssuanceModule_evt_SetTokenIssued"`

**Flexible Leverage Index - Redeem**

`setprotocol_v2."DebtIssuanceModule_evt_SetTokenRedeemed"`

## Mint / Redeem Example

```sql
dpi_daily_minted_units AS (
SELECT 
        date_trunc('day', evt_block_time) AS day, 
        "_setToken" AS token_address,
        SUM("_quantity"/1e18) AS amount
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenIssued"
    WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    GROUP BY 1,2
)
```

\*\*\*\*

```sql
dpi_daily_redeemed_units AS (
SELECT 
        date_trunc('day', evt_block_time) AS day, 
        "_setToken" AS token_address,
        SUM("_quantity"/1e18) AS amount
    FROM setprotocol_v2."BasicIssuanceModule_evt_SetTokenRedeemed"
    WHERE "_setToken" = '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'
    GROUP BY 1,2
)
```

