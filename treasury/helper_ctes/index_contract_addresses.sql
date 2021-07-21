

-- , index_contract_addresses as (

    select '\x154c154c589b4aeccbf186fb8bc668cd7c213762'::bytea as address
        , 'Centralised Exchange Listing' as address_alias
    union all
    select '\xe83de75eb3e84f3cbca3576351d81dbeda5645d4'::bytea as address
        , 'Analytics Working Group' as address_alias
    union all
    select '\xd4bcc2b5d21fe67c8be351cdb47ec1b2cd7e84a7'::bytea as address
        , 'Growth Working Group' as address_alias
    union all
    select '\x0dea6d942a2d8f594844f973366859616dd5ea50'::bytea as address
        , 'DPI Manager' as address_alias
    union all
    select '\x25100726b25a6ddb8f8e68988272e1883733966e'::bytea as address
        , 'DPI Rebalancer' as address_alias
    union all
    select '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'::bytea as address
        , 'ETH2x-FLI Token' as address_alias
    union all
    select '\x445307De5279cD4B1BcBf38853f81b190A806075'::bytea as address
        , 'ETH2x-FLI Manager' as address_alias
    union all
    select '\x1335D01a4B572C37f800f45D9a4b36A53a898a9b'::bytea as address
        , 'ETH2x-FLI Strategy Adapter' as address_alias
    union all
    select '\x26F81381018543eCa9353bd081387F68fAE15CeD'::bytea as address
        , 'ETH2x-FLI Fee Adapter' as address_alias
    union all
    select '\x0F1171C24B06ADed18d2d23178019A3B256401D3'::bytea as address
        , 'ETH2x-FLI SupplyCapIssuanceHook' as address_alias
    union all
    select '\x0b498ff89709d3838a063f1dfa463091f9801c2b'::bytea as address
        , 'BTC2x-FLI Token' as address_alias
    union all
    select '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'::bytea as address
        , 'BTC2x-FLI Manager' as address_alias
    union all
    select '\x4a99733458349505A6FCbcF6CD0a0eD18666586A'::bytea as address
        , 'BTC2x-FLI Strategy Adapter' as address_alias
    union all
    select '\xA0D95095577ecDd23C8b4c9eD0421dAc3c1DaF87'::bytea as address
        , 'BTC2x-FLI Fee Adapter' as address_alias
    union all
    select '\x6c8137f2f552f569cc43bc4642afbe052a12441c'::bytea as address
        , 'BTC2x-FLI SupplyCapAllowedCallerIssuanceHook' as address_alias
    union all
    select '\x0954906da0Bf32d5479e25f46056d22f08464cab'::bytea as address
        , 'INDEX Token Address' as address_alias
    union all
    select '\xDD111F0fc07F4D89ED6ff96DBAB19a61450b8435'::bytea as address
        , 'INDEX Initial Airdrop Address' as address_alias
    union all
    select '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'::bytea as address
        , 'INDEX DPI Farming Contract 1 (Oct - Dec)' as address_alias
    union all
    select '\xB93b505Ed567982E2b6756177ddD23ab5745f309'::bytea as address
        , 'INDEX DPI Farming Contract 2 (Dec. 2020 - March 2021)' as address_alias
    union all
    select '\x66a7d781828B03Ee1Ae678Cd3Fe2D595ba3B6000'::bytea as address
        , 'Index Methodologist Bounty (18 months vesting)' as address_alias
    union all
    select '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea as address
        , 'Community Treasury Year 1 Vesting' as address_alias
    union all
    select '\xd89C642e52bD9c72bCC0778bCf4dE307cc48e75A'::bytea as address
        , 'Community Treasury Year 2 Vesting' as address_alias
    union all
    select '\x71F2b246F270c6AF49e2e514cA9F362B491Fbbe1'::bytea as address
        , 'Community Treasury Year 3 Vesting' as address_alias
    union all
    select '\xf64d061106054Fe63B0Aca68916266182E77e9bc'::bytea as address
        , 'Set Labs Year 1 Vesting' as address_alias
    union all
    select '\x4c11dfd35a4fe079b41d5d9729ed34c00d487712'::bytea as address
        , 'Set Labs Year 2 Vesting' as address_alias
    union all
    select '\x0D627ca04A97219F182DaB0Dc2a23FB4a5B02A9D'::bytea as address
        , 'Set Labs Year 3 Vesting' as address_alias
    union all
    select '\x5c29aa6761803bcfda7f683eaa0ff9bddda3649d'::bytea as address
        , 'Defi Pulse Year 1 Vesting' as address_alias
    union all
    select '\xce3c6312385fcf233ab0de574b0cb1a588566c3f'::bytea as address
        , 'Defi Pulse Year 2 Vesting' as address_alias
    union all
    select '\x0f58793e8cf39d6b60919ffaf773a7f95a568146'::bytea as address
        , 'Defi Pulse Year 3 Vesting' as address_alias
    union all
    select '\x319b852cd28b1cbeb029a3017e787b98e62fd4e2'::bytea as address
        , 'January 2021 Merkle Rewards Account' as address_alias
    union all
    select '\xeb1cbc809b21dddc71f0f9edc234eee6fb29acee'::bytea as address
        , 'December 2020 Merkle Rewards Account' as address_alias
    union all
    select '\x209f012602669c88bbda687fbbfe6a0d67477a5d'::bytea as address
        , 	'October 2020 Merkle Rewards Account' as address_alias
    union all
    select '\xa6bb7b6b2c5c3477f20686b98ea09796f8f93184'::bytea as address
        ,	'November 2020 Merkle Rewards Account' as address_alias
    union all
    select '\xCa3C3570beb35E5d3D85BCd8ad8F88BefaccFF10'::bytea as address
        , 'February 2021 Merkle Rewards Account' as address_alias
    union all
    select '\xa87fbb413f8de11e47037c5d697cc03de29e4e4b'::bytea as address
        , 'March 2021 Merkle Rewards Account' as address_alias
    union all
    select '\x973a526a633313b2d32b9a96ed16e212303d6905'::bytea as address
        ,	'April 2021 Merkle Rewards Account' as address_alias
    union all
    select '\x10F87409E405c5e44e581A4C3F2eECF36AAf1f92'::bytea as address
        , 'INDEX Sale 2 of 3 Multisig - Dylan, Greg, Punia' as address_alias
    



-- _