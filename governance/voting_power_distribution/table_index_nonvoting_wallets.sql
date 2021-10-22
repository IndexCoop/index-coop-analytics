CREATE OR REPLACE view dune_user_generated.index_addresses (wallet_address, wallet_type) as values 
('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'::bytea, 'Nonvoting'::text)-- 1st DPI LM Rewards
,('\xB93b505Ed567982E2b6756177ddD23ab5745f309'::bytea, 'Nonvoting'::text) -- 2nd DPI LM Rewards
,('\x56d68212113AC6cF012B82BD2da0E972091940Eb'::bytea, 'Nonvoting'::text) -- ETHFLI LM Rewards (not active yet)
,('\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'::bytea, 'Nonvoting'::text) -- 2nd MVI LM Rewards
,('\xa73df646512c82550c2b3c0324c4eedee53b400c'::bytea, 'Voting'::text) -- INDEX on Sushiswap, can actually vote via IndexPowah
,('\x3452a7f30a712e415a0674c0341d44ee9d9786f9'::bytea, 'Voting'::text) -- INDEX on Uniswap_v2, can actually vote via IndexPowah
,('\x8c13148228765Ba9E84EAf940b0416a5e349A5e7'::bytea, 'Voting'::text) -- INDEX on Uniswap_v3, can actually vote via IndexPowah
,('\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55'::bytea, 'Nonvoting'::text) --- Set: rebalancer TBD if this needs to be included
,('\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea, 'Nonvoting'::text) -- INDEX Treasury
,('\x66a7d781828b03ee1ae678cd3fe2d595ba3b6000'::bytea, 'Nonvoting'::text) -- Index Methodologist Vesting
,('\xdd111f0fc07f4d89ed6ff96dbab19a61450b8435'::bytea, 'Nonvoting'::text) -- Early Community Rewards Vesting
,('\x8f06fba4684b5e0988f215a47775bb611af0f986'::bytea, 'Nonvoting'::text) -- To Initial Liquidity Mining Vesting
,('\x26e316f5b3819264df013ccf47989fb8c891b088'::bytea, 'Nonvoting'::text) -- To Index Community Treasury 1 Year 
,('\xd89c642e52bd9c72bcc0778bcf4de307cc48e75a'::bytea, 'Nonvoting'::text) -- To Index Community Treasury Year 2
,('\x71f2b246f270c6af49e2e514ca9f362b491fbbe1'::bytea, 'Nonvoting'::text) -- To Index Community Treasury Year 3
,('\xf64d061106054fe63b0aca68916266182e77e9bc'::bytea, 'Nonvoting'::text) -- To Set Labs Year 1 Vesting
,('\x4c11dfd35a4fe079b41d5d9729ed34c00d487712'::bytea, 'Nonvoting'::text) -- To Set Lab Year 2 Vesting
,('\x0d627ca04a97219f182dab0dc2a23fb4a5b02a9d'::bytea, 'Nonvoting'::text) -- To Set Lab Year 3 Vesting
,('\x5c29aa6761803bcfda7f683eaa0ff9bddda3649d'::bytea, 'Nonvoting'::text) -- To DeFi Pulse Year 1 Vesting
,('\xce3c6312385fcf233ab0de574b0cb1a588566c3f'::bytea, 'Nonvoting'::text) -- To DeFi Pulse Year 2 Vesting
,('\x0f58793e8cf39d6b60919ffaf773a7f95a568146'::bytea, 'Nonvoting'::text) -- To DeFi Pulse Year 3 Vesting
,('\x673d140eed36385cb784e279f8759f495c97cf03'::bytea, 'Nonvoting'::text) -- DPI - Methodologist Account
,('\xcf19a7c81fcf0e01c927f28a2b551405e58c77e5'::bytea, 'Nonvoting'::text) -- Balancer Pool, cannot actually vote via IndexPowah
,('\x941604E66E72360691232b913b67632c26af424D'::bytea, 'Nonvoting'::text) -- FTC vesting contract, cannot actually vote via IndexPowah
,('\xF8BDFC9bB58dC0C4d2E77e04AAc673B99D09c7Cd'::bytea, 'Nonvoting'::text) -- FTC vesting contract, cannot actually vote via IndexPowah
,('\x5a2cAf985d71A281662E0925738e54123dA56c40'::bytea, 'Nonvoting'::text) -- FTC vesting contract, cannot actually vote via IndexPowah
,('\xc12d290ffA999dAA837F48df99E8Bc70c2dfc26d'::bytea, 'Nonvoting'::text) -- FTC vesting contract, cannot actually vote via IndexPowah
,('\x3cB23D1836052B20828c5D7DE80805C2c2782de6'::bytea, 'Nonvoting'::text) -- FTC vesting contract, cannot actually vote via IndexPowah
,('\x40D4fD107B3D4ebe51b29252458631397462b016'::bytea, 'Nonvoting'::text) -- FTC vesting contract, cannot actually vote via IndexPowah
