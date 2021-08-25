CREATE OR REPLACE view dune_user_generated.index_nonvoting_addresses (wallet_address) as values 
('\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'::bytea)-- 1st DPI LM Rewards
,('\xB93b505Ed567982E2b6756177ddD23ab5745f309'::bytea) -- 2nd DPI LM Rewards
,('\x56d68212113AC6cF012B82BD2da0E972091940Eb'::bytea) -- ETHFLI LM Rewards (not active yet)
,('\x5bC4249641B4bf4E37EF513F3Fa5C63ECAB34881'::bytea) -- 2nd MVI LM Rewards
,('\xa73df646512c82550c2b3c0324c4eedee53b400c'::bytea) -- INDEX on Sushiswap
,('\x3452a7f30a712e415a0674c0341d44ee9d9786f9'::bytea) -- INDEX on Uniswap_v2
,('\x8c13148228765Ba9E84EAf940b0416a5e349A5e7'::bytea) -- INDEX on Uniswap_v3 
,('\xd3d555bb655acba9452bfc6d7cea8cc7b3628c55'::bytea) --- Set: rebalancer TBD if this needs to be included
,('\x9467cfadc9de245010df95ec6a585a506a8ad5fc'::bytea) -- INDEX Treasury
,('\x66a7d781828b03ee1ae678cd3fe2d595ba3b6000'::bytea) -- Index Methodologist Vesting
,('\xdd111f0fc07f4d89ed6ff96dbab19a61450b8435'::bytea) -- Early Community Rewards Vesting
,('\x8f06fba4684b5e0988f215a47775bb611af0f986'::bytea) -- To Initial Liquidity Mining Vesting
,('\x26e316f5b3819264df013ccf47989fb8c891b088'::bytea) -- To Index Community Treasury 1 Year 
,('\xd89c642e52bd9c72bcc0778bcf4de307cc48e75a'::bytea) -- To Index Community Treasury Year 2
,('\x71f2b246f270c6af49e2e514ca9f362b491fbbe1'::bytea) -- To Index Community Treasury Year 3
,('\xf64d061106054fe63b0aca68916266182e77e9bc'::bytea) -- To Set Labs Year 1 Vesting
,('\x4c11dfd35a4fe079b41d5d9729ed34c00d487712'::bytea) -- To Set Lab Year 2 Vesting
,('\x0d627ca04a97219f182dab0dc2a23fb4a5b02a9d'::bytea) -- To Set Lab Year 3 Vesting
,('\x5c29aa6761803bcfda7f683eaa0ff9bddda3649d'::bytea) -- To DeFi Pulse Year 1 Vesting
,('\xce3c6312385fcf233ab0de574b0cb1a588566c3f'::bytea) -- To DeFi Pulse Year 2 Vesting
,('\x0f58793e8cf39d6b60919ffaf773a7f95a568146'::bytea) -- To DeFi Pulse Year 3 Vesting
,('\x673d140eed36385cb784e279f8759f495c97cf03'::bytea) -- DPI - Methodologist Account
