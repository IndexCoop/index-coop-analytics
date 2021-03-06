-- https://dune.xyz/queries/298614
CREATE TABLE if not exists dune_user_generated.indexcoop_tokens
    (
      symbol varchar,        
      name varchar,                               
      index_type varchar,             
      issuance_model varchar, 
      issuance_chain varchar, 
      token_address bytea
      )
;

truncate table dune_user_generated.indexcoop_tokens;

insert into dune_user_generated.indexcoop_tokens 
(symbol,        name,                               index_type,             issuance_model, issuance_chain, token_address) values
('DPI',         'DeFi Pulse Index',                 'Composite',            'Standard',     'Ethereum',     '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea),
('MVI',         'Metaverse Index',                  'Composite',            'Standard',     'Ethereum',     '\x72e364f2abdc788b7e918bc238b21f109cd634d7'::bytea),
('BED',         'Bankless BED Index',               'Composite',            'Standard',     'Ethereum',     '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'::bytea),
('DATA',        'Data Economy Index',               'Composite',            'Standard',     'Ethereum',     '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'::bytea),
('ETH2x-FLI',   'ETH 2x Flexible Leverage Index',   'Flexible Leverage',    'Debt',         'Ethereum',     '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'::bytea),
('BTC2x-FLI',   'BTC 2x Flexible Leverage Index',   'Flexible Leverage',    'Debt',         'Ethereum',     '\x0b498ff89709d3838a063f1dfa463091f9801c2b'::bytea),
('GMI',         'Bankless DeFi Innovation Index',   'Composite',            'Debt',         'Ethereum',     '\x47110d43175f7f2c2425e7d15792acc5817eb44f'::bytea),
('icETH',       'Interest Compounding ETH',         'Flexible Leverage',    'Debt',         'Ethereum',     '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'::bytea)
;
