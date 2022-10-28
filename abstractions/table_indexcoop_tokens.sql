-- https://dune.xyz/queries/298614
-- drop table if exists dune_user_generated.indexcoop_tokens cascade ;
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
(symbol,        name,                               index_type,     token_address) values
('DPI',         'DeFi Pulse Index',                 'Composite',    '\x1494ca1f11d487c2bbe4543e90080aeba4ba3c2b'::bytea),
('MVI',         'Metaverse Index',                  'Composite',    '\x72e364f2abdc788b7e918bc238b21f109cd634d7'::bytea),
('BED',         'Bankless BED Index',               'Composite',    '\x2af1df3ab0ab157e1e2ad8f88a7d04fbea0c7dc6'::bytea),
('DATA',        'Data Economy Index',               'Composite',    '\x33d63ba1e57e54779f7ddaeaa7109349344cf5f1'::bytea),
('ETH2x-FLI',   'ETH 2x Flexible Leverage Index',   'Leverage',     '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'::bytea),
('BTC2x-FLI',   'BTC 2x Flexible Leverage Index',   'Leverage',     '\x0b498ff89709d3838a063f1dfa463091f9801c2b'::bytea),
('GMI',         'Bankless DeFi Innovation Index',   'Composite',    '\x47110d43175f7f2c2425e7d15792acc5817eb44f'::bytea),
('icETH',       'Interest Compounding ETH',         'Yield',        '\x7C07F7aBe10CE8e33DC6C5aD68FE033085256A84'::bytea),
('JPG',         'NFT Index',                        'Composite',    '\x02e7ac540409d32c90bfb51114003a9e1ff0249c'::bytea),
('FIXED-DAI',   'Fixed Rate Yield Index (DAI)',     'Yield',        '\x015558c3ab97c9e5a9c8c437c71bb498b2e5afb3'::bytea),
('CMI',         'Compound Money Market Index',      'Yield',        '\x87a5F13f08EB61f513eddA70dA88C58B8c8a74fc'::bytea)
;
