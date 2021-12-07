with

index_coop_interest_bearing_tokens as (select * from (values
('fBED-19',         'BED',          'Rari Capital',         'Fuse Pool 19',     '\x20c461b0214eebb71be32b6a1605ed992a8f1410'::bytea,   8   ),
('bBED',            'BED',          'Beta Finance',         NULL,               '\x265807d818ff14d44b552f5897abdad6eed603e2'::bytea,   18  ),
('aDPI',            'DPI',          'Aave',                 'V2',               '\x6f634c6135d2ebd550000ac92f494f9cb8183dae'::bytea,   18  ),
('crDPI',           'DPI',          'Cream',                'V1',               '\x2a537fa9ffaea8c1a41d3c2b68a9cb791529366d'::bytea,   8   ),
('cyDPI',           'DPI',          'Cream',                'V2 IronBank',      '\x7736ffb07104c0c400bb0cc9a7c228452a732992'::bytea,   18  ),
('fDPI-19',         'DPI',          'Rari Capital',         'Fuse Pool 19',     '\xf06f65a6b7d2c401fcb8b3273d036d21fe2a5963'::bytea,   8   ),
('bDPI',            'DPI',          'Beta Finance',         NULL,               '\x8b3ff556739b19a66077d01dfc5d131ecaac3596'::bytea,   18  ),
('vaDPI',           'DPI',          'Vesper Finance',       NULL,               '\x9b91ab47cefC35dbe4DDCC7983fFA1fB40795663'::bytea,   18  ),
('fETH2x-FLI-19',   'ETH2x-FLI',    'Rari Capital',         'Fuse Pool 19',     '\xf0fe94d76fd77c1d9915616261e7e19865cedc2c'::bytea,   8   ),
('bETH2x-FLI',      'ETH2x-FLI',    'Beta Finance',         NULL,               '\x9122c38b11888f24637ecc1fee7abc67cf346508'::bytea,   18  ),
('bMVI',            'MVI',          'Beta Finance',         NULL,               '\x322897F8b9eed2533540Fbb74D90ADe74D80fbfA'::bytea,   18  )
) as t (symbol, asset_symbol, protocol, version, address, decimals)
)

select * from index_coop_interest_bearing_tokens