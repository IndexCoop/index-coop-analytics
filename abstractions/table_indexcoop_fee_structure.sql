-- TODO: This is currently rendered as a view but this should be a table
-- https://dune.xyz/queries/317160

CREATE OR REPLACE VIEW 
dune_user_generated.indexcoop_fee_structure
(symbol,        methodologist,      begin_date,                 end_date,                   streaming_fee,  issue_fee,  redeem_fee,     methodologist_split) as (values
('DPI',         'DeFi Pulse',       '2020-09-10'::timestamp,    current_date::timestamp,    .0095,          .0000,      .0000,          0.30),     
('MVI',         null,               '2021-04-06'::timestamp,    '2021-10-29'::timestamp,    .0095,          .0000,      .0000,          0.00),
('BED',         'Bankless',         '2021-07-13'::timestamp,    current_date::timestamp,    .0025,          .0000,      .0000,          0.50),
('DATA',        'Titans of Data',   '2021-09-20'::timestamp,    current_date::timestamp,    .0095,          .0000,      .0000,          0.40),
('ETH2x-FLI',   'DeFi Pulse',       '2021-03-14'::timestamp,    current_date::timestamp,    .0195,          .0010,      .0010,          0.30),
('BTC2x-FLI',   'DeFi Pulse',       '2021-05-05'::timestamp,    current_date::timestamp,    .0195,          .0010,      .0010,          0.40),
('ETH2x-FLI-P', 'DeFi Pulse',       '2021-12-02'::timestamp,    current_date::timestamp,    .0195,          .0010,      .0010,          0.40),
('MVI',         'MetaPortal',       '2021-10-30'::timestamp,    current_date::timestamp,    .0095,          .0000,      .0000,          0.30),
('GMI',         'Bankless',         '2021-12-29'::timestamp,    current_date::timestamp,    .0195,          .0000,      .0000,          0.40)
)

-- Notes:
    -- Dates are inclusive
    -- If methodologist is internal then null