% mint-redeem-fee-calculator
%
% written by Joseph Knecht
%
% calculates mint redeem fee income for different fee levels given the token parameters
% uses Monte Carlo sampling, treating the price-NAV movement as Brownian motion with drift
% 
% key output: fees
%
% written for Octave, version 6.3.0 
%
% To do: 
% . supply changes, very minor effect
% . convergence stoppage

addpath('statistics-1.4.3/install-conditionally/distributions/');

% MVI_slip = ([.3 .5 .42 .51 .6 .69 .78 .86 .95 1.04 1.13 1.22 1.65 1.91 2.42 3.68 5.32 7.61 21.43 61.51]-.3)/100;
% USD = [0 1000 2000 3000 4000 5000 6000 7000 8000 9000 10000 11000 16000 19000 25000 40000 60000 90000 290000 900000];

clear;
NAV = 1;

% index variables to input
gas_underlyings = 1200;
gas_issuance = 700;
gas_sell = 150;
gas_total = (gas_underlyings + gas_issuance + gas_sell);

% drift and volatility of index
drift = 8e-4; 
volatility = 30*drift;

% LP fee
l = 0.3/100;

% how fast slippage increases with tradesize
% higher number means lower liquidity
slope_underlying = 5e-7;
slope_token = 8.5e-7; % based on MVI

times = 5e4;
n = 0:1e4;
 
% calculate parameters for NAV
slippage_underlying = (l + slope_underlying * n* NAV)./(1 + slope_underlying * n * NAV);
cost_buy_underlying = NAV * n ./ (1 - slippage_underlying);
cost_slippage_sell_underlying = n * NAV .* slippage_underlying;
income_redeem = NAV * n; 


mintredeemfee = .00:.005:0.05; % range of mint-redeem fees to calculate
fees = zeros(length(mintredeemfee),1);

for i = 1:length(mintredeemfee)
  price = NAV;

  mrfee = mintredeemfee(i);
  cost_mint_fee = mrfee * n .* NAV;
  cost_redeem_fee = mrfee * n .* price;

  arb_count = 0;
  mint_count = 0;
  redeem_count = 0;
  disp(sprintf('\nfee: %f feestep: %d / %d', ...
    mintredeemfee(i), i, length(mintredeemfee)));
  drawnow;
  
  for time = 1:times
   price = price + normrnd(drift, volatility);

   slippage_token = (l+slope_token*n*price) ./ (1+slope_token*n*price);
   cost_slippage_sell_token = n * price .* slippage_token;
   cost_buy_token = n * price ./ (1-slippage_token);
   cost_mint_total = cost_buy_underlying + cost_slippage_sell_token + cost_mint_fee + gas_total;
   cost_redeem_total = cost_buy_token + cost_slippage_sell_underlying + cost_redeem_fee + gas_total;
  
   income_mint = price * n;
   
   profit_mint = income_mint - cost_mint_total;
   profit_redeem = income_redeem - cost_redeem_total;
    
   [max_profit_mint idx_mint] = max(profit_mint);
   [max_profit_redeem idx_redeem] = max(profit_redeem);
   
   if max_profit_mint > 0
      fees(i) = fees(i) + cost_mint_fee(idx_mint);
      price = NAV;
      mint_count = mint_count + 1;
      arb_count = arb_count + 1;
    elseif
      max_profit_redeem > 0
      fees(i) = fees(i) + cost_redeem_fee(idx_redeem);
      price = NAV;
      redeem_count = redeem_count + 1;
      arb_count = arb_count + 1;
    endif
    
    pricehistory(time) = price;
  endfor
  disp(sprintf('time: %d mints: %d redeems: %d arbs: %d cumm income: %5.2f', ...
    time, mint_count, redeem_count, arb_count, fees(i)));
endfor
