library(httr)
library(tidyverse)
library(lubridate)

# GOAL: we need a tool to help us stay on top of upcoming projects in the metaverse
#     we are waiting for updates to the CoinGecko API to go live that will allow us to query by category
#     the idea is to query for metaverse categories and return tokens that meet a minimum criteria
#     consider turning this into a shiny dashboard for the MVI methodologists to use

# DeFi (to be used as an example)
r <- GET('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=non-fungible-tokens-nft')

coins <- content(r)

for(i in 1:length(coins)) {
  
  coin <- coins[[i]]
  
  temp <- tibble("id" = ifelse(is.null(coin$id), 'NA', coin$id),
                 "symbol" = ifelse(is.null(coin$symbol), 'NA', coin$symbol),
                 "market_cap" = ifelse(is.null(coin$market_cap), 'NA', coin$market_cap),
                 "fdv" = ifelse(is.null(coin$fully_diluted_valuation), 'NA', coin$fully_diluted_valuation),
                 "total_volume" = ifelse(is.null(coin$total_volume), 'NA', coin$total_volume),
                 "circ_supply" = ifelse(is.null(coin$circulating_supply), 'NA', coin$circulating_supply),
                 "max_supply" = ifelse(is.null(coin$max_supply), 'NA', coin$max_supply))
  
  if(i == 1) {
    
    final <- temp
    
  } else {
    
    final <- rbind(final, temp)
    
  }
  
}

View(final)

write_csv(final, "mvi_token_screening_04_19_2021.csv")
