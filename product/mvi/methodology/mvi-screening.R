library(httr)
library(tidyverse)
library(lubridate)

# GOAL: we need a tool to help us stay on top of upcoming projects in the metaverse
#     we are waiting for updates to the CoinGecko API to go live that will allow us to query by category
#     the idea is to query for metaverse categories and return tokens that meet a minimum criteria
#     consider turning this into a shiny dashboard for the MVI methodologists to use

categories <- c("non-fungible-tokens-nft", "entertainment", "augmented-reality", "virtual-reality","music", "metaverse")

for(c in 1:length(categories)) {

  url <- paste0('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=', 
                categories[c])
  r <- GET(url)
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
  
  if(c == 1){
    
    fin <- final
    
  } else {
    
    f <- final %>% filter(!(id %in% fin$id))
    fin <- rbind(fin, f)
    
  }

}

out <- fin %>% 
  select(1:2) %>% 
  cbind(
    fin %>% 
      select(3:7) %>% 
      mutate_if(is.character, as.numeric)
    ) %>%
  arrange(desc(market_cap))

View(out)

write_csv(out, "mvi_token_screening_07_23_2021.csv")
