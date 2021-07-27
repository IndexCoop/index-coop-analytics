library(httr)
library(tidyverse)
library(lubridate)

# get all symbols
symbols <- c('audio', 'axs', 'mana', 'dg', 'meme', 'enj', 'nftx', 'rari', 
             'rfox', 'revv', 'tvk', 'sand', 'whale', 'gala', 'muse', 'waxp', 'chz',
             'ern', 'ilv')

# if a new token needs to be added, this section will help grab the id for the token

# symbols_new <- c('ern', 'ilv')
# 
# r1 <- GET('https://api.coingecko.com/api/v3/coins/list')
# coins <- content(r1)
# df <- data.frame(matrix(unlist(coins), nrow=length(coins), byrow=TRUE))
# colnames(df) <- c('id', 'symbol', 'name')
# df %>% filter(symbol %in% symbols_new) %>% pull(id)

# get all id's
ids <- c('audius', 'axie-infinity', 'decentraland', 'decentral-games', 'degenerator', 'enjincoin',
         'nftx', 'rarible', 'redfox-labs-2', 'revv', 'terra-virtua-kolect', 'the-sandbox', 'whale', 
         'gala', 'muse-2', 'wax', 'chiliz', 'ethernity-chain', 'illuvium')

# query market cap and circulation data for all tokens
for(id in ids){
  
  str_s <- paste0('https://api.coingecko.com/api/v3/coins/', id)
  r_s <- GET(str_s)
  out_s <- content(r_s)
  supply <- out_s$market_data$total_supply
  
  str <- paste0('https://api.coingecko.com/api/v3/coins/', id, '/market_chart?vs_currency=usd&days=max&interval=daily')
  r <- GET(str)
  out <- content(r)
  dates <- date(as.POSIXct(unlist(out$market_caps)[c(TRUE, FALSE)] / 1000, origin = "1970-01-01", tz = "UTC"))
  prices <- unlist(out$prices)[c(FALSE, TRUE)]
  fully <- prices * supply
  mcs <- unlist(out$market_caps)[c(FALSE, TRUE)]
  perc_supply <- mcs / fully
  perc_supply <- ifelse(perc_supply > 1, 0, perc_supply)
  
  temp <- tibble("date" = dates, 
                 "market_cap" = mcs, 
                 "fully_diluted" = fully,
                 "percent_supply" = perc_supply,
                 "symbol" = symbols[which(id == ids)])
  
  if(id == 'audius') {
    
    final <- temp
    
  } else {
    
    final <- rbind(final, temp)
    
  }
  
}

# format market cap data
t <- final %>% 
  select(date, symbol, market_cap) %>% 
  spread(key = symbol, value = market_cap) %>% 
  fill(-date)

# format % of supply cirulating data
p <- final %>% 
  select(date, symbol, percent_supply) %>%
  spread(key = symbol, value = percent_supply) %>% 
  fill(-date)

# format fully diluated valuation data
f <- final %>%
  select(date, symbol, fully_diluted) %>%
  spread(key = symbol, value = fully_diluted) %>%
  fill(-date)

# write data to csv
t %>% write_csv("mvi_token_market_caps_07_23_2021.csv")
p %>% write_csv("mvi_token_percent_supply_07_23_2021.csv")
f %>% write_csv("mvi_token_fully_diluted_07_23_2021.csv")

