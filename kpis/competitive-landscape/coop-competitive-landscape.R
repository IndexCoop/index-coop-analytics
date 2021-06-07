library(httr)
library(tidyverse)
library(lubridate)

# set scientific notation option
options(scipen = 99)

# manage all project/product pairs in one data structure
index_products <- list(
  
  # Index Coop
  list('Index Coop', 'defipulse-index', 'DPI'),
  list('Index Coop', 'coinshares-gold-and-cryptoassets-index-lite', 'CGI'),
  list('Index Coop', 'metaverse-index', 'MVI'),
  list('Index Coop', 'btc-2x-flexible-leverage-index', 'BTC2x-FLI'),
  list('Index Coop', 'eth-2x-flexible-leverage-index', 'ETH2x-FLI'),
  
  # Indexed
  list('Indexed Finance', 'defi-top-5-tokens-index', 'DEFI5'),
  list('Indexed Finance', 'cryptocurrency-top-10-tokens-index', 'CC10'),
  list('Indexed Finance', 'degen-index', 'DEGEN'),
  list('Indexed Finance', 'oracle-top-5', 'ORCL5'),
  list('Indexed Finance', 'nft-platform-index', 'NFTP'),
  list('Indexed Finance', '484-fund', 'ERROR'),
  
  # PowerPool
  list('PowerPool', 'power-index-pool-token', 'PIPT'),
  list('PowerPool', 'assy-index', 'ASSY'),
  list('PowerPool', 'yearn-ecosystem-token-index', 'YETI'),
  list('PowerPool', 'yearn-lazy-ape', 'YLA'),
  
  # PieDAO
  list('PieDAO', 'piedao-balanced-crypto-pie', 'BCP'),
  list('PieDAO', 'piedao-btc', 'BTC++'),
  list('PieDAO', 'piedao-defi', 'DEFI++'),
  list('PieDAO', 'piedao-defi-large-cap', 'DEFI+L'),
  list('PieDAO', 'piedao-defi-small-cap', 'DEFI+S'),
  list('PieDAO', 'piedao-yearn-ecosystem-pie', 'YPIE'),
  list('PieDAO', 'metaverse-nft-index', 'PLAY'),
  
  # BasketDAO
  list('BasketDAO', 'basketdao-defi-index', 'BDI')
  
)

index <- data.frame(matrix(unlist(index_products), nrow = length(index_products), byrow = T))
colnames(index) <- c('project', 'product', 'symbol')
index <- as_tibble(index)

# API Time
for(i in 1:nrow(index)) {

  # call coingecko and process results
  str <- paste0('https://api.coingecko.com/api/v3/coins/', index$product[i], '/market_chart?vs_currency=usd&days=max') 
  r <- GET(str)
  out <- content(r)
  
  # format date from timestamp to day, pull out prices, prepare to combine
  dates <- date(as.POSIXct(unlist(out$market_caps)[c(TRUE, FALSE)] / 1000, origin = "1970-01-01", tz = "UTC"))
  market_caps <- unlist(out$market_caps)[c(FALSE, TRUE)]
  project <- rep(index$project[i], length(dates))
  product <- rep(index$product[i], length(dates))
  symbol <- rep(index$symbol[i], length(dates))
  
  # create temp table with current product
  temp <- tibble(
    "project" = project,
    "symbol" = symbol,
    "date" = dates,
    "market_cap" = market_caps
  )
  
  # bind temp table to final table
  if(i == 1) {
    
    final <- temp
    
  } else {
    
    final <- rbind(final, temp)
    
  }
  
}

# data frame by project/product/date
product_mcaps <- final %>%
  group_by(project, symbol, date) %>%
  summarize(market_cap = mean(market_cap))

#data frame by project/date
project_mcaps <- product_mcaps %>%
  group_by(project, date) %>%
  summarize(market_cap = sum(market_cap))

# quick visual of project AUM
project_mcaps %>%
  ggplot(aes(x = date, y = market_cap, color = project)) +
  geom_line() +
  scale_y_continuous(name = "Market Cap", labels = scales::comma) +
  scale_x_date(name = "") +
  theme_bw()

# write .csvs
write_csv(product_mcaps, 'index-coop-competitive-landscape-product-mcaps-2021_06_07.csv')
write_csv(project_mcaps, 'index-coop-competitive-landscape-project-mcaps-2021_06_07.csv')
