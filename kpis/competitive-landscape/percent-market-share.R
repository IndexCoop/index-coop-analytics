library(httr)
library(tidyverse)
library(lubridate)
library(dplyr)
library(tidyr)
library(zoo)
library(ggthemes)
library(reshape2)


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
  list('Index Coop', 'bankless-bed-index', 'BED'),
  list('Index Coop', 'data-economy-index', 'DATA'),
  
  # Indexed
  list('Indexed Finance', 'defi-top-5-tokens-index', 'DEFI5'),
  list('Indexed Finance', 'cryptocurrency-top-10-tokens-index', 'CC10'),
  list('Indexed Finance', 'degen-index', 'DEGEN'),
  list('Indexed Finance', 'oracle-top-5', 'ORCL5'),
  list('Indexed Finance', 'nft-platform-index', 'NFTP'),
  list('Indexed Finance', 'future-of-finance-fund', 'FFF'),
  
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
  list('BasketDAO', 'basketdao-defi-index', 'BDI'),
  
  #Amun
  list('Amun', 'amun-defi-index', 'dfi'),
  list('Amun', 'amun-defi-momentum-index', 'dmx')
  
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
# mutate and complete create NAs for the dates that the API isn't pulling data correctly
product_mcaps <- final %>%
  group_by(project, symbol, date) %>%
  summarize(market_cap = mean(market_cap))%>%
  mutate(Date = as.Date(date)) %>%
  complete(date = seq.Date(min(date), max(date), by="day"))

#the extra date column and the project column were messing the whole thing up, get them outta here for now
product_mcaps$Date = NULL
product_mcaps$project = NULL

#spread it out by product, then fill in NA with last available data
product_mcaps_formatted <- product_mcaps %>%
  spread(key = symbol, value = "market_cap") %>%
  na.locf(na.rm = FALSE)

#Stack it back up into 3 columns
product_mcaps_formatted2 <- product_mcaps_formatted %>%
  gather(key = "symbol", value = "market_cap",
         c(-"date"))

#re-add the project column and organize it by date
product_mcaps_formatted3 <- merge(index, product_mcaps_formatted2, by = "symbol")

product_mcaps_formatted3 <- product_mcaps_formatted3[order(product_mcaps_formatted3$date),]

#data frame by project/date
project_mcaps <- product_mcaps_formatted3 %>%
  group_by(project, date) %>%
  summarize(market_cap = sum(market_cap, na.rm = TRUE)) 

#sum up the each project's mcap each day
project_mcaps_formatted <- project_mcaps %>%
  spread(key = project, value = market_cap) %>%
  replace(is.na(.), 0)


#sum of TVL by day w/ starting date of 4/15/2020 
#(Find and replace the closing row with the number of rows in "project_mcaps_formatted)
date_sum <- data.frame(rowSums(project_mcaps_formatted[1:nrow(project_mcaps_formatted),2:7], na.rm = TRUE)) 

#TVL by project with sum at the end
date_with_total <- bind_cols(project_mcaps_formatted, date_sum)

#Rename sum column name to not be gibberish
#number in file name needs to be replaced as above (find and replace)
colnames(date_with_total)[ncol(date_with_total)] <- "Total TVL"

#copy the data
percent_of_tvl <- date_with_total

#adding percentage of TVL columns
percent_of_tvl$'BasketDAO %' <- (date_with_total$BasketDAO / date_with_total$`Total TVL`) * 100
percent_of_tvl$'Index Coop %' <- (date_with_total$`Index Coop` / date_with_total$`Total TVL`) * 100
percent_of_tvl$'NDX %' <- (date_with_total$`Indexed Finance` / date_with_total$`Total TVL`) * 100
percent_of_tvl$'PieDAO %' <- (date_with_total$PieDAO / date_with_total$`Total TVL`) * 100
percent_of_tvl$'PowerPool %' <- (date_with_total$PowerPool / date_with_total$`Total TVL`) * 100
percent_of_tvl$'Amun %' <- (date_with_total$Amun / date_with_total$`Total TVL`) * 100

#copy the data
tvl2 <- percent_of_tvl

#NULL out unnecessary columns
tvl2$Amun = NULL
tvl2$BasketDAO = NULL
tvl2$`Index Coop` = NULL
tvl2$`Indexed Finance` = NULL
tvl2$PieDAO = NULL
tvl2$PowerPool = NULL
tvl2$`Total TVL` = NULL

#quick visual of market share

#convert data to long format
tvl2_long <- melt(tvl2, id = "date")

#rename columns in tvl2_long
colnames(tvl2_long)[colnames(tvl2_long) == "value"] <- "Percent"
colnames(tvl2_long)[colnames(tvl2_long) == "variable"] <- "Project"
colnames(tvl2_long)[colnames(tvl2_long) == "date"] <- "Date"

#Index True false for stylizing
mutate(tvl2_long, isIndex = (Project == 'Index Coop %'))

#graph it!
tvl2_long %>%
  mutate(tvl2_long,isIndex = (Project == 'Index Coop %')) %>%
  ggplot(aes(x = Date, y = Percent,color = Project)) +
  geom_line(aes(linetype = isIndex), size = .75, alpha = 0.75)+
  labs(title = "Market Share by Project",
       x = "Date",
       y = "Market Share (%)",
       color = " Index Project"
  )+
  theme_fivethirtyeight()+
  theme(axis.title = element_text(), plot.background = element_rect(fill = "#FFFFFF"), panel.background = element_rect(fill = "#FFFFFF"), legend.background = element_rect(fill = "#FFFFFF"), legend.key = element_rect(fill = "#FFFFFF"))+
  scale_linetype_manual(values = c("dashed", "solid"), guide = "none")
d 


