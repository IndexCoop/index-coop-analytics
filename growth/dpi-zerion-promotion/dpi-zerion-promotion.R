library(tidyverse)
library(lubridate)

index <- read_csv("dpi-zerion-index-price-feed.csv")
sells <- read_csv("dpi-zerion-sells-during-promotion.csv")
buys <- read_csv("index_campaign_april_2021.csv")
transfer_check <- read_csv("index_campaign_april_2021_who_did_transfers.csv")
transfers <- read_csv("dpi-zerion-transfers-during-promotion.csv")

# initial data transformations

buys <-  buys %>%
  filter(ymd_hms(buys$block_time) >= ymd('2021-04-13'))

sells <- sells %>%
  mutate(address = paste0("0", substr(address, 2, nchar(address))),
         tx_hash = paste0("0", substr(tx_hash, 2, nchar(tx_hash))))

transfers <- transfers %>%
  mutate(from_address = paste0("0", substr(from_address, 2, nchar(from_address))),
         to_address = paste0("0", substr(to_address, 2, nchar(to_address))),
         evt_tx_hash = paste0("0", substr(evt_tx_hash, 2, nchar(evt_tx_hash)))
  )
         
# no transfers that led to sells to check out at this time
transfer_wallets <- transfer_check %>%
  filter(!(hash %in% sells$tx_hash)) %>%
  left_join(transfers, by = c('hash' = 'evt_tx_hash')) %>%
  filter(!is.na(type)) %>%
  left_join(sells_agg, by = c('to_address' = 'address')) %>%
  filter(!is.na(min_sell_block_time))

# removing sells from buys within 7 days

# aggregate buys
buys_agg <- buys %>%
  group_by(wallet) %>%
  summarize(min_buy_block_time = min(block_time), max_buy_block_time = max(block_time),
            buy_volume = sum(volume), buy_usd_volume = sum(usd_volume))

# aggregate sells that happened post first promotion buy
sells_agg <- sells %>%
  left_join(buys_agg %>%
              select(wallet, min_buy_block_time, max_buy_block_time), by = c('address' = 'wallet')) %>%
  filter(block_time >= min_buy_block_time) %>%
  select(-c(min_buy_block_time, max_buy_block_time)) %>%
  group_by(address) %>%
  summarize(min_sell_block_time = min(block_time), max_sell_block_time = max(block_time),
            sell_volume = sum(amount), sell_usd_volume = sum(usd_volume)) 

# combine buys and sells
buy_sell <- buys_agg %>%
  left_join(sells_agg, by = c('wallet' = 'address'))

# calculate eligible usd volume
t <- buy_sell %>%
  mutate(buy_sell_day_diff = time_length(interval(max_buy_block_time, min_sell_block_time), unit = 'day')) %>%
  mutate(eligible_usd_volume = case_when(
    is.na(buy_sell_day_diff) ~ buy_usd_volume,
    buy_sell_day_diff >= 7 ~ buy_usd_volume,
    buy_sell_day_diff < 7 ~ buy_usd_volume - (buy_usd_volume * (sell_volume / buy_volume))
  ))

index_price_avg <- mean(index$price)

# 5% index rewards
rewards <- t %>%
  filter(eligible_usd_volume >= 10) %>%
  select(wallet, eligible_usd_volume) %>%
  mutate(usd_value_reward = eligible_usd_volume * .05,
          index_reward = (eligible_usd_volume * .05) / index_price_avg)

rewards %>% write_csv("dpi_zerion_promotion_index_rewards.csv")
