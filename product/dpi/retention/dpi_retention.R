library(tidyverse)
library(lubridate)
library(plotly)
library(gridExtra)
library(ggthemes)
library(zoo)

# base data is pulled from the DPI Retention Base query on Dune: https://duneanalytics.com/queries/16759
# SQL also found in dpi_retention_base.sql
# run the query on dune, download it and save it in the data/ directory
dat <- read_csv('data/DPI_Retention_Base_2021_04_06.csv')

# identify contracts / arb bots
temp <- dat %>% 
  group_by(address, date(ymd_hms(evt_block_minute))) %>% 
  summarize(n_amount = sum(amount), n_tx = n())
  
contract_addresses <- temp %>%
  filter(n_tx >= 2, n_amount <= 1e-14) %>%
  distinct(address) %>%
  pull(address)

# set up data
d <- dat %>%
  filter(!(address %in% contract_addresses)) %>%
  mutate(dt = ymd_hms(evt_block_minute)) %>%
  select(address, dt, amount, type, evt_tx_hash)

temp <- dat %>%
  filter(!(address %in% contract_addresses)) %>%
  mutate(dt = ymd_hms(evt_block_minute)) %>%
  select(address, dt, amount, type, evt_tx_hash) %>%
  arrange(address, dt) %>%
  group_by(address) %>%
  summarize(dt,
            date = date(dt),
            amount, 
            running_exposure = cumsum(amount), 
            type, 
            evt_tx_hash,
            exposure_group = case_when(
              max(amount) >= 250 ~ "250+",
              max(amount) >= 50 ~ "50-249",
              max(amount) >= 10 ~ "10-49",
              TRUE ~ "<10"
              ),
            cohort = as.yearmon(dt, "%m/%Y")
            )

# determine groups and "signup" cohorts
groups <- temp %>% 
  group_by(address) %>% 
  summarize(cohort = min(cohort), group = min(exposure_group))

cohorts_raw <- d %>% 
  arrange(desc(dt)) %>% 
  mutate(cohort = as.yearmon(dt, "%m/%Y")) %>% 
  pull(cohort)

cohort_levels <- unique(cohorts_raw)
current_cohort <- as.yearmon(today(), "%m/%Y")
completed_cohorts <- cohorts_levels[which(cohorts_levels != current_cohort)]

# calculate exposure days for each address
cntr <- 1
for(t_address in unique(temp$address)) {

  min_date <- min(temp %>% filter(address == t_address) %>% pull(date))
  max_date <- lubridate::today(tzone = 'GMT')
  cand <- tibble(address = t_address, date = seq(min_date, max_date, by="days"))
  
  if(cntr == 1){
    
    final <- cand
    cntr <- cntr + 1
    
  } else {
    
    final <- rbind(final, cand)
    cntr <- cntr + 1
    
  }
  
  print(cntr)
  
}

# save to temp .rds so no need to re-run that again
saveRDS(final, "final.rds")

# join back to temp to get the amount for each day of exposure
fini <- final %>%
  left_join(temp %>% group_by(address, date) %>% summarize(amount = sum(amount)), by = c('address', 'date')) %>%
  mutate(amount = ifelse(is.na(amount), 0, amount)) %>%
  group_by(address) %>%
  summarize(date, amount, running_exposure = cumsum(amount), day = 1:n()) %>%
  ungroup %>%
  left_join(groups, by = c('address')) %>%
  mutate(retained = ifelse(running_exposure > 0, 1, 0))

# determine which days to include for which months (want to have a lag period for maturity)
include_days <- fini %>%
  group_by(cohort) %>%
  summarize(max_day = max(day)) %>%
  mutate(include_days = round(max_day * 0.75))

fin <- fini %>% 
  merge(include_days %>% select(cohort, include_days), by = 'cohort')

fin$cohort <- factor(fin$cohort, levels = factor(cohort_levels))

# retention
fin %>%
  filter(day <= include_days) %>%
  group_by(day, cohort) %>%
  summarize(retention = mean(retained)) %>%
  ggplot(aes(x = day, y = retention, color = cohort)) +
  geom_line() +
  theme_bw() +
  xlab("days since initial exposure") + ylab("retention") +
  labs(title = "DPI Retention", 
       subtitle = "Cohorts determined by the month of initial exposure to DPI.", 
       caption = "Source: Dune Analytics", 
       col = "cohort") +
  scale_colour_colorblind()

# net DPI retention
fin %>% 
  filter(day <= round(max(day) * .75)) %>%
  ungroup() %>% 
  group_by(day) %>% 
  summarize(amount = sum(amount)) %>%
  summarize(day, amount, 
            running_amount = cumsum(amount), 
            net_retention = cumsum(amount) / first(amount)) %>%
  ggplot(aes(x = day, y = net_retention)) +
  geom_line() +
  theme_bw() +
  xlab("days since initial exposure") + ylab("amount of initial exposure") +
  labs(title = "DPI Unit Retention", 
       subtitle = "Growth in unit exposure aggregated across all addresses ever having DPI exposure.", 
       caption = "Source: Dune Analytics", 
       col = "cohort") +
  scale_colour_colorblind()

# DPI retention by exposure group
# <10
l1 <- fin %>%
  filter(day <= include_days) %>%
  filter(group == '<10') %>%
  group_by(day, cohort) %>%
  summarize(retention = mean(retained)) %>%
  ggplot(aes(x = day, y = retention, color = cohort)) +
  geom_line() +
  theme_bw() +
  xlab("") + ylab("retention") +
  labs(subtitle = "<10 DPI", 
       col = "cohort") +
  scale_colour_colorblind()

# 10-49
l2 <- fin %>%
  filter(day <= include_days) %>%
  filter(group == '10-49') %>%
  group_by(day, cohort) %>%
  summarize(retention = mean(retained)) %>%
  ggplot(aes(x = day, y = retention, color = cohort)) +
  geom_line() +
  theme_bw() +
  xlab("") + ylab("") +
  labs(subtitle = "10-49 DPI", 
       col = "cohort") +
  scale_colour_colorblind()

# 50-249
l3 <- fin %>%
  filter(day <= include_days) %>%
  filter(group == '50-249') %>%
  group_by(day, cohort) %>%
  summarize(retention = mean(retained)) %>%
  ggplot(aes(x = day, y = retention, color = cohort)) +
  geom_line() +
  theme_bw() +
  xlab("days since initial exposure") + ylab("retention") +
  labs(subtitle = '50-249 DPI',
       col = "cohort") +
  scale_colour_colorblind()

# 250+
l4 <- fin %>%
  filter(day <= include_days) %>%
  filter(cohort != 'sep') %>%
  filter(group == '250+') %>%
  group_by(day, cohort) %>%
  summarize(retention = mean(retained)) %>%
  ggplot(aes(x = day, y = retention, color = cohort)) +
  geom_line() +
  theme_bw() +
  xlab("days since initial exposure") + ylab("") +
  labs(subtitle = "250+ DPI (Whale)", 
       col = "cohort") +
  scale_colour_colorblind()

g_legend<-function(a.gplot){
  tmp <- ggplot_gtable(ggplot_build(a.gplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)}

legen <- g_legend(l1)

grid.arrange(arrangeGrob(l1 + theme(legend.position="none"), 
                         l2 + theme(legend.position="none"), 
                         l3 + theme(legend.position="none"), 
                         l4 + theme(legend.position="none"), nrow = 2), 
             legen, ncol = 2, widths = c(9, 2))


# number of address x exposure
g <- groups %>% 
  group_by(group, cohort) %>% 
  summarize(addresses = n()) 

g$cohort <- factor(g$cohort)
g$group <- factor(g$group, levels = c('<10', '10-49', '50-249', '250+'))

g %>%
  filter(cohort %in% factor(completed_cohorts)) %>%
  ggplot(aes(fill = group, y = addresses, x = cohort)) + 
  geom_bar(position = 'dodge', stat = 'identity') +
  geom_text(aes(label = addresses),
            size = 3,
            vjust = -.5, 
            position = position_dodge(0.9),
            color = 'black') + 
  theme_bw() + 
  xlab("") + ylab("addresses") +
  labs(title = "DPI Holders Growth", 
       caption = "Source: Dune Analytics", 
       fill = 'DPI Exposure') +
  scale_fill_colorblind()

g %>%
  filter(cohort %in% factor(completed_cohorts)) %>%
  ggplot(aes(fill = group, y = addresses, x = cohort)) + 
  geom_bar(position = 'fill', stat = 'identity') +
  theme_bw() + 
  xlab("") + ylab("% of addresses") +
  labs(title = "DPI Holder Exposure Distribution", 
       caption = "Source: Dune Analytics", 
       fill = 'DPI Exposure') +
  scale_fill_colorblind()


# DPI Whale Retention
whale <- fin %>%
  filter(group == '250+')

# net retention
whale %>%
  filter(group == '250+' & cohort != 'Sep 2020') %>%
  filter(day <= include_days) %>%
  mutate(whale_retention = ifelse(running_exposure >= 250, 1, 0)) %>%
  group_by(day, cohort) %>%
  summarize(whale = mean(whale_retention)) %>%
  ggplot(aes(x = day, y = whale, color = cohort)) +
  geom_line() +
  theme_bw() +
  xlab("days since initial exposure") + ylab("% of whales retained as whales") +
  labs(title = "DPI Whale Retention", 
       subtitle = "Cohorts determined by the month of initial exposure to DPI.", 
       caption = "Source: Dune Analytics", 
       col = "cohort") +
  scale_colour_colorblind()


# AUM x Exposure Levels
# # addresses, # AUM, % of addresses, % of AUM

fin$group <- factor(fin$group, levels = c('<10', '10-49', '50-249', '250+'))

a <- fin %>%
  group_by(group, date) %>%
  summarize(addresses = n_distinct(address)) %>%
  ggplot(aes(x = date, y = addresses, color = group)) +
  geom_line() +
  theme_bw() +
  labs(y = "Absolute #", x = "", col = "DPI Exposure Level") + 
  scale_colour_colorblind()

b <- fin %>%
  group_by(group, date) %>%
  summarize(addresses = n_distinct(address)) %>%
  group_by(date) %>%
  summarize(group, date, addresses, p = addresses / sum(addresses)) %>%
  ggplot(aes(x = date, y = p, color = group)) +
  geom_line() +
  theme_bw() + 
  theme(axis.title.x = element_blank(), axis.text.x = element_blank()) +
  labs(y = "% of Total", title = "Addresses w/ DPI Exposure") +
  scale_colour_colorblind()

c <- fin %>% 
  group_by(group, date) %>%
  summarize(exposure = sum(running_exposure)) %>%
  group_by(date) %>%
  summarize(group, date, exposure, p = exposure / sum(exposure)) %>%
  ggplot(aes(x = date, y = exposure, color = group)) +
  geom_line() +
  theme_bw() +
  labs(x = "", y = "") +
  scale_colour_colorblind()

d <- fin %>% 
  # group_by(address) %>% 
  # # filter(date == max(date)) %>%
  group_by(group, date) %>%
  summarize(exposure = sum(running_exposure)) %>%
  group_by(date) %>%
  summarize(group, date, exposure, p = exposure / sum(exposure)) %>%
  ggplot(aes(x = date, y = p, color = group)) +
  geom_line() +
  theme_bw() + 
  theme(axis.title.x = element_blank(), axis.text.x = element_blank()) +
  labs(y = "", title = "DPI AUM") +
  scale_colour_colorblind()

g_legend<-function(a.gplot){
  tmp <- ggplot_gtable(ggplot_build(a.gplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)}

leg <- g_legend(a)

grid.arrange(arrangeGrob(b + theme(legend.position="none"), 
             d + theme(legend.position="none"), 
             a + theme(legend.position="none"), 
             c + theme(legend.position="none"), nrow = 2), leg, ncol = 2, widths = c(9, 2))





