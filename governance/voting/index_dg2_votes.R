# libraries
library(tidyverse)

# data
## snapshot reports were downloaded from https://snapshot.org/#/index

## snapshot-report-fli-dg2
fli <- read_csv("data/snapshot-report-fli-dg2.csv")

## snapshot-report-mvi-dg2
mvi <- read_csv("data/snapshot-report-mvi-dg2.csv")

## snapshot-report-tti-dg2
tti <- read_csv("data/snapshot-report-tti-dg2.csv")

# analytics
fli %>%
  arrange(desc(balance)) %>%
  mutate(perc_of_vote = balance / sum(balance), cumu_balance = cumsum(balance)) %>%
  mutate(cumu_perc_of_vote = cumsum(perc_of_vote), voter = 1:n()) %>%
  ggplot(aes(x = voter, y = cumu_perc_of_vote)) +
  geom_line() +
  geom_vline(xintercept = 5, linetype="dotted", size = .5) +
  theme_bw()

mvi %>%
  arrange(desc(balance)) %>%
  mutate(perc_of_vote = balance / sum(balance), cumu_balance = cumsum(balance)) %>%
  mutate(cumu_perc_of_vote = cumsum(perc_of_vote), voter = 1:n()) %>%
  ggplot(aes(x = voter, y = cumu_perc_of_vote)) +
  geom_line() +
  geom_vline(xintercept = 5, linetype="dotted", size = .5) +
  theme_bw()

tti %>%
  arrange(desc(balance)) %>%
  mutate(perc_of_vote = balance / sum(balance), cumu_balance = cumsum(balance)) %>%
  mutate(cumu_perc_of_vote = cumsum(perc_of_vote), voter = 1:n()) %>%
  ggplot(aes(x = voter, y = cumu_perc_of_vote)) +
  geom_line() +
  geom_vline(xintercept = 5, linetype="dotted", size = .5) +
  theme_bw()
