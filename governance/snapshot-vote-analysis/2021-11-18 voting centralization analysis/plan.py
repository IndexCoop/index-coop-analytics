# 1. query GraphQL for all proposal IDs
# 2. Iterate through proposals and query GraphQL for votes for each proposal
# 3. Check vote results for "0" vote responses - if it's a 0 it's because the proposal closed too recently and we should discard it
