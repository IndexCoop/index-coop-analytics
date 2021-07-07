import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import quantecon as qe
import requests
import string


"""
Description: Voter analysis for Index Coop DAO Decision Gate 2 Governance Votes
Prereqs:
    - Download Snapshot vote reports either manually or using snapshot_report_download.py script
    - Download the token's hodler distribution from Etherscan:
        - for INDEX token, Download CSV link at https://etherscan.io/token/0x0954906da0Bf32d5479e25f46056d22f08464cab#balances
        - this could not be automated as Etherscan requires recaptcha. Could be automated with a graph or etherscan pro API call
    - Install python dependencies listed above
"""


# Parameters: Substitute with the values of the project and Snapshot vote you want to analyze
# Running analsysis for all Index Coop Decision Gate 2 votes
vote_urls = {
    'BTC2X-FLI': 'https://snapshot.org/#/index/proposal/Qmc2DPHoKnyYxRjQfwbpbBngt5xHZrLBgkywGqZm7hHnEU',
    'SMI': 'https://snapshot.org/#/index/proposal/QmYBG5zerdSkC9TGwguy5biCS5h2cg49PQCHCukJqHmfE1',
    'TTI': 'https://snapshot.org/#/index/proposal/QmTPVkgfJBB1go2DCFhmacjgSWrLUzTFimdTGdB7d9Q2ao',
    'MVI': 'https://snapshot.org/#/index/proposal/QmadsabYMJC96jU2S2kPCSh1suVfDVApGLwrux2WwsHd7x',
    'ETH2X-FLI2': 'https://snapshot.org/#/index/proposal/QmYHV2vdTaSubtNJefSoYx82ypsmtzrT7CGUU1EHsXzHC3',
    'ETH2X-FLI1': 'https://snapshot.org/#/index/proposal/QmQwQn4k324kMKPjsSX6ZEzjkkKWh1DNfAN2mQ3dd5aP1a'
}
proposal_ids = [url.split('/')[-1] for url in vote_urls.values()]
local_download_folder_path = 'C:/Users/craig/Downloads/'
etherscan_token_hodler_csv_path = 'etherscan_index_hodler_table.csv'

# Remove treasury, vesting and DEX LP wallets. Manually entered from https://etherscan.io/token/0x0954906da0Bf32d5479e25f46056d22f08464cab#balances
wallet_addresses_to_exclude = [
    '0xd89c642e52bd9c72bcc0778bcf4de307cc48e75a',
    '0xf64d061106054fe63b0aca68916266182e77e9bc',
    '0x26e316f5b3819264df013ccf47989fb8c891b088',
    '0x9467cfadc9de245010df95ec6a585a506a8ad5fc',
    '0xe2250424378b6a6dc912f5714cfd308a8d593986',
    '0x71f2b246f270c6af49e2e514ca9f362b491fbbe1',
    '0x4c11dfd35a4fe079b41d5d9729ed34c00d487712',
    '0x66a7d781828b03ee1ae678cd3fe2d595ba3b6000',
    '0x0d627ca04a97219f182dab0dc2a23fb4a5b02a9d',
    '0x5c29aa6761803bcfda7f683eaa0ff9bddda3649d',
    '0xce3c6312385fcf233ab0de574b0cb1a588566c3f',
    '0xb93b505ed567982e2b6756177ddd23ab5745f309',
    '0xdd111f0fc07f4d89ed6ff96dbab19a61450b8435',
    '0x0f58793e8cf39d6b60919ffaf773a7f95a568146',
    '0xa73df646512c82550c2b3c0324c4eedee53b400c',
    '0xcf19a7c81fcf0e01c927f28a2b551405e58c77e5',
    '0x3452a7f30a712e415a0674c0341d44ee9d9786f9',
    '0x674bdf20a0f284d710bc40872100128e2d66bd3f',
    '0x8f06fba4684b5e0988f215a47775bb611af0f986',
    '0x673d140eed36385cb784e279f8759f495c97cf03'
]

# Enter the quorum threshold for the project, get current circulating supply from CoinGecko
# Warning: this provides approximate results based on current outstanding supply and therefore may not be accurate for
#          projects with high inflation that deviates from supply during historical votes. Historical supply could be fed
#          with database queries or historically accurate wallet balance CSV files
coin_gecko_response = requests.get('https://api.coingecko.com/api/v3/coins/index-cooperative?tickers=true&market_data=true').json()
circulating_supply = coin_gecko_response['market_data']['circulating_supply']
quorum_threshold = 0.15
yes_vote_threshold = 0.6
votes_needed_for_quorum = round(quorum_threshold * circulating_supply, 2)
etherscan_token_hodler_csv_path = 'etherscan_index_hodler_table.csv'

def compute_gini(series_label:str, series_np_array:np.array):
    gini = qe.gini_coefficient(series_np_array)
    print(f'{series_label} Gini Coefficient: {gini}')

    # Plot Lorenz curve
    equality_curve, lorenz_curve = qe.lorenz_curve(series_np_array)
    fig, ax = plt.subplots()
    ax.plot(equality_curve, lorenz_curve, label=f'Lorenz curve of {series_label}')
    ax.plot(equality_curve, equality_curve, label='Lorenz curve, equality')
    ax.legend()
    plt.show()

# Read Etherscan.io hodler wallet balance csv file
index_hodlers_with_treasury = pd.read_csv(etherscan_token_hodler_csv_path)
index_hodler_df = index_hodlers_with_treasury.loc[~index_hodlers_with_treasury.HolderAddress.isin(wallet_addresses_to_exclude)]
index_hodler_df = index_hodler_df.sort_values('Balance', ascending=False)
index_hodler_df['cumulative_dist'] = index_hodler_df['Balance'].cumsum()
index_hodler_df.index = range(len(index_hodler_df))
index_hodler_df.Balance.describe()
wallet_balance_sum = index_hodler_df['Balance'].sum()
theoretical_minimum_number_of_voters_needed = min(index_hodler_df.index[index_hodler_df['cumulative_dist'] > votes_needed_for_quorum]) + 1
print(f'The minimum number of (all) hodlers needed to pass an initiative is: {theoretical_minimum_number_of_voters_needed}')
theoretical_minimum_number_of_low_balance_voters =  len(index_hodler_df) - max(index_hodler_df.index[(wallet_balance_sum-index_hodler_df['cumulative_dist']) > votes_needed_for_quorum])
print(f'The minimum number of (low balance) hodlers needed to pass an initiative if no one of higher rank participates is: {theoretical_minimum_number_of_low_balance_voters} / {len(index_hodler_df)}')
print(f'Global Wealth Gini Coefficient (2019): 0.885, source: https://en.wikipedia.org/wiki/List_of_countries_by_wealth_inequality')
hodler_balance_array = index_hodler_df['Balance'].to_numpy()
compute_gini('All INDEX wallet balances', hodler_balance_array)
index_hodler_df.sort_values('Balance').plot.bar(y='Balance', figsize=(24,12)).xaxis.set_visible(False)

index_hodler_df['percent_of_voting_supply'] = (index_hodler_df['cumulative_dist']/index_hodler_df['Balance'].sum()) * 100
index_hodler_df['percentile'] = (index_hodler_df.index/len(index_hodler_df)) * 100
index_hodler_df['HolderAddress'] = index_hodler_df['HolderAddress'].astype(str)
number_of_wallets_in_threshold = [min(index_hodler_df.index[index_hodler_df['percentile'] > percentile]) for percentile in top_hodler_percentile_thresholds]
max_percentile_thresholds = [0.1, 1.0, 10.0, 100.0]
percentile_members = dict()
for i in range(0, len(max_percentile_thresholds)):
    percentile = max_percentile_thresholds[i]
    prior_percentile_range_max = max_percentile_thresholds[i-1] if i > 0 else 0.0
    percentile_range = f'{str(prior_percentile_range_max)}-{max_percentile_thresholds[i]}%'
    total_n_members =  len(index_hodler_df.loc[index_hodler_df['percentile'] < percentile])
    range_member_df = index_hodler_df.loc[(index_hodler_df['percentile'] < percentile) & (index_hodler_df['percentile'] >= prior_percentile_range_max)]
    range_n_members = len(range_member_df)
    member_list = range_member_df.HolderAddress.tolist()
    percentile_members[percentile_range] = {
        'percentile_val': percentile,
        'total_n_members': total_n_members,
        'range_n_members': range_n_members,
        'member_list': member_list
    }
voter_df = pd.DataFrame()
voter_df['address'] = index_hodler_df['HolderAddress']

analysis_results = dict()
def run_vote_analysis(vote_key:str):
    analysis_results[vote_key] = dict()
    vote_url = vote_urls[vote_key]
    proposal_id = vote_url.split('/')[-1]
    vote_df = pd.read_csv(f'{local_download_folder_path}snapshot-report-{proposal_id}.csv')
    vote_df['address'] = vote_df.address.str.lower()
    vote_df = vote_df.sort_values('balance', ascending=False)
    vote_df['cumulative_dist'] = vote_df['balance'].cumsum()
    vote_df.index = range(len(vote_df))
    votes_for = round(vote_df.loc[vote_df['choice'] == 1, 'balance'].sum(), 2)
    voters_for = round(vote_df.loc[vote_df['choice'] == 1, 'choice'].sum(), 2)
    votes_against = round(vote_df.loc[vote_df['choice'] == 2, 'balance'].sum(), 2)
    voters_against = len(vote_df.loc[vote_df['choice'] == 2, 'choice'])
    print(f'Votes for: {votes_for}, Votes against: {votes_against}')
    prop_in_favor = (float(votes_for)/(votes_for + votes_against))
    print(f'Percent in favor: {round(prop_in_favor*100, 2)}%')
    print(f'Unique voters (wallets) for: {voters_for}, Unique voters (wallets) against: {voters_against}')
    print(f'Did the vote exceed the quorum threshold? {"Yes" if votes_for > votes_needed_for_quorum else "No"}')
    lowest_rank_balance_needed_to_pass = np.nan
    if votes_for > votes_needed_for_quorum and prop_in_favor > yes_vote_threshold:
        succeeded = 1
        print(f'This initiative succeeded. {votes_for} votes in favor > {votes_needed_for_quorum} votes needed to meet quorum')
        minimum_number_of_voters_needed = min(vote_df.index[vote_df['cumulative_dist'] > votes_needed_for_quorum]) + 1
        print(f'The minimum number of voting hodlers needed to pass this initiative was: {minimum_number_of_voters_needed}')
    else:
        succeeded = 0
        print(f'This initiative failed. {votes_for} votes in favor < {votes_needed_for_quorum} votes needed to meet quorum')
        votes_short_by = round(votes_needed_for_quorum - votes_for, 2)
        print(f'The minimum number of additional votes needed to pass was {votes_short_by}')
        if index_hodler_df['Balance'].max() > votes_short_by:
            lowest_rank_balance_needed_to_pass =  min(index_hodler_df.index[index_hodler_df['Balance'] > votes_short_by]) + 1
            print(f'This means the initiative could have passed if any wallet with balance ranked in the top {lowest_rank_balance_needed_to_pass} had voted')
    voter_balance_array = vote_df['balance'].to_numpy()
    compute_gini('All INDEX voter balances', voter_balance_array)
    vote_df.plot.bar(y='balance', figsize=(24,8)).xaxis.set_visible(False)
    if votes_for > votes_needed_for_quorum:
        quorum_threshold_index = min(vote_df.index[vote_df['cumulative_dist'] > votes_needed_for_quorum])
        plt.axvline(x=quorum_threshold_index, color='red')
    # Percent of top .1, 1 and 10% hodlers voting in initiative
    voter_addresses = vote_df['address'].tolist()
    analysis_results[vote_key] = {
        'votes_for': votes_for,
        'voters_for': voters_for,
        'votes_against': votes_against,
        'voters_against': voters_against,
        'prop_in_favor': prop_in_favor,
        'succeeded': succeeded,
        'votes_short_by': votes_short_by if not succeeded else 0,
        'minimum_number_of_voters_needed': minimum_number_of_voters_needed if succeeded else np.nan,
        'lowest_rank_balance_needed_to_pass': lowest_rank_balance_needed_to_pass,
    }
    for key, val in percentile_members.items():
        range_members_voted = len([member for member in val['member_list'] if member in voter_addresses])
        print(f'{range_members_voted} out of {val["range_n_members"]} members in the {key} range voted in this initiative')
        percent_voting = float(range_members_voted)/val['range_n_members'] * 100
        print(f'{round(percent_voting, 2)}% of wallets in {key} range voted in this initiative')
        analysis_results[vote_key][f'percent_{key}_voted'] = round(percent_voting, 2)
    voter_df[vote_key] = voter_df['address'].isin(voter_addresses)

for vote_key in vote_urls.keys():
    run_vote_analysis(vote_key)

# Voted in n initiatives
voter_df['num_votes'] = voter_df.select_dtypes(include=['bool']).sum(axis=1)
voted_at_least_once = len(voter_df.loc[voter_df["num_votes"]>=1])
percent_voted_at_least_once = round(100*voted_at_least_once/len(voter_df), 2)
print(f'Number of holders who have voted on at least 1 initiative: {voted_at_least_once} / {len(voter_df)} ({percent_voted_at_least_once}%)')
print('Vote count distribution: \n'+ str(voter_df.num_votes.value_counts()))

pd.DataFrame(voter_df.num_votes.value_counts()).loc[1:6, 'num_votes'].plot.bar()
plt.title("Number of holders with n DG2 votes")
plt.xlabel("Number of DG2 votes")
plt.ylabel("Number of wallets")

# Overall initiative comparison
analysis_results_df = pd.DataFrame(analysis_results)
analysis_results_df

analysis_results_transposed = analysis_results_df.transpose()
analysis_results_transposed

descriptives = analysis_results_df.apply(pd.Series.describe, axis=1)
descriptives

descriptives['percent_0.0-0.1%_voted':]['mean'].plot.bar()
plt.title('Average percent voting by percentile')
plt.ylabel('Percent voting')

analysis_results_transposed.loc[analysis_results_transposed['succeeded']==1, 'minimum_number_of_voters_needed'].plot.bar()
plt.title('Minimum number of voters needed to meet quorum')
plt.ylabel('Minimum number of voters needed to meet quorum')
