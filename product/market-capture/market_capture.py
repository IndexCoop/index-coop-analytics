# _*_ encoding:utf-8 _*_
# This script calculates index market capture by day through coingekco api
# market capture = index market cap / sum(each composition's market cap in the index )

# prerequisite:
#     1. install coingecko api python library  https://github.com/man-c/pycoingecko
#     2. prepare index compositions info as a csv file which contain the info about when a coin is added
#        or removed from the index and its id in coingecko. e.g. dpi_index.csv, mvi_index.csv.

# maintenance: each time a coin is added or removed from a index the csv file must change accordingly.

# result is saved as a csv file which contains the index market capture by day.


from pycoingecko import CoinGeckoAPI
import pandas as pd
import numpy as np
import time
import datetime


today = datetime.datetime.now().strftime("%Y-%m-%d")
# connect coingecko api
cg = CoinGeckoAPI()

def time_to_unix(str_time):
    """
    convert str time tp unix timestamp
    :param str_time: yyyy-mm-dd
    :return: timestamp
    """
    return time.mktime(time.strptime(str_time, "%Y-%m-%d"))


def get_index_compositions_market_cap(compositions_table):
    """
    get index compositions market cap by day
    :param compositions_table: dataframe which contains index composition info
    :return: dataframe which is index compositions marketcap by day
    """
    coins_cap = pd.DataFrame(columns=['dates','coinscap','coins'])
    count = 0
    for coin in compositions_table.values:
        coin_id = coin[4]
        from_timestamp = time_to_unix(coin[2])
        if coin[2] == coin[3]:
            to_timestamp = time_to_unix(today)
        else:
            to_timestamp = time_to_unix(coin[3])
        datas = cg.get_coin_market_chart_range_by_id(id=coin_id,vs_currency='usd',from_timestamp=from_timestamp,to_timestamp=to_timestamp)
        # waxe has no market cap data,so use Fully Diluted Market Cap instead
        if coin_id == 'waxe':
            datas_df = pd.DataFrame(datas['prices'],columns=['dates','coinscap'])
            datas_df['coinscap'] = datas_df['coinscap']*3700000
        else:
            datas_df = pd.DataFrame(datas['market_caps'],columns=['dates','coinscap'])
        datas_df['coins'] = coin[1]
        coins_cap=coins_cap.append(datas_df)
        time.sleep(5)
        count += 1
        print('round %d ,get market cap of %s'%(count,coin_id))
    coins_cap['days'] = pd.to_datetime(coins_cap['dates'], unit='ms').dt.date
    coins_cap = coins_cap.groupby(['coins', 'days']).nth(0).reset_index()
    coins_cap = coins_cap.groupby('days')['coinscap'].sum().reset_index()
    return coins_cap


def get_index_market_cap(id,from_time):
    """
    get index marketcap
    :param id: coingekco id
    :param from_time: index start time yyyy-mm-dd
    :return: dataframe which contains days and marketcap
    """
    from_timestamp = time_to_unix(from_time)
    to_timestamp = time_to_unix(today)
    index_caps = cg.get_coin_market_chart_range_by_id(id=id, vs_currency='usd',
                                                    from_timestamp=from_timestamp, to_timestamp=to_timestamp)
    index_df = pd.DataFrame(index_caps['market_caps'], columns=['dates', 'index_marketcap'])
    index_df['days'] = pd.to_datetime(index_df['dates'], unit='ms').dt.date
    index_df = index_df.drop(columns='dates')
    index_df = index_df.groupby('days').nth(0).reset_index()
    return index_df


def get_index_market_capture(index_info_dir,id,from_time):
    """
    get index market capture
    :param index_info_dir: dir of index info table
    :param id: coingecko id of index
    :param from_time: index start time yyyy-mm-dd
    :return: dataframe, compositions and index market cap by day
    """
    # read dpi composition info
    index_table  = pd.read_csv(index_info_dir)
    coins_cap = get_index_compositions_market_cap(index_table)
    index_cap = get_index_market_cap(id,from_time)
    market_capture = index_cap.merge(coins_cap, on='days', how='left')
    market_capture['market_capture'] = market_capture['index_marketcap'] / market_capture['coinscap']*100
    return market_capture.round(3)


if __name__ == '__main__':
    # dpi market capture
    dpi_market_capture = get_index_market_capture(index_info_dir='./dpi_index.csv',id='defipulse-index',from_time='2020-09-10')
    # save result as dpi_market_capture.csv
    dpi_market_capture.to_csv('./dpi_market_capture.csv',columns = ['days','index_marketcap','coinscap','market_capture'],index=False)

    # mvi market capture
    mvi_market_capture = get_index_market_capture(index_info_dir='./mvi_index.csv',id='metaverse-index',from_time='2021-04-06')
    # save result as mvi_market_capture.csv
    mvi_market_capture.to_csv('./mvi_market_capture.csv',columns = ['days','index_marketcap','coinscap','market_capture'],index=False)


