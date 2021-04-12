# if you dont have the binance library,
# do pip install python-binance

from binance.client import Client # pip install python-binance
import pandas as pd # pip install pandas
import numpy as np # pip install numpy


############### parameters #############
credentials_file = ''   # a file that holds your API and Secret keys, eg. 'b.txt'
PAIR = "BTTUSDT" # name of crypto pair that you want to download its historical data
PERIOD = "15m" # candlestick interval eg. 5m,15m,1h,4h,1d,1w

# dont change below, info retreive follows this order
CANDLE_INFO = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time",\
               "Quote asset volume", "Number of trades", "Taker buy base asset volume",\
               "Taker buy quote asset volume", "Ignore"]
########################################

# get API and secret key from file
with open(credentials_file,'r') as f:
    API_KEY, SEC_KEY = f.read().split('\n')


client = Client(API_KEY, SEC_KEY)  #connecting


def to_dataframe(candles, info_list):
    '''
    function to convert data to dataframe
    '''
    dic = {desc:[] for desc in  info_list}
    for candle in candles:
        
        for idx, data in enumerate(candle):
            dic[info_list[idx]].append(float(data))  
    df = pd.DataFrame(dic);df.set_index('Open time', inplace=True)
    return df


def download_pair(pair_name, interval, C_info, client=client):
    '''
    main function to download historical data of a given crypto pair
    and returns it as a pandas dataframe for easy processing/analysis
    '''
    frames = []
    
    # get earliest timestamp
    timestamp = client._get_earliest_valid_timestamp(pair_name, interval)
    
    # get dataframe from earliest timestamp
    data = client.get_historical_klines(pair_name, interval, timestamp, limit=999999999)
    frames.append(to_dataframe(data, C_info))
    
    # get last timestamp in the dataframe
    timestamp = int(frames[-1].tail(1).index.values[0])
    
    # get another dataframe from last timestamp
    data = client.get_historical_klines(pair_name, interval, timestamp, limit=999999999)
    frames.append(to_dataframe(data, C_info))
    
    #while timestamps are mismatch, get more data
    while frames[-2].tail(1).index != frames[-1].tail(1).index:
        timestamp = int(frames[-1].tail(1).index.values[0])
        data = client.get_historical_klines(pair_name, interval, timestamp, limit=999999999)
        frames.append(to_dataframe(data, C_info))
        
    return pd.concat(frames).drop_duplicates()

def save_pair(df, pair_name, period=PERIOD):
    '''
    helper function to seve data as a csv file
    '''
    df.to_csv(f'{pair_name}{period}.csv')



df = download_pair(PAIR, PERIOD, CANDLE_INFO)
save_pair(df, PAIR, PERIOD)
