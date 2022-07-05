import pandas as pd
from yahooquery import Ticker
import numpy as np
from datetime import datetime
from datetime import timedelta
from finviz.screener import Screener
from logger.logger import logger

start = datetime.now() + timedelta(-365)
end = datetime.now()


def get_single_ticker_data(ticker,start_dt=start,end_dt=end,interval='1d',min_lag_period=60,period=None):
    valid_periods = ['1d', '5d', '7d', '60d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']
    if period is not None:
        if period not in valid_periods:
            raise Exception(f"Date provided must be one of these: {valid_periods}")

    if period is not None:
        t_df = Ticker(ticker).history(period=period, interval=interval, adj_ohlc=True)
    else:
        t_df = Ticker(ticker).history(start=start_dt, end=end_dt, interval=interval, adj_ohlc=True)
    if isinstance(t_df, pd.DataFrame) and len(t_df.index) > min_lag_period:
        zero_rows = len(t_df.loc[t_df['volume'] == 0].index)
        if zero_rows != 0:
            t_df.loc[t_df['volume'] == 0,'volume'] = 7

        #   -------------------------------------------
        #   Some yahoo records have duplicate rows
        #   -------------------------------------------
        idx = np.unique(t_df.index.values, return_index=True)[1]
        removing = len(t_df.index) - len(idx)
        if removing > 0:
            t_df = t_df.iloc[idx]

        t_df['ticker'] = t_df._get_label_or_level_values('symbol')
        t_df['dt'] = pd.to_datetime(t_df._get_label_or_level_values('date'))
        t_df.index = pd.to_datetime(t_df._get_label_or_level_values('date'))
    else:
        t_df = None
    return t_df.reset_index(drop=True)


def get_bb_stocks():
    return_df = None
    try:
        #filters = ['sh_price_o5']
        filters = ['sh_price_5to10']
        stock_list = Screener(filters=filters, table='Overview', order='price').data
        return_df = pd.DataFrame(stock_list)

    except Exception as e:
        logger.error(e)
    return return_df
