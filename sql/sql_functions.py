import sqlalchemy as db
import configparser
import pandas as pd
import stocks
from logger.logger import logger

config = configparser.ConfigParser()
config.read('settings.ini')


def create_engine():
    return db.create_engine(f'mssql+pyodbc://'
                              f'{config["sql"]["username"]}:{config["sql"]["password"]}'
                              f'@{config["sql"]["server"]}/{config["sql"]["database"]}'
                              f'?driver={config["sql"]["driver"]}')


def create_stock_price_table():
    return 'CREATE TABLE Stock_Price (' \
               '[ticker] varchar(10),' \
               '[dt] datetime,' \
               '[open] float,' \
               '[high] float,' \
               '[low] float,' \
               '[close] float,' \
               '[adj_close] float,' \
                '[dividends] float,' \
                '[splits] float,' \
                '[volume] int,' \
           ');'


def get_ticker_max_date():
    return 'SELECT trim(ticker) AS ticker, Max(dt) AS dt ' \
           'FROM [Stocks].[dbo].[stock_price] ' \
           'GROUP BY ticker ' \
           'ORDER BY [ticker]'


def get_ticker_data(ticker=None,min_date=None,max_date=None):
    sql = f'SELECT * FROM [Stocks].[dbo].[stock_price] '
    if min_date is not None:
        sql += f"WHERE [dt] >= \'{min_date}\' "
    if max_date is not None:
        sql += f"and [dt] <= \'{max_date} \' "
    if ticker is not None:
        sql += f"and [ticker] = \'{ticker} \' "
    sql += f"ORDER BY [ticker],[dt]"
    return sql


def load_single_stock(ticker, start, end, open_close_conn=True, conn=None, engine=None):
    new_data = pd.DataFrame()
    if open_close_conn:
        engine = create_engine()
        conn = engine.connect()
    try:
        data = stocks.get_single_ticker_data(ticker, start_dt=start, end_dt=end)
        db_data = pd.read_sql(get_ticker_data(ticker=ticker, min_date=data['dt'].min(), max_date=data['dt'].max()),
                              conn)
        db_data['ticker'] = db_data['ticker'].str.strip()

        new_data = data.merge(db_data, how='outer', on=['ticker', 'dt'], suffixes=('', '_drop'))
        new_data = new_data[new_data['close_drop'].isna()]
        new_data.drop([col for col in new_data.columns if 'drop' in col], axis=1, inplace=True)

        if len(new_data.index) > 0:
            new_data.to_sql('Stock_Price', engine, index=False, if_exists="append", schema="dbo")
            logger.info(f'Ticker: {ticker} - Loaded {len(new_data.index)} records')
        else:
            logger.info(f'Ticker: {ticker} - Data load up to date')
    except Exception as e:
        logger.error(e)
    finally:
        if open_close_conn:
            conn.close()
            engine.dispose()


def get_tickers_max_date():
    engine = create_engine()
    conn = engine.connect()

    db_data = None
    try:
        db_data = pd.read_sql(get_ticker_max_date(),conn)
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()
        engine.dispose()
    return db_data

