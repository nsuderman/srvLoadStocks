from datetime import datetime as dt
from datetime import timedelta
import numpy as np
import stocks
from logger.logger import logger
from multiprocessing.dummy import Pool as ThreadPool
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sql import sql_functions as sql

start = dt(2010, 1, 1)
end = dt(dt.now().year, dt.now().month, dt.now().day)
start_last_week = end + timedelta(days=-7)

thread_index = 0


def get_stocks_dates():
    ticker_dates = sql.get_tickers_max_date()
    stock_obj = stocks.get_bb_stocks()['Ticker']
    stock_obj.name = "ticker"
    stock_obj = stock_obj.to_frame()
    tickers = ticker_dates.merge(stock_obj, on='ticker',how="outer")
    tickers["dt"].fillna(start, inplace=True)
    return tickers


def load_stocks_from_list():
    # ~~~~~~~~~~~~~~~~~~Get max stock date to not run processes if not needed
    global end
    end = stocks.get_single_ticker_data("QQQ", start_dt=start_last_week, end_dt=end,min_lag_period=0)['dt'].max()

    # ~~~~~~~~~~~~~~~~~~get stock list and start dates
    tickers = get_stocks_dates()
    ticker_list = tickers.to_dict(orient='records')

    work_parallel(ticker_list)

    '''
     # ~~~~~~~~~~~~~~~~~~Set DB Connection
    engine = sql.create_engine()
    conn = engine.connect()

    # ~~~~~~~~~~~~~~~~~~Loop lists for all stock tickers
    try:
        l = len(tickers.index)
        for i, row in tickers.iterrows():
            stock = row['ticker']
            max_datetime = row['dt']

            if max_datetime < end:
                logger.info(f'Loading {stock} - {i} of {l} {(i/l):.2%}')
                sql.load_single_stock(ticker=stock, start=max_datetime, end=end,open_close_conn=False,conn=conn,engine=engine)
            else:
                logger.info(f'Ticker: {stock} - Data load up to date {i} of {l} {(i/l):.2%}')
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()
        engine.dispose()
    '''

    a=1


def thread_worker(stock_list_dict):
    global thread_index
    thread_index += 1
    index = thread_index
    f1(stock_list_dict, index)


def f1(stock_list_dict, index):
    stock = stock_list_dict["ticker"]
    max_datetime = stock_list_dict["dt"]
    length = stock_list_dict["length"]

    session = Session()
    try:
        if max_datetime < end:
            logger.info(f'Loading {stock} - {index} of {length} {(index / length):.2%}')
            sql.load_single_stock(ticker=stock, start=max_datetime, end=end, open_close_conn=False, conn=conn,
                                  engine=engine)
        else:
            logger.info(f'Ticker: {stock} - Data load up to date {i} of {l} {(i / l):.2%}')
    except Exception as e:
        logger.error(e)


    print(f'Ticker: {stock_list_dict["ticker"]} - Datetime: {stock_list_dict["dt"]} - '
          f'thread index: {index} - Length {stock_list_dict["length"]}')
    # now all calls to Session() will create a thread-local session.
    # If we call upon the Session registry a second time, we get back the same Session.
    #session = Session()
    # do something around the session and the number...

    # You can even directly use Session to perform DB actions.
    # See: https://docs.sqlalchemy.org/en/13/orm/contextual.html#implicit-method-access
    # when methods are called on the Session object, they are proxied to the underlying Session being maintained by the registry.


def work_parallel(stock_list_dict, thread_number=4):
    i = 0
    for dic in stock_list_dict:
        i+=1
        dic['length'] = len(stock_list_dict)

    pool = ThreadPool(thread_number)
    results = pool.map(thread_worker, stock_list_dict)
    # If you don't care about the results, just comment the following 3 lines.
    # pool.close()
    # pool.join()
    # return results



if __name__ == '__main__':

    load_stocks_from_list()

    engine = sql.create_engine()
    session_factory = sessionmaker(bind=engine)

    # The Session object created here will be used by the function f1 directly.
    Session = scoped_session(session_factory)

    numbers = [1, 2, 3]
    work_parallel(numbers, 8)

    Session.remove()
    #load_single_stock(ticker="AA", start=start, end=end))


    a=1





