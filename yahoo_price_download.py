from pandas_datareader import data as pdr
import pandas as pd
import fix_yahoo_finance as yf
import time
import pymysql.cursors
yf.pdr_override()


db_host = 'localhost'
db_user = 'root'
db_pass = 'mysql'
db_name = 'stock_prices'
conn = pymysql.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
cursor = conn.cursor()


def add_data_vendor():
    cursor.execute(
        "INSERT INTO data_vendor (name, website_url) VALUES ('YahooFinance', 'https://finance.yahoo.com')")
    conn.commit()


YAHOO_VENDOR_ID = 1
all_tickers = pd.read_sql("SELECT ticker, id FROM security", conn)
ticker_index = dict(all_tickers.to_dict('split')['data'])
tickers = list(ticker_index.keys())


def download_data_chunk(start_idx, end_idx, tickerlist, start_date=None):
    """
    Download stock data using pandas-datareader
    :param start_idx: start index
    :param end_idx: end index
    :param tickerlist: which tickers are meant to be downloaded
    :param start_date: the starting date for each ticker
    :return: writes data to mysql database
    """
    ms_tickers = []
    for ticker in tickerlist[start_idx:end_idx]:
        print(f"{tickerlist.index(ticker)}/{len(tickerlist)} ticker")
        df = pdr.get_data_yahoo(ticker, start=start_date)

        if df.empty:
            print(f"df is empty for {ticker}")
            ms_tickers.append(ticker)
            time.sleep(3)
            continue

        for row in df.itertuples():
            values = [YAHOO_VENDOR_ID, ticker_index[ticker]] + list(row)
            values[2] = values[2].strftime('%Y-%m-%d %H:%M:%S')
            try:
                cursor.execute("INSERT INTO daily_price (data_vendor_id, ticker_id, price_date, open_price, "
                               "high_price, low_price, close_price, adj_close_price, volume) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               tuple(values))
            except Exception as e:
                print(str(e))
        conn.commit()
        return ms_tickers


def download_all_data(tickerlist, chunk_size=100, start_date=None):
    n_chunks = -(-len(tickerlist) // chunk_size)

    ms_tickers = []
    for i in range(n_chunks):
        start = 100 * i
        end = 100 * i + chunk_size
        print(start, end)
        # This will download data from the earliest possible date
        ms_from_chunk = download_data_chunk(start, end, tickerlist, start_date)
        ms_tickers.append(ms_from_chunk)
        # If more than 40 tickers are missing, it's probably some throttle issue
        if len(ms_from_chunk) > 40:
            time.sleep(120)
        else:
            time.sleep(10)
    return ms_tickers


def second_pass_download(missing_tickers):
    # get current tickers in df
    ms_tickers = []
    for ticker in missing_tickers:
        df = pdr.get_data_yahoo(ticker)
        if df.empty:
            print(f"df is empty for {ticker}")
            time.sleep(3)
            ms_tickers.append(ticker)
            continue

        for row in df.itertuples():
            row_list = list(row)
            values = [YAHOO_VENDOR_ID, ticker_index[ticker]] + row_list
            values[2] = values[2].strftime('%Y-%m-%d %H:%M:%S')
            try:
                cursor.execute("INSERT INTO daily_price (data_vendor_id, ticker_id, price_date, open_price, "
                               "high_price, low_price, close_price, adj_close_price, volume) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               tuple(values))
            except Exception as e:
                print(str(e))
        conn.commit()

    return ms_tickers


second_pass_download(['MON', 'BF-B'])


def update_prices():
    # get present tickers
    present_ticker_ids = pd.read_sql(
        "SELECT DISTINCT ticker_id FROM daily_price", conn)
    index_ticker = {v: k for k, v in ticker_index.items()}
    present_tickers = [index_ticker[i]
                       for i in list(present_ticker_ids['ticker_id'])]
    # Because all tickers are updated on the same day, we can just
    # find the last date for any ticker
    dates = pd.read_sql(
        "SELECT price_date FROM daily_price WHERE ticker_id=1", conn)
    last_date = dates.iloc[-1, 0]
    download_all_data(present_tickers, start_date=last_date)


def reset():
    cursor.execute("DELETE FROM daily_price")
    cursor.execute("ALTER TABLE daily_price AUTO_INCREMENT=1")
    conn.commit()


if __name__ == '__main__':
    # add_data_vendor()
    # missing = download_all_data(tickers)
    second_pass_download(['MON', 'BF-B'])
