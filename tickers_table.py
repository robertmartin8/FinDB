import pandas as pd
import pymysql.cursors


db_host = 'localhost'
db_user = 'root'
db_pass = 'mysql'
db_name = 'stock_prices'
conn = pymysql.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
cursor = conn.cursor()


def write_exchanges():
    cursor.execute(
        "INSERT INTO exchange (name, currency) VALUES ('NYSE', 'USD')")
    cursor.execute(
        "INSERT INTO exchange (name, currency) VALUES ('NASDAQ', 'USD')")
    cursor.execute(
        "INSERT INTO exchange (name, currency) VALUES ('NYSEARCA', 'USD')")
    conn.commit()


yahoo = pd.read_csv('data/all_yahoo_tickers.csv')
yahoo_tickers = yahoo['Ticker'].dropna()


def write_etf_tickers():
    etf_tickers = pd.read_csv(
        'data/ETFs_Tickers.csv')['ticker'].values.tolist()

    etf_ticker_names = pd.read_csv('data/nasdaq_etf.csv')[['Symbol', 'Name']]
    etf_ticker_names = dict(etf_ticker_names.to_dict('split')['data'])

    us_tickers = [t for t in etf_tickers if "." not in t and len(
        t) < 11 and "#" not in t]
    for t in us_tickers:
        if t in etf_ticker_names:
            cursor.execute("INSERT INTO security (exchange_id, ticker, name, sector, industry) VALUES"
                           "(%s, %s, %s, %s, %s)", (3, t, etf_ticker_names[t], "ETF", "ETF"))
        else:
            cursor.execute("INSERT INTO security (exchange_id, ticker, name, sector, industry) VALUES"
                           "(%s, %s, %s, %s, %s)", (3, t, f"{t} ETF", "ETF", "ETF"))
    conn.commit()


def write_nyse_tickers():
    available_tickers = yahoo_tickers.str.split('.').str[0]

    nyse = pd.read_csv('data/nyse_tickers.csv')
    print("Number of NYSE tickers:", len(nyse))
    nyse.drop(['LastSale', 'MarketCap', 'IPOyear', 'Summary Quote',
               'Unnamed: 9', 'ADR TSO'], axis=1, inplace=True)
    nyse_yahoo = nyse[nyse['Symbol'].isin(available_tickers)].copy()
    print("Number of NYSE tickers on yahoo finance:", len(nyse_yahoo))
    nyse_yahoo.columns = ['ticker', 'name', 'sector', 'industry']
    nyse_yahoo['exchange_id'] = 1
    cols = list(nyse_yahoo.columns)
    nyse_yahoo = nyse_yahoo[cols[-1:] + cols[:-1]]

    for row in nyse_yahoo.itertuples(index=False):
        try:
            cursor.execute("INSERT INTO security (exchange_id, ticker, name, sector, industry)"
                           "VALUES (%s, %s, %s, %s, %s)", row)
        except:
            cursor.execute(
                "INSERT INTO security (exchange_id, ticker, name) VALUES (%s, %s, %s)", row[:3])
    conn.commit()


def write_nasdaq_tickers():
    available_tickers = yahoo_tickers.str.split('.').str[0]

    nasdaq = pd.read_csv('data/nasdaq_tickers.csv')
    print("Number of NASDAQ tickers:", len(nasdaq))
    nasdaq.drop(['LastSale', 'MarketCap', 'IPOyear', 'Summary Quote',
                 'Unnamed: 9', 'ADR TSO'], axis=1, inplace=True)
    nasdaq_yahoo = nasdaq[nasdaq['Symbol'].isin(available_tickers)].copy()
    print("Number of NASDAQ tickers on yahoo finance:", len(nasdaq_yahoo))
    nasdaq_yahoo.columns = ['ticker', 'name', 'sector', 'industry']
    nasdaq_yahoo['exchange_id'] = 2
    cols = list(nasdaq_yahoo.columns)
    nasdaq_yahoo = nasdaq_yahoo[cols[-1:] + cols[:-1]]

    for row in nasdaq_yahoo.itertuples(index=False):
        try:
            cursor.execute("INSERT INTO security (exchange_id, ticker, name, sector, industry)"
                           "VALUES (%s, %s, %s, %s, %s)", row)
        except:
            cursor.execute(
                f"INSERT INTO security (exchange_id, ticker, name) VALUES (%s, %s, %s)", row[:3])
    conn.commit()


def reset():
    cursor.execute("DELETE FROM security")
    cursor.execute("ALTER TABLE security AUTO_INCREMENT=1")
    conn.commit()


if __name__ == '__main__':
    # write_exchanges()
    # write_etf_tickers()
    write_nyse_tickers()
    write_nasdaq_tickers()
