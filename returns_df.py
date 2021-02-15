import pymysql.cursors
import pandas as pd


# Portfolio of 20 tickers
ticker_list = [
    "GOOG",
    "AAPL",
    "FB",
    "BABA",
    "AMZN",
    "GE",
    "AMD",
    "WMT",
    "BAC",
    "GM",
    "T",
    "UAA",
    "SHLD",
    "XOM",
    "RRC",
    "BBY",
    "MA",
    "PFE",
    "JPM",
    "SBUX",
]

conn = pymysql.connect("localhost", "root", "mysql", "stock_prices")
cursor = conn.cursor()

all_data = []
for ticker in ticker_list:
    cursor.execute("SELECT id FROM security WHERE ticker=%s", (ticker,))
    ticker_id = cursor.fetchone()[0]

    stock_df = pd.read_sql(
        f"SELECT price_date, adj_close_price FROM daily_price WHERE ticker_id={ticker_id}",
        conn,
    )
    stock_df = stock_df.drop_duplicates()
    stock_df.columns = ['date', ticker]
    stock_df = stock_df.set_index(
        pd.to_datetime(stock_df["date"])
    ).drop("date", axis=1)
    if len(stock_df) < 500:
        print(ticker)
        continue
    all_data.append(stock_df)

df = pd.concat(all_data, axis=1)
# df = df.pct_change()
df = df["1989-12-29":]
df.to_csv("stock_returns.csv")

conn.close()