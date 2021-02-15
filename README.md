## FinDB

*Warning: this project is not maintained and may be outdated.*

This is a small project which aims to create a local SQL databse of stock prices for use in research projects, accompanying a [blog post](https://reasonabledeviations.com/2018/02/01/stock-price-database/) I wrote on the subject.

When I wrote the code, the best way to get free stock data was `pandas-datareader` with `fix-yahoo-finance`. Nowadays, that is no longer necessary thanks to the excellent `yfinance` library ([link](https://pypi.org/project/yfinance/)). The code in `yahoo_price_download.py` should probably be updated.  

## Data sources

- [index/mutual fund](https://github.com/MichaelDz6/Yahoo_Finance_ETFs_Web_Scraper)
- [nyse/nasdaq: official website](https://www.nasdaq.com/screening/company-list.aspx - http://investexcel.net/all-yahoo-finance-stock-tickers/)
