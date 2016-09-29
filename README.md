# yahoo-finance #

Python module to get stock data from Yahoo! Finance

# General Info -- Michael Tu #

## Yahoo API ##
This Git Repo uses Yahoo's Private API Keys.

You can register new private APIs keys [here](https://developer.yahoo.com/yql/) under step 2 of “Get Started”.

Yahoo Finance API Brief Intro [here](http://meumobi.github.io/stocks%20apis/2016/03/13/get-realtime-stock-quotes-yahoo-finance-api.html)


## Yahoo API Query Limit ##

### Private API Query Limit ###
* limited to 20,000 requests per hour per IP
* limited to a total of 100,000 requests per day per API Key.

### Public API (without authentication) Query Limit ###
* limited to 2,000 requests per hour per IP
* limited to a total of 48,000 requests a day


# Install #

Using pip:

```
#!shell

$ pip install yahoo-finance
```

From development repo (requires git)

```
#!shell
$ git clone git://github.com/lukaszbanasiak/yahoo-finance.git
$ cd yahoo-finance
$ python setup.py install
```

# Data Values -- Michael Tu #
All avaliable data values are in shown in the "data_set_keys.txt" file.

Not all values can be obtained through a ``.get()`` method. 

Only some of those values are implemented as shown below.

To get the data_set, use ``.get_data_set()`` method. The data_set is in a dictionary format.

## Avalible methods for Currency ##


* ``get_bid()``
* ``get_ask()``
* ``get_rate()``
* ``get_trade_datetime()``
* ``refresh()``

## Avalible methods for Shares ##

* ``get_price()``
* ``get_change()``
* ``get_percent_change()``
* ``get_volume()``
* ``get_prev_close()``
* ``get_open()``
* ``get_avg_daily_volume()``
* ``get_stock_exchange()``
* ``get_market_cap()``
* ``get_book_value()``
* ``get_ebitda()``
* ``get_dividend_share()``
* ``get_dividend_yield()``
* ``get_earnings_share()``
* ``get_days_high()``
* ``get_days_low()``
* ``get_year_high()``
* ``get_year_low()``
* ``get_50day_moving_avg()``
* ``get_200day_moving_avg()``
* ``get_price_earnings_ratio()``
* ``get_price_earnings_growth_ratio()``
* ``get_price_sales()``
* ``get_price_book()``
* ``get_short_ratio()``
* ``get_trade_datetime()``
* ``get_historical(start_date, end_date)``
* ``get_info()``
* ``get_name()``
* ``refresh()``

# Usage examples #



## Get currency data ##

Example: EUR/PLN (EURPLN=X)
    
```
#!shell

    >>> from yahoo_finance import Currency
    >>> eur_pln = Currency('EURPLN=X')
    >>> print(eur_pln.get_bid())
    '4.2007'
    >>> print(eur_pln.get_ask())
    '4.2091'
    >>> print(eur_pln.get_rate())
    '4.2049'
    >>> print(eur_pln.get_trade_datetime())
    '2014-03-05 11:23:00 UTC+0000'
```


Refresh data from market


   
```
#!shell

    >>> eur_pln.refresh()
    >>> print(eur_pln.get_rate())
    '4.2052'
    >>> print(eur_pln.get_trade_datetime())
    '2014-03-05 11:27:00 UTC+0000'
```


## Get shares data ##

Example: Yahoo! Inc. (YHOO)

```
#!shell

    >>> from yahoo_finance import Share
    >>> yahoo = Share('YHOO')
    >>> print(yahoo.get_open())
    '36.60'
    >>> print(yahoo.get_price())
    '36.84'
    >>> print(yahoo.get_trade_datetime())
    '2014-02-05 20:50:00 UTC+0000'
```



Refresh data from market
  
```
#!shell

    >>> yahoo.refresh()
    >>> print(yahoo.get_price())
    '36.87'
    >>> print(yahoo.get_trade_datetime())
    '2014-02-05 21:00:00 UTC+0000'
```

Historical data


   
```
#!shell

    >>> print(yahoo.get_historical('2014-04-25', '2014-04-29'))
    [{u'Volume': u'28720000', u'Symbol': u'YHOO', u'Adj_Close': u'35.83', u'High': u'35.89', u'Low': u'34.12', u'Date': u'2014-04-29', u'Close': u'35.83', u'Open': u'34.37'}, {u'Volume': u'30422000', u'Symbol': u'YHOO', u'Adj_Close': u'33.99', u'High': u'35.00', u'Low': u'33.65', u'Date': u'2014-04-28', u'Close': u'33.99', u'Open': u'34.67'}, {u'Volume': u'19391100', u'Symbol': u'YHOO', u'Adj_Close': u'34.48', u'High': u'35.10', u'Low': u'34.29', u'Date': u'2014-04-25', u'Close': u'34.48', u'Open': u'35.03'}]
```

More readable output :)

```
#!shell


    >>> from pprint import pprint
    >>> pprint(yahoo.get_historical('2014-04-25', '2014-04-29'))
    [{u'Adj_Close': u'35.83',
      u'Close': u'35.83',
      u'Date': u'2014-04-29',
      u'High': u'35.89',
      u'Low': u'34.12',
      u'Open': u'34.37',
      u'Symbol': u'YHOO',
      u'Volume': u'28720000'},
     {u'Adj_Close': u'33.99',
      u'Close': u'33.99',
      u'Date': u'2014-04-28',
      u'High': u'35.00',
      u'Low': u'33.65',
      u'Open': u'34.67',
      u'Symbol': u'YHOO',
      u'Volume': u'30422000'},
     {u'Adj_Close': u'34.48',
      u'Close': u'34.48',
      u'Date': u'2014-04-25',
      u'High': u'35.10',
      u'Low': u'34.29',
      u'Open': u'35.03',
      u'Symbol': u'YHOO',
      u'Volume': u'19391100'}]
```

Summary information for our example

  
```
#!shell

    >>> from pprint import pprint
    >>> pprint(yahoo.get_info())
    {u'FullTimeEmployees': u'12200',
     u'Industry': u'Internet Information Providers',
     u'Sector': u'Technology',
     u'end': u'2014-05-03',
     u'start': u'1996-04-12',
     u'symbol': u'YHOO'}
```

Requirements
------------

See ``requirements.txt``