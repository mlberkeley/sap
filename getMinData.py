import boto3
import urllib.request
import json

# Create Table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stock-v1')

# Parameters Initialization
stocks = open("s&p500.txt", "r").read().split("\n")

# URL Format
urlPrefix = "https://query2.finance.yahoo.com/v7/finance/chart/"
url2 = "?period2="
period2 = str(1476914709)
url3 = "&period1="
period1 = str(1476569109)
url4 = "&interval="
interval = "1m"
url5 = "&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com"

i = 0
while i < len(stocks):
    symbol = stocks[i]
    url = urlPrefix + symbol + url2 + period2 + url3 + period1 + url4 + interval + url5
    htmltext = urllib.request.urlopen(url).read().decode('utf8')
    try:
        data = json.loads(htmltext)["chart"]["result"][0]
        quote = data["indicators"]["quote"][0]
        _timestamp = data["timestamp"]
        _close = quote["close"]
        _high = quote["high"]
        _low = quote["low"]
        _open = quote["open"]
        _volume = quote["volume"]
        with table.batch_writer(overwrite_by_pkeys=['symbol', 'timestamp']) as batch:
            for i in range(len(_timestamp)):
                batch.put_item(
                    Item={
                        'symbol': symbol,
                        'timestamp': str(_timestamp[i]),
                        'close': str(_close[i]),
                        'high': str(_high[i]),
                        'low': str(_low[i]),
                        'open': str(_open[i]),
                        'volume': str(_volume[i])
                    }
                )
    except Exception as e:
        pass
    i += 1

