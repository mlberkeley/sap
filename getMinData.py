import boto3
import urllib.request
import json
import datetime
import time

# Create Table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stock-v1')

# Parameters Initialization
stocks = open("s&p500.txt", "r").read().split("\n")

# URL Format
urlPrefix = "https://query2.finance.yahoo.com/v7/finance/chart/"
url2 = "?period="
url3 = "&interval="
interval = "1m"
url4 = "&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com"
today = str(time.mktime(datetime.date.today().timetuple()))


i = 0
while i < len(stocks):
    symbol = stocks[i]
    url = urlPrefix + symbol + url2 + today + url3 + interval + url4
    htmltext = urllib.request.urlopen(url).read().decode('utf8')
    try:
        print("adding values for " + symbol +" stock quotes ... ")
        data = json.loads(htmltext)["chart"]["result"][0]
        quote = data["indicators"]["quote"][0]
        with table.batch_writer(overwrite_by_pkeys=['symbol', 'timestamp']) as batch:
            for j in range(len(data["timestamp"])):
                _timestamp = str(data["timestamp"][j])
                _close = str(quote["close"][j])
                _high = str(quote["high"][j])
                _low = str(quote["low"][j])
                _open = str(quote["open"][j])
                _volume = str(quote["volume"][j])
                if _timestamp != 'None' and _close != 'None' and _high != 'None':
                    if _low != 'None' and _open != 'None' and _volume != 'None':
                        batch.put_item(
                            Item={
                                'symbol': symbol,
                                'timestamp': _timestamp,
                                'close': _close,
                                'high': _high,
                                'low': _low,
                                'open': _open,
                                'volume': _volume
                            }
                        )
        print("success \n")
    except Exception as e:
        print(str(e))
    i += 1

