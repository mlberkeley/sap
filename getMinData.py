import boto3
import urllib.request
import json

# Create Table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stock-v1')

# Parameters Initialization
stocks = ['AAPL', 'GOOG', 'SPY', 'YHOO']
    #stocks = open("nasdaqSymbols.txt", "r").read().split("\n")

urlPrefix = "http://www.bloomberg.com/markets/api/bulk-time-series/price/"
urlAffix = "%3AUS?timeFrame="
options = ["1_DAY"]
    #options = ["1_DAY", "1_MONTH", "1_YEAR", "5_YEAR"]

i = 0
while i < len(stocks):
    for option in options:
        symbol = stocks[i]
        htmltext = urllib.request.urlopen(urlPrefix + symbol + urlAffix + option).read().decode('utf8')
        try:
            data = json.loads(htmltext)[0]
            lastClose = data["previousClosingPriceOneTradingDayAgo"]
            with table.batch_writer(overwrite_by_pkeys=['symbol', 'dateTime']) as batch:
                for value in data["price"]:
                    batch.put_item(
                        Item={
                            'symbol': symbol,
                            'dateTime': value["dateTime"],
                            'price': str(value["value"]),
                            'lastClose': str(lastClose)
                        }
                    )
        except Exception as e:
            print("failed :" + str(e))
    i += 1

