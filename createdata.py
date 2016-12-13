#Just direct file_path to the .db file and change the query accordingly

import sqlite3

def load_data(sqlContext, file_path=None):
    if file_path is None:
        file_path = "StockData.db"

    conn = sqlite3.connect(file_path)
    c = conn.cursor()
    # c.execute("Create Index on StockData")
    # result = c.execute("CREATE TABLE aaData AS SELECT sd.id, sd.Close - sd.Open as D0, sd.Low as D1, sd.High as D2, sd.Volume as D3 FROM StockData sd WHERE sd.Symbol='AA'  ORDER BY date(Date) DESC")
    # result = c.execute("SELECT id, Close - Open as D0, Low as D1, High as D2, Volume as D3 FROM StockData WHERE Symbol='AA' ORDER BY date(Date) DESC")
    result = c.execute("SELECT * from aaData")
    data = [r for r in result]

    df = sqlContext.createDataFrame(data, ['index', 'close-open', 'low', 'high', 'volume'])
    return df
def test():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    # note that not passing a filename for the database will default to Phillip's hard coding
    load_data(sqlContext).show()

if __name__ == "__main__":

    test()
