#Just direct file_path to the .db file and change the query accordingly

import sqlite3

def load_data(sqlContext, file_path=None):
    if file_path is None:
        file_path = "/Users/philkuz/projects/mlab/sap/StockData.db"

    conn = sqlite3.connect(file_path)
    c = conn.cursor()
    # c.execute("Create Index on StockData")
    # result = c.execute("SELECT ROW_NUMBER() OVER(ORDER BY Date) AS RowNumber, Close - Open as D0, Low as D1, High as D2, Volume as D3 FROM StockData WHERE Symbol == AA")
    result = c.execute("SELECT id, Close - Open as D0, Low as D1, High as D2, Volume as D3 FROM StockData WHERE Symbol='AA'")
    data = [r for r in result]

    df = sqlContext.createDataFrame(data, ['index', 'close-open', 'low', 'high', 'volume'])
    return df
def test():
    # note that not passing a parameter will default to Phillip's hard coding
    load_data().show()

if __name__ == "__main__":
    from pyspark import SparkContext
    from pyspark.sql import SQLContext

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    test()
