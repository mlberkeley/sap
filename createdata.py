#Just direct file_path to the .db file and change the query accordingly

import sqlite3
from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext()
sql = SQLContext(sc)

file_path = "/Users/RahilMathur/quant/databases/StockData.db"

conn = sqlite3.connect(file_path)
c = conn.cursor()

result = c.execute("Select Close - Open as D0, Low as D1, High as D2, Volume as D3 from StockData limit 100")
data = [r for r in result]

df = sql.createDataFrame(data, ['D0', 'D1', 'D2', 'D3'])
df.show()
