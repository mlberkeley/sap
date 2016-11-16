# theta0, 1, 2
# d_dataframe <- time indexed, column for each d_i from 0 <= i <= 4
# store in f_now, s.t. f_now[i] is d_0*theta0 + d_1*theta1 + ... + d_3*theta3

from pyspark.sql import *
from pyspark import SparkContext
sc = SparkContext()
sql = SQLContext(sc)
df = sql.createDataFrame([(1, 2, 3), (4, 5, 6), (7, 8, 9)], ['0', '1', '2'])
theta0, theta1, theta2 = 5, 6,7 
df_update = df.select((df['0'] * theta0).alias('0'), (df['1']*theta1).alias('1'),(df['2']*theta2).alias('2'))
newdf = df_update.withColumn('f_now', sum(df_update[col] for col in df_update.columns))
newdf = newdf.select('f_now').show()