# theta0, 1, 2
# d_dataframe <- time indexed, column for each d_i from 0 <= i <= 4
# store in f_now, s.t. f_now[i] is d_0*theta0 + d_1*theta1 + ... + d_3*theta3

from pyspark.sql import *

d_0 = d_dataframe.col(0).map(lambda x: theta0 * x)
d_1 = d_dataframe.col(1).map(lambda x: theta1 * x)
d_2 = d_dataframe.col(2).map(lambda x: theta2 * x)

df_update = sqlContext.createDataFrame(d_0, d_1, d_2)
df_update = df_update.withColumn('f_now', df_update.col('d_0') + df_update.col('d_1') + df_update.col('d_2'))