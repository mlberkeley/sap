'''
Tested separately. Still receiving problems with lag()
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lag, pow, col
from pyspark.sql.window import Window
from scan_reward import scan

sc = SparkContext()
sqlContext = SQLContext(sc)
################
# PLACEHOLDERS #
################

# theta4 := scalar value for last parameter
# fprime := scalar containing dot productg
# scan := standard implementation of scan which gives a moving sum of values
n = 10
fprime = -3
theta4 = 10

# range_df := dataframe containing [0, n]
range_df = sqlContext.range(n+1)

#$\theta_4^f$:
theta4f = range_df.select(pow(theta4, n - col('id')).alias('value'))

#$\theta_4^r$:
theta4r = range_df.select(pow(theta4, col('id') - n).alias('value'))

# moving average F ; assumes scan is implemented
Fhat_temp = scan(theta4f.select(fprime * col('value')))

# assumes that theta4f and all those have a value column name
Fhat = Fhat_temp.join(theta4r).select((Fhat_temp.value * theta4r.value).alias('value'))

# take the difference between the value and previous value
w = Window.orderBy('index')
F = Fhat.select(Fhat.value - lag("value"))
