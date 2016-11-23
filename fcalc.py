'''
Tested separately. Still receiving problems with lag()
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions as F
from pyspark.sql.window import Window
import sys

sc = SparkContext()
sqlContext = SQLContext(sc)

# theta4 := scalar value for last parameter
# fprime := scalar containing dot productg
# scan := standard implementation of scan which gives a moving sum of values
n = 10
fprime = -3
theta4 = 10

# range_df := dataframe containing [0, n]
range_df = sqlContext.range(n + 1).select(F.col('id').alias('index'))

#$\theta_4^r
#$\theta_4^f
theta4fr = range_df.select('index', F.pow(theta4, n - F.col('index')).alias('f'), F.pow(theta4, F.col('index') - n).alias('r'))

# moving average F ; assumes scan is implemented
w_scan = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
Fhat = theta4fr.select('index', (fprime * F.sum('f').over(w_scan) * F.col('r')).alias('Fhat'))

# take the difference between the value and previous value
w_lag = Window.orderBy('index').rowsBetween(-1, -1)
Fdif = Fhat.select('index', F.col('Fhat') - F.lag('Fhat', default=0).over(w_lag))
