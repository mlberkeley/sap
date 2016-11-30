'''
Calcultes the F-lag from the data.
Authors: Phillip Kuznetsov, Zhonxia Yan (Z)
'''

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

# theta4 := scalar value for last parameter
# fprime := scalar containing dot productg
# scan := standard implementation of scan which gives a moving sum of values
n = 10

# range_df := dataframe containing [0, n]


def fcalc(df, theta, fprime, sqlContext):
    range_df = sqlContext.range(n + 1).select(F.col('id').alias('index'))
    #$\theta_4^r
    #$\theta_4^f
    theta4fr = range_df.select('index', F.pow(theta[4], n - F.col('index')).alias('f'), F.pow(theta[4], F.col('index') - n).alias('r'))

    # aliasing to join by index
    theta4fr = theta4fr.alias('theta4fr')
    fprime = fprime.alias('fprime')

    Fhat = theta4fr.join(fprime, F.col('fprime.index') == F.col('theta4fr.index')).select('fprime.index', 'r', (F.col('fprime.fprime') * F.col('theta4fr.f')).alias('fprimef'))

    # moving average F ; assumes scan is implemented
    w_scan = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
    Fhat = Fhat.select('index', ( F.sum('fprimef').over(w_scan) * F.col('r')).alias('Fhat'))

    # take the difference between the value and previous value
    w_lag = Window.orderBy('index').rowsBetween(-1, -1)
    Fdif = Fhat.select('index', F.col('Fhat').alias('F'), (F.col('Fhat') - F.lag('Fhat', default=0).over(w_lag)).alias('lagF'))
    return Fdif
def test():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import random

    sc = SparkContext()
    sql = SQLContext(sc)

    N = 5
    theta  = [5, 6, 7, 8, 10]
    columns = ['index', 'D', 'F', 'close', 'open']
    # dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] | open[i] | dU[i]/dR[i] for i in [1,n]
    df = sql.createDataFrame([[i] + [random.randint(0,4) for _ in range(len(columns) - 1)] for i in range(N)], columns)
    fprime = sql.createDataFrame([[i,random.randint(0,1)] for i in range(N)], ['index', 'fprime'])
    fcalc(df, theta, fprime, sql).show()
if __name__ == "__main__":
    test()
