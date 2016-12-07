'''
Calcultes the F-lag from the data.
Authors: Phillip Kuznetsov, Zhonxia Yan (Z)
'''

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

def fcalc(D_Fprime, theta, F_0):
    '''
    D_Fprime : dataframe with D and F' at all data points. Starts with index 1
    theta    : numpy array of parameters
    F_0      : value of F at point 0
    '''
    N = D_Fprime.count()
    df = D_Fprime.withColumn('F_forw_weight', F.pow(theta[-1], N - F.col('index')))
    df = df.withColumn('F_rev_weight', F.pow(theta[-1], F.col('index') - N))
    df = df.withColumn('F_0_weight', F.pow(theta[-1], F.col('index')))

    w_scan = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
    D_F = df.withColumn('F', F.col('F_0_weight') * F_0 + F.sum(F.col('Fprime') * F.col('F_forw_weight')).over(w_scan) * F.col('F_rev_weight'))

    # take the difference between the value and previous value
    w_lag = Window.orderBy('index').rowsBetween(-1, -1)
    D_F_Fprev = D_F.withColumn('Fprev', F.lag('F', default=F_0).over(w_lag))
    return D_F_Fprev

def test():
    N = 5
    theta  = [5, 6, 7, 8, 10]
    columns = ['index', 'D', 'F', 'close', 'open']
    # dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] | open[i] | dU[i]/dR[i] for i in [1,n]
    F_0 = 2
    fprime = sqlContext.createDataFrame([[i, random.randint(0, 2)] for i in range(1, N)], ['index', 'fprime'])
    fcalc(fprime, theta, F_0).show()
    
if __name__ == "__main__":
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import random

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    test()
