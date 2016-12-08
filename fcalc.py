'''
Calcultes the F-lag from the data.
Authors: Phillip Kuznetsov, Zhonxia Yan (Z)
'''

from pyspark.sql import functions as F
from scan import scan_sequential
import sys

def fcalc(sqlContext, D_Fprime, theta, dataColumns, F_0):
    '''
    D_Fprime : dataframe with D and F' at all data points. Starts with index 1
    theta    : numpy array of parameters
    F_0      : value of F at point 0
    '''
    N = D_Fprime.count()
    df = D_Fprime.withColumn('F_forw_weight', F.pow(theta[-1], N - F.col('index')))
    df = df.withColumn('F_rev_weight', F.pow(theta[-1], F.col('index') - N))
    df = df.withColumn('F_0_weight', F.pow(theta[-1], F.col('index')))

    df = df.withColumn('Fprime_weighted', F.col('Fprime') * F.col('F_forw_weight'))

    df = scan_sequential(sqlContext, df, 'Fprime_weighted', 'Fprime_weighted_scan')
    D_F = df.withColumn('F', F.col('F_0_weight') * F_0 + F.col('Fprime_weighted_scan') * F.col('F_rev_weight'))

    Fprev = D_F.select((F.col('index') + 1).alias('index'), F.col('F').alias('Fprev'))
    Fprev = Fprev.unionAll(sqlContext.createDataFrame([(0 + 1, F_0)], schema=Fprev.schema))

    D_F_Fprev = D_F.join(Fprev, 'index', 'inner')
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
