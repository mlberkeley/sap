# theta[0], 1, 2
# d_dataframe <- time indexed, column for each d_i from 0 <= i <= 4
# store in f_now, s.t. f_now[i] is d_0*theta[0] + d_1*theta[1] + ... + d_3*theta[3]

from pyspark.sql import functions as F
from itertools import islice

def calcFPrime(D, theta, columns):
    N = len(columns)
    D_Fprime = D.withColumn('Fprime', sum(D[column] * theta_i for column, theta_i in zip(columns, islice(theta, N))))
    return D_Fprime

def test():
    N = 5
    theta  = [6, 7, 8, 10, 2]
    columns = ['index', 'close-open', 'low', 'high', 'volume']
    # dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] - open[i] | dU[i]/dR[i] for i in [1,n]
    df = sqlContext.createDataFrame([[i] + [random.randint(0, 2) for _ in range(len(columns) - 1)] for i in range(N)], columns)
    calcFPrime(df, theta).show()

if __name__ == "__main__":
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import random

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    test()
