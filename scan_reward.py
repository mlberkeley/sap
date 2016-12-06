'''
    Warning: TESTED boo -ya
    Authors: Gan Tu (Michael), Phillip Kuznetsov
    project: sap - mlab fall 2016
'''
from pyspark.sql import functions as F


''' Calculate: R_i = F_(i-1) * (close_i - open_i) - delta * |F_i - F_(i-1)| '''
def reward(D_F_Fprev, delta):
    '''
    D_F_Fprev : dataframe containing all the data, F, and F_(i-1)
    '''
    D_F_Fprev_R = D_F_Fprev.withColumn('R', F.col('Fprev') * F.col('close-open') - delta * F.abs(F.col('F') - F.col('Fprev')))
    return D_F_Fprev_R

def test():
    columns = ['index', 'F', 'Fprev', 'close-open']
    N = 5
    # dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] | open[i] | dU[i]/dR[i] for i in [1,n]
    df = sqlContext.createDataFrame([[i] + [random.randint(0, 4) for _ in range(3)] for i in range(N)], columns)
    reward(df, 0.1).show()

if __name__ == "__main__":
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import random
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    test()
