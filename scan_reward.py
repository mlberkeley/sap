'''
    Warning: TESTED boo -ya
    Authors: Gan Tu (Michael), Phillip Kuznetsov
    project: sap - mlab fall 2016
'''
from pyspark.sql import functions as F


''' Calculate: R_i = F_(i-1) * (close_i - open_i) - delta * |F_i - F_(i-1)| '''
def reward(data, dfF, delta=0.1):
    '''
    Takes in dataframees and calculates rewards.

    :data - Dataframe that contains 'close', 'open' and 'delta'
    :dfF - DataFrame that contains the lag
    '''
    data = data.alias('data')
    dfF = dfF.alias('dfF')
    df = data.join(dfF, F.col('data.index') == F.col('dfF.index')).select(F.col('dfF.index').alias('index'),
        ((F.col('data.close') - F.col('data.open')) \
        * F.col('dfF.F') \
        - delta * F.abs(F.col('dfF.lagF'))).alias('R'))
    return df

def test():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import random
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    columns = ['index', 'D', 'F', 'close', 'open']
    N = 5
    # dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] | open[i] | dU[i]/dR[i] for i in [1,n]
    df = sqlContext.createDataFrame([[i] + [random.randint(0,4) for _ in range(len(columns) - 1)] for i in range(N)], columns)
    dfF = sqlContext.createDataFrame([[i]+ [random.randint(0,4) for _ in range(2)] for i in range(N)], ['index', 'F', 'lagF'])
    reward(df, dfF).show()

if __name__ == "__main__":
    test()
