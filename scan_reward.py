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
    # Assume the column for Fi dataframe is: "F1"
    # Assume the column in Fi_1 dataframe is: "F2"
    data = data.alias('data')
    dfF = dfF.alias('dfF')
    df = data.join(dfF, F.col('data.index')== F.col('dfF.index')).select(
        ((F.col('data.close') - F.col('data.open')) \
        * F.col('dfF.lagF') \
        - delta * F.abs(F.col('dfF.lagF'))).alias('reward') )

    return df

def test():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext

    sc = SparkContext()
    sql = SQLContext(sc)

    df = sql.createDataFrame([(10, 2, 3, 4, 6), (20, 15, 3, 24, 15), (22, 32, 1, 7, 3)], ['close','open','delta','F1','F2'])
    reward(df)
