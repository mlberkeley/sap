# theta[0], 1, 2
# d_dataframe <- time indexed, column for each d_i from 0 <= i <= 4
# store in f_now, s.t. f_now[i] is d_0*theta[0] + d_1*theta[1] + ... + d_3*theta[3]

from pyspark.sql import functions as F


def fprime(df, theta, sqlContext):
    zerodf = sqlContext.createDataFrame([(0,0,)], ['index','fprime'])
    df_update = df.select('index',
            (df['open'] * theta[0]).alias('open'),
            (df['close']*theta[1]).alias('close'),
            (df['F']*theta[2]).alias('F'),
            (df['D']*theta[3]).alias('D'))
    fdf = df_update.withColumn('fprime', sum(df_update[col] for col in df_update.columns if col != 'index'))

    fdf = fdf.select('index','fprime')
    fdf = fdf.union(zerodf)
    return fdf

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
    fprime(df, theta, sql).show()
if __name__ == "__main__":
    test()
