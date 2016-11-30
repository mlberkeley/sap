
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import time
import random

def calcGradient(df, ds_deta):
    # These values are needed nowhere else; we don't need to bother computing them

    # $$\frac{dU_T}{d\theta}=\sum_{t=1}^T \frac{dU_T}{dR_t} ((\frac{dR_t}{df_t})*(\frac{df_t}{d\theta}) + (\frac{dR_t}{df_{t-1}})*(\frac{df_{t-1}}{d\theta})) $$
    # $$dR_t/df_t = sgn(f[t-1]-f[t])$$
    # $$dR_t/df_{t-1} = close[t] - open[t] -dR_t/df_t$$
    # $$df_{t-1}/d\theta = D_{t-1}$$
    # $$df_t/d\theta = D_t$$
    ds_deta = ds_deta.alias('ds_deta')
    w = Window.orderBy('index').rowsBetween(-1, -1)

    df_2 = df.select('index', F.col('D').alias('D_i'), F.lag('D', default=0).over(w).alias('D_i-1'), F.col('F').alias('F_i'), F.lag('F', default=0).over(w).alias('F_i-1'), 'close', 'open')
    df_2 = df_2.withColumn('dR_dF', F.signum(F.col('F_i-1') - F.col('F_i'))).alias('df_2')
    df_2 = df_2.join(ds_deta, F.col('ds_deta.index') == F.col('df_2.index')).select(
                F.col('ds_deta.index').alias('index'),
                F.col('df_2.D_i').alias('D_i'),
                F.col('df_2.dR_dF').alias('dR_dF'),
                F.col('df_2.D_i-1').alias('D_i-1'),
                F.col('df_2.close').alias('close'),
                F.col('df_2.open').alias('open'),
                F.col('ds_deta.ds_deta').alias('dU_dR'),
                F.col('df_2.F_i').alias('F_i'),
                F.col('df_2.F_i-1').alias('F_i-1'))
    dU_dtheta_i = df_2.select('index', (F.col('dU_dR') * (F.col('dR_dF') * F.col('D_i') + (F.col('close') - F.col('open') - F.col('dR_dF')) * F.col('D_i-1'))).alias('dU_dtheta'))

    dU_dtheta = dU_dtheta_i.groupBy().sum()
    # df_2.show()
    # dU_dtheta_i.show()
    # dU_dtheta.show()
    return dU_dtheta

def test():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    N = 5
    columns = ['index', 'D', 'F', 'close', 'open']
    # dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] | open[i] | dU[i]/dR[i] for i in [1,n]
    df = sqlContext.createDataFrame([[i] + [random.randint(0,4) for _ in range(len(columns) - 1)] for i in range(N)], columns)
    ds_deta = sqlContext.createDataFrame([[i,random.randint(0,1)] for i in range(N)], ['index', 'ds_deta'])
    calcGradient(df, ds_deta).show()
if __name__ == "__main__":
    test()
