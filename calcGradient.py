from pyspark.sql import functions as F
import sys
import time
import random

def calcGradient(sqlContext, D_F_Fprev_R_dU_dR, D_0_F, dataColumns):
    # These values are needed nowhere else; we don't need to bother computing them

    # $$\frac{dU_T}{d\theta}=\sum_{t=1}^T \frac{dU_T}{dR_t} ((\frac{dR_t}{df_t})*(\frac{df_t}{d\theta}) + (\frac{dR_t}{df_{t-1}})*(\frac{df_{t-1}}{d\theta})) $$
    # $$dR_t/df_t = sgn(f[t-1]-f[t])$$
    # $$dR_t/df_{t-1} = close[t] - open[t] -dR_t/df_t$$
    # $$df_{t-1}/d\theta = D_{t-1}$$
    # $$df_t/d\theta = D_t$$
    df_i = D_F_Fprev_R_dU_dR.withColumn('dR_dF', F.signum(F.col('Fprev') - F.col('F')))
    df_i = df_i.withColumn('dR_dFprev', F.col('close-open') - F.col('dR_dF'))

    # create a dataframe with the zeroth data point
    fullColumns = df_i.columns
    columnMap = {}
    for i, column in enumerate(dataColumns):
        columnMap[column] = float(D_0_F[i])
    columnMap['Fprev'] = float(D_0_F[-1])
    columnMap['index'] = 0

    df_prev = df_i.alias('df_prev')
    df_0 = sqlContext.createDataFrame([[columnMap.get(column, 0.0) for column in fullColumns]], schema=df_prev.schema)
    df_prev = df_prev.union(df_0)
    df_i = df_i.alias('df_i')

    derivedColumns = dataColumns + ['Fprev']
    dU_dColumns = ['dU_d%s' % (column) for column in derivedColumns]

    df_i_prev = df_i.join(df_prev, F.col('df_i.index') == F.col('df_prev.index') + 1, 'inner')
    dU_dtheta = df_i_prev.select('df_i.index', 'df_i.F', 'df_i.A', 'df_i.B', *((F.col('df_i.dU_dR') * (F.col('df_i.dR_dF') * F.col('df_i.%s' % (column)) + F.col('df_i.dR_dFprev') * F.col('df_prev.%s' % (column)))).alias(dU_dColumn) for column, dU_dColumn in zip(derivedColumns, dU_dColumns)))

    summedColumns = ['sum(%s)' % (column) for column in dU_dColumns]
    return dU_dtheta.groupBy().sum(*dU_dColumns), summedColumns

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
