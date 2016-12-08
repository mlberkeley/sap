from pyspark.sql import functions as F
from scan import scan_sequential
import sys
import random

def running_averages(sqlContext, D_F_Fprev_R, A_0, B_0, eta):
    N = D_F_Fprev_R.count()

    df = D_F_Fprev_R.withColumn('R2', F.col('R') * F.col('R'))

    # i = index. Find eta * (1 - eta)^(N - i) as the decay factor for R and R2. 
    # Find (1 - eta)^i as the decay factor for A_0 and B_0.
    df = df.withColumn('R_R2_forw_weight', eta * F.pow(1 - eta, N - F.col('index')))
    df = df.withColumn('R_R2_rev_weight', F.pow(1 - eta, F.col('index') - N))
    df = df.withColumn('A_B_0_weight', F.pow(1 - eta, F.col('index')))

    # Find (1 - eta)^(-(N - i)) to reverse the decay after the scan. Finds the decayed versions of A_0, B_0, R, R^2
    df = df.withColumn('A_0_weighted', F.col('A_B_0_weight') * A_0)
    df = df.withColumn('B_0_weighted', F.col('A_B_0_weight') * B_0)
    df = df.withColumn('R_weighted', F.col('R_R2_forw_weight') * F.col('R'))
    df = df.withColumn('R2_weighted', F.col('R_R2_forw_weight') * F.col('R2'))

    # Do a scan over decayed R and R^2 and use counter_decay to reverse some of the decay. Find A from A_0 and scan R, find B from B_0 and scan R^2
    df = scan_sequential(sqlContext, df, 'R_weighted', 'R_weighted_scan')
    df = scan_sequential(sqlContext, df, 'R2_weighted', 'R2_weighted_scan')
    df = df.withColumn('A', F.col('A_0_weighted') + F.col('R_R2_rev_weight') * F.col('R_weighted_scan'))
    df = df.withColumn('B', F.col('B_0_weighted') + F.col('R_R2_rev_weight') * F.col('R2_weighted_scan'))

    df_lag = df.select((F.col('index') + 1).alias('index'), F.col('A').alias('Aprev'), F.col('B').alias('Bprev'))
    df_lag = df_lag.unionAll(sqlContext.createDataFrame([[1, A_0, B_0]], schema=df_lag.schema))
    df = df.join(df_lag, 'index', 'inner')

    # Calculate dS_deta from the t-th point and the (t-1)-th point for the two timesteps in the formula
    # df = df.withColumn('dS_deta', ((F.col('B') * (F.col('R') - F.col('A')) - 0.5 * F.col('A') * (F.col('R2') - F.col('B'))) / F.pow(F.col('B') - F.col('A') * F.col('A'), 1.5)))

    # Calculate dU_dR
    df = df.withColumn('dU_dR', (F.col('Bprev') - F.col('Aprev') * F.col('R')) / F.pow(F.col('Bprev') - F.col('Aprev') * F.col('Aprev'), 1.5))

    return df

def test():
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    A_0 = 0
    B_0 = 1
    eta = 0.1
    
    df_r = sqlContext.createDataFrame(list(enumerate([random.randint(0,1) for i in range(N)])), ['index', 'R'])
    running_averages(df_r, A_0, B_0, eta).show()

if __name__ == "__main__":
    test()
