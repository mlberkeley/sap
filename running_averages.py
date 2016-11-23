from pyspark import SparkContext
from pyspark.sql import SQLContext, functions as F
from pyspark.sql.window import Window
import sys
import time
import numpy as np

start = time.clock()

sc = SparkContext()
sqlContext = SQLContext(sc)

N = 500

eta = 0.75

# sqlContext.sql("CREATE TABLE R_Table (R float)");
# df_r = sqlContext.sql('SELECT R FROM R_Table')
prev_a = 2
prev_b = 5

df_r = sqlContext.createDataFrame(list(enumerate([int(i) for i in np.random.choice(2, N)])), ['index', 'R'])
# df_r = sqlContext.createDataFrame([(1, 1), (2, 1), (3, 3), (4, 2), (5, 4)], ['index', 'R'])
df_prev_a_b = sqlContext.createDataFrame([(0, prev_a, prev_b)], ['index', 'A', 'B'])
n = df_r.count()

# i = index. Find eta * (1 - eta)^(n - i) as the decay factor for R and R2. Find (1 - eta)^i as the decay factor for A_0 and B_0.
df_decays = df_r.select('index', (eta * F.pow(1 - eta, n - F.col('index'))).alias('R_R2_decay'), (F.pow(1 - eta, F.col('index'))).alias('A_B_decay'), 'R', (F.col('R') * F.col('R')).alias('R2'))

# Find (1 - eta)^(-(n - i)) to reverse the decay after the scan. Finds the decayed versions of A_0, B_0, R, R^2
df_decayed_r_r2 = df_decays.select('index', F.pow(1 - eta, -(n - F.col('index'))).alias('counter_decay'), (F.col('A_B_decay') * prev_a).alias('A_0'), (F.col('A_B_decay') * prev_b).alias('B_0'), (F.col('R_R2_decay') * F.col('R')).alias('R_decay'), (F.col('R_R2_decay') * F.col('R2')).alias('R2_decay'))

# Do a scan over decayed R and R^2 and use counter_decay to reverse some of the decay. Find A from A_0 and scan R, find B from B_0 and scan R^2
w_scan = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
df_a_b = df_decayed_r_r2.select('index', (F.col('A_0') + F.col('counter_decay') * F.sum('R_decay').over(w_scan)).alias('A'), (F.col('B_0') + F.col('counter_decay') * F.sum('R2_decay').over(w_scan)).alias('B'))

df_a_b = df_a_b.union(df_prev_a_b).alias('df_a_b')
df_decays = df_decays.alias('df_decays')

# w_lag = Window.orderBy('index').rowsBetween(-1, -1)

# Calculate dS_deta. Do a join between the t-th point and the (t-1)-th point for the two timesteps in the formula
dS_deta = df_a_b.join(df_decays, F.col('df_a_b.index') + 1 == F.col('df_decays.index')).select(F.col('df_a_b.index'), ((F.col('B') * (F.col('R') - F.col('A')) - 0.5 * F.col('A') * (F.col('R2') - F.col('B'))) / F.pow(F.col('B') - F.col('A') * F.col('A'), 1.5)).alias('dS_deta'))
dS_deta.show()

print('TIME ELAPSED:', time.clock() - start)