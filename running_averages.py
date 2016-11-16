from pyspark import SparkContext
from pyspark.sql import SQLContext, functions as F
from pyspark.sql.window import Window
import sys

sc = SparkContext()
sqlContext = SQLContext(sc)

eta = 0.5

# sqlContext.sql("CREATE TABLE R_Table (R float)");
# df_r = sqlContext.sql('SELECT R FROM R_Table')
prev_A = 2
prev_B = 5

df_r = sqlContext.createDataFrame([(1, 1), (2, 1), (3, 3), (4, 2), (5, 4)], ['index', 'R'])
prev_A_B = sqlContext.createDataFrame([(0, 0, 0)], ['index', 'A', 'B'])
n = df_r.count()

# CHECK: Assumes dataframes are sorted from least recent to most recent
# selects R, R2, and decay = eta * (1 - eta)^(n - 1 - row_number)
df_decay_r_r2 = df_r.select(F.col('index'), (F.lit(eta) * F.pow(F.lit(eta), F.lit(n - 1) - F.col('index'))).alias('decay'), F.col('R'), (F.col('R') * F.col('R')).alias('R2'))

# calculate all eta * (1 - eta)^(n - 1 - row_number) * R and eta * (1 - eta)^(n - 1 - row_number) * R^2
df_decayed_r_r2 = df_decay_r_r2.select(F.col('index'), (F.col('decay') * F.col('R')).alias('R_decay'), (F.col('decay') * F.col('R2')).alias('R2_decay'))


# generates the cumulative updates to prev_A, prev_B with a scan
w = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
deltaA_deltaB = df_decayed_r_r2.select(F.col('index'), F.sum('R_decay').over(w).alias('A_update'), F.sum('R2_decay').over(w).alias('B_update'))

# selects decay for A and B: decay = (1 - eta)^(n - row_number)
df_decay = df_r.select(F.col('index'), F.pow(F.lit(1 - eta), F.lit(n) - F.col('index')).alias('decay'))
df_decayed_a_b = df_decay.select(F.col('index'), (F.col('decay') * F.lit(prev_A)).alias('A_decay'), (F.col('decay') * F.lit(prev_B)).alias('B_decay'))

# old A, B and newly calculated A, B
df_a_b = df_decayed_a_b.join(deltaA_deltaB, 'index').select(F.col('index'), (F.col('A_update') + F.col('A_decay')).alias('A'), (F.col('B_update') + F.col('B_decay')).alias('B')).union(prev_A_B)

df_a_b = df_a_b.alias('df_a_b')
df_decay_r_r2 = df_decay_r_r2.alias('df_decay_r_r2')

# join tables but offset indices by 1 since that's what the formula wants
dS_deta = df_a_b.join(df_decay_r_r2, F.col('df_a_b.index') + 1 == F.col('df_decay_r_r2.index')).select(((F.col('B') * (F.col('R') - F.col('A')) - F.lit(0.5) * F.col('A') * (F.col('R2') - F.col('B'))) / F.pow(F.col('B') - F.col('A') * F.col('A'), F.lit(1.5))).alias('dS_deta'))
