from pyspark import SparkContext
from pyspark.sql import SQLContext, functions as F
from pyspark.sql.window import Window
import sys

sc = SparkContext()
sqlContext = SQLContext(sc)

eta = 0.75

# sqlContext.sql("CREATE TABLE R_Table (R float)");
# df_r = sqlContext.sql('SELECT R FROM R_Table')
prev_a = 2
prev_b = 5

df_r = sqlContext.createDataFrame([(1, 1), (2, 1), (3, 3), (4, 2), (5, 4)], ['index', 'R'])
df_prev_a_b = sqlContext.createDataFrame([(0, prev_a, prev_b)], ['index', 'A', 'B'])
n = df_r.count()

df_decays = df_r.select(F.col('index'), (eta * F.pow(1 - eta, n - F.col('index'))).alias('R_R2_decay'), (F.pow(1 - eta, F.col('index'))).alias('A_B_decay'), F.col('R'), (F.col('R') * F.col('R')).alias('R2'))

df_decayed_r_r2 = df_decays.select(F.col('index'), F.pow(1 - eta, -(n - F.col('index'))).alias('counter_decay'), (F.col('A_B_decay') * prev_a).alias('A_0'), (F.col('A_B_decay') * prev_b).alias('B_0'), (F.col('R_R2_decay') * F.col('R')).alias('R_decay'), (F.col('R_R2_decay') * F.col('R2')).alias('R2_decay'))

w = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
df_a_b = df_decayed_r_r2.select(F.col('index'), (F.col('A_0') + F.col('counter_decay') * F.sum('R_decay').over(w)).alias('A'), (F.col('B_0') + F.col('counter_decay') * F.sum('R2_decay').over(w)).alias('B'))

df_a_b = df_a_b.union(df_prev_a_b).alias('df_a_b')
df_decays = df_decays.alias('df_decays')

# join tables but offset indices by 1 since that's what the formula wants
dS_deta = df_a_b.join(df_decays, F.col('df_a_b.index') + 1 == F.col('df_decays.index')).select(F.col('df_a_b.index'), ((F.col('B') * (F.col('R') - F.col('A')) - 0.5 * F.col('A') * (F.col('R2') - F.col('B'))) / F.pow(F.col('B') - F.col('A') * F.col('A'), 1.5)).alias('dS_deta'))
