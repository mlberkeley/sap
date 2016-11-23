from pyspark import SparkContext
from pyspark.sql import SQLContext, functions as F
from pyspark.sql.window import Window
import sys
import time
import numpy as np

sc = SparkContext()
sqlContext = SQLContext(sc)

N = 5

# dataframe2 := dataframe contains: | i | D[i] | f[i] | close[i] | open[i] | dU[i]/dR[i] for i in [1,n]

df = sqlContext.createDataFrame([[i] + list([int(x) for x in arr]) for i, arr in enumerate(np.random.choice(5, (N, 5)))], ['index', 'D', 'F', 'close', 'open', 'dU_dR'])

# These values are needed nowhere else; we don't need to bother computing them

# $$\frac{dU_T}{d\theta}=\sum_{t=1}^T \frac{dU_T}{dR_t} ((\frac{dR_t}{df_t})*(\frac{df_t}{d\theta}) + (\frac{dR_t}{df_{t-1}})*(\frac{df_{t-1}}{d\theta})) $$
# $$dR_t/df_t = sgn(f[t-1]-f[t])$$
# $$dR_t/df_{t-1} = close[t] - open[t] -dR_t/df_t$$
# $$df_{t-1}/d\theta = D_{t-1}$$
# $$df_t/d\theta = D_t$$

w = Window.orderBy('index').rowsBetween(-1, -1)

df_2 = df.select('index', F.col('D').alias('D_i'), F.lag('D', default=0).over(w).alias('D_i-1'), F.col('F').alias('F_i'), F.lag('F', default=0).over(w).alias('F_i-1'), 'close', 'open', 'dU_dR')
dU_dtheta = df_2.select('index', (F.col('dU_dR') * (F.signum(F.col('F_i-1') - F.col('F_i')) * F.col('D_i') + (F.col('close') - F.col('open') - F.signum(F.col('F_i-1') - F.col('F_i'))) * F.col('D_i-1'))).alias('dU_dtheta'))
df_2.show()
dU_dtheta.show()