
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext, functions as F
from pyspark import SparkContext
from pyspark.sql.window import Window
import sys


# In[2]:

sc = SparkContext()
sql = SQLContext(sc)


# In[ ]:

n = 10
theta0, theta1, theta2, theta3, theta4 = 5, 6, 7, 8, 10
fprime=-3

# In[3]:


df = sql.createDataFrame([(1, 1, 2, 3, 10), (2, 4, 5, 6,11), (3, 7, 8, 9, 12)], ['index', '0', '1', '2', '3'])
zerodf = sql.createDataFrame([(0,0,)], ['index','fprime'])

df_update = df.select('index',(df['0'] * theta0).alias('0'), (df['1']*theta1).alias('1'),(df['2']*theta2).alias('2'), (df['3']*theta3).alias('3'))
fprime = df_update.withColumn('fprime', sum(df_update[col] for col in df_update.columns[1:]))

fprime = fprime.select('index','fprime')
fprime = fprime.union(zerodf)
# replace n with length of fprime
# replace fprime with

# In[5]:

# range_df := dataframe containing [0, n]
range_df = sql.range(n + 1).select(F.col('id').alias('index'))

#$\theta_4^r
#$\theta_4^f
theta4fr = range_df.select('index', F.pow(theta4, n - F.col('index')).alias('f'), F.pow(theta4, F.col('index') - n).alias('r'))

# aliasing to join by index
theta4fr = theta4fr.alias('theta4fr')
fprime = fprime.alias('fprime')

Fhat = theta4fr.join(fprime, F.col('fprime.index') == F.col('theta4fr.index')).select('fprime.index', 'r', (F.col('fprime.fprime') * F.col('theta4fr.f')).alias('fprimef'))

# moving average F ; assumes scan is implemented
w_scan = Window.orderBy('index').rangeBetween(-sys.maxsize, 0)
Fhat = Fhat.select('index', ( F.sum('fprimef').over(w_scan) * F.col('r')).alias('Fhat'))

# take the difference between the value and previous value
w_lag = Window.orderBy('index').rowsBetween(-1, -1)
Fdif = Fhat.select('index', F.col('Fhat') - F.lag('Fhat', default=0).over(w_lag))
Fdif.show()

# In[ ]:
