
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext, functions as F
from pyspark import SparkContext
from pyspark.sql.window import Window
import sys
from scan_reward import reward
from fprime import fprime as calcFPrime
from fcalc import fcalc
from running_averages import running_averages
from calcGradient import calcGradient

# In[2]:

sc = SparkContext()
sql = SQLContext(sc)


# In[ ]:

n = 10
theta  = [5, 6, 7, 8, 10]


df = sql.createDataFrame([(1, 1, 2, 3, 10), (2, 4, 5, 6,11), (3, 7, 8, 9, 12)], ['index', 'open', 'close', 'D', 'F'])

fprime = calcFPrime(df, theta, sql)
Fdif = fcalc(df, theta, fprime, sql)

# Calculate the reward
rewarddf = reward(df, Fdif)
ds_deta = running_averages(rewarddf, sql)
gradient = calcGradient(df, ds_deta)
gradient.show()

# In[ ]:
