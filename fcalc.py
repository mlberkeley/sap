'''
Warning untested
'''
from pyspark.sql.functions import lag
import pyspark.SparkContext as sc
################
# PLACEHOLDERS #
################

# theta4 := scalar value for last parameter
# fprime := scalar containing dot productg
# scan := standard implementation of scan which gives a moving sum of values
n = 10
fprime = -3

# range_df := dataframe containing [0, n]
range_df = sc.range(n+1)
#$\theta_4^f$:
theta4f = range_df.map(lambda x: theta4 ** (n - x))
#$\theta_4^r$:
theta4r = range_df.map(lambda x: theta4 ** (x- n))
# moving average F ; assumes scan is implemented
Fhat_temp = scan(theta4f.map(lambda x: fprime * x)
# assumes that theta4f and all those have a value column name
Fhat = Fhat_temp.join(theta4r).select((Fhat_temp.value * theta4r.value).alias('value'))
F = Fhat.select(Fhat.value - lag("value"))
