'''
    Warning: UNTESTED
    author: Gan Tu (Michael)
    project: sap - mlab fall 2016
'''

from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

def scan(x, table):
    df2 = sqlContext.sql("SELECT" + x + ", SUM(" + x + ") OVER (ORDER BY " + x + ") FROM " + table + " ORDER BY " + x)
    return d2

'''
    Apply the fn on DF1.VALUE1 and DF2.VALUE2
    Return a dataframe, joined with DF1 and DF2, with an added
    column titled NAME containing values just calcualted.
'''
def applyFunction(df1, df2, fn, name, value1, value2):
    joined = df1.join(df2)
    f = udf(fn, DoubleType())
    return joined.withColumn(name, f(joined[value1], joined[value2]))


''' Calculate: R_i = F_(i-1) * (close_i - open_i) - delta * |F_i - F_(i-1)| '''
def reward(Fi_1, Fi, delta, close, opens):
    def difference(x, y): 
        return x - y
    # Assume the column in close dataframe is: "close"
    # Assume the column in opens dataframe is: "open"
    c_join_o = applyFunction(close, opens, difference, "close-open-difference", "close", "open")


    def abs_difference(x, y):
        return abs(difference(x, y))
    # Assume the column in Fi_1 dataframe is: "f"
    # Assume the column in Fi dataframe is: "f"
    f_join_f = applyFunction(Fi_1, Fi_1, abs_difference, "abs-f-difference", "f", "f")

    def multiply(x, y):
        return x * y
    # Assume the column in Fi_1 dataframe is: "f"
    db1 = applyFunction(Fi_1, c_join_o, multiply, "FC-FO", "F", "close-open-difference")
    # Assume the column in Fi dataframe is: "f"
    # Assume the column in delta dataframe is: "delta"
    db2 = applyFunction(delta, f_join_f, multiply, "DF-DF", "delta", "abs-f-difference")

    r = applyFunction(db1, db2, difference, "FC-FO", "DF-DF")
    return r 

