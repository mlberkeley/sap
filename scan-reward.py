'''
    Warning: TESTED
    author: Gan Tu (Michael)
    project: sap - mlab fall 2016
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext, functions as F

sc = SparkContext()
sql = SQLContext(sc)

''' Calculate: R_i = F_(i-1) * (close_i - open_i) - delta * |F_i - F_(i-1)| '''
def reward(df):
    # Assume the column for Fi dataframe is: "F1"
    # Assume the column in Fi_1 dataframe is: "F2"
    return df.select(((df['close'] - df['open']) * df['F1'] - df['delta'] * F.abs(df['F1'] - df['F2'])).alias('reward')).show()

def test():
    df = sql.createDataFrame([(10, 2, 3, 4, 6), (20, 15, 3, 24, 15), (22, 32, 1, 7, 3)], ['close','open','delta','F1','F2'])
    reward(df)

