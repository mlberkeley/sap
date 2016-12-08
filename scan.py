def scan_mappy(lst):
    z = list(map(lambda x: ([x],x),lst))
    # each iteration, we group into tuples of tuples, then combine
    while len(z)>1:
        # group into tuples: we couldn't figure out how to do this with spark
        zg=[]
        for i in range(int(len(z)/2)):
            zg.append((z[2*i],z[2*i+1]))
        if len(zg)<.5*len(z):
            zg.append((z[len(z)-1],([],0)))
        # now map groups together: this is easy to spark-ify.
        z=list(map(lambda x: (x[0][0]+list(map( lambda y:y+x[0][1], x[1][0])), x[0][1]+x[1][1]) ,zg))
    return z[0][0]

def scan_sequential(sqlContext, df, scan_column, scan_result_column):
    rows = df.select('index', scan_column).collect()
    content = [(row['index'], row[scan_column]) for row in rows]
    content.sort()
    indices, values = zip(*content)
    scan = scan_mappy(values)
    indexed_scans = zip(indices, scan)
    df_scan = sqlContext.createDataFrame(indexed_scans, ['index', scan_result_column])
    df = df.join(df_scan, df.index == df_scan.index).drop(df_scan.index)
    return df

from pyspark.sql import functions as F, types as T

def merge_arrays(front_array, back_array):
    front_sum = front_array[-1]
    return front_array + [i + front_sum for i in back_array]

def scan_parallel(sqlContext, df, scan_column, scan_result_column):
    merge_arrays_udf = F.udf(merge_arrays, T.ArrayType(T.DoubleType()))
    df_scan = df.select('index', F.col(scan_column).alias('subsum'), F.array(scan_column).alias('subarray'))
    while df_scan.count() > 1:
        df_scan_odd = df_scan.where(F.col('index') % 2 == 1).alias('odd')
        df_scan_even = df_scan.where(F.col('index') % 2 == 0).alias('even')

        if df_scan_even.count() < df_scan_odd.count(): # odd number, add a blank row to even
            df_last = sqlContext.createDataFrame([[df_scan.count() + 1, 0.0, []]], schema=df_scan.schema)
            df_scan_even = df_scan_even.union(df_last)

        # assume first index is odd and that indices are consecutive
        df_scan = df_scan_odd.join(df_scan_even, F.col('odd.index') + 1 == F.col('even.index'), 'inner')
        df_scan = df_scan.select((F.col('even.index') / 2).astype(T.IntegerType()).alias('index'), \
                     (F.col('even.subsum') + F.col('odd.subsum')).alias('subsum'), \
                     merge_arrays_udf(F.col('odd.subarray'), F.col('even.subarray')).alias('subarray'))
    
    scan_array = df_scan.collect()[0]['subarray']
    scans = sqlContext.createDataFrame(((i + 1, x) for i, x in enumerate(scan_array)), ['index', scan_result_column])

    df = df.join(scans, df.index == scans.index).drop(scans.index)
    return df


from pyspark.sql import SQLContext, functions as F
from pyspark import SparkContext
import numpy as np

if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    n = 7
    scan_column = 'x'
    result_column = 'x_sum'
    df = sqlContext.createDataFrame([[i + 1, float(i + 1)] for i in range(n)], ['index', 'x'])
    scan_sequential(sqlContext, df, scan_column, result_column).show()
    # print(scan_mappy([1, 2, 3, 4, 5, 6, 7]))
    # scan_parallel(sqlContext, df, scan_column, result_column).show()
