
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext, functions as F
from pyspark import SparkContext

import sys
import numpy as np

from scan_reward import reward
from fprime import calcFPrime
from fcalc import fcalc
from running_averages import running_averages
from calcGradient import calcGradient

from naive_model import get_F_A_B


def loop(D, theta, F_0, D_0_F, A_0, B_0, delta, eta, rho):
    '''
    D     : n x (d - 1) matrix of all the features for n timesteps.
            (d - 1) because dth feature is the F output of the previous timestep
            e.g. a row might be [close-open, low, high, volume]
    theta : n x d matrix of coefficient, last parameter is the recurrence
    F_0   : initial F (trade value)
    D_0_F : (d x 1) initial dF_prev_dtheta
    A_0   : initial A (running exponential average of reward)
    B_0   : initial B (running exponential average of reward^2)
    delta : coefficient in reward function
    eta   : decay rate for running averages A and B
    rho   : update rate for theta
    '''
    # D = D.withColumn('close-open', F.col('close') - F.col('open')).drop('close').drop('open')
    dataColumns = [x for x in D.columns if x != 'index']
    D_Fprime = calcFPrime(D, theta, dataColumns)

    D_F_Fprev = fcalc(sqlContext, D_Fprime, theta, dataColumns, F_0)
    D_F_Fprev = D_F_Fprev.select('index', 'F', 'Fprev', *dataColumns) # get rid of useless columns

    D_F_Fprev_R = reward(D_F_Fprev, delta)

    D_F_Fprev_R_dU_dR = running_averages(sqlContext, D_F_Fprev_R, A_0, B_0, eta)
    D_F_Fprev_R_dU_dR = D_F_Fprev_R_dU_dR.select('index', 'F', 'Fprev', 'R', 'A', 'B', 'dU_dR', *dataColumns)

    dU_dtheta, summedColumns = calcGradient(sqlContext, D_F_Fprev_R_dU_dR, D_0_F, dataColumns)
    dU_dtheta = dU_dtheta.collect()

    dU_dtheta = np.array([dU_dtheta[0][column] for column in summedColumns])
    theta += rho * dU_dtheta
    output = D_F_Fprev_R_dU_dR.where(F.col('index') == D.count()).select('F', 'A', 'B', 'Fprev', *dataColumns).collect()
    row = output[0]
    F_0_new = row['F']
    A_0_new = row['A']
    B_0_new = row['B']
    D_0_F_new = np.array([row[column] for column in dataColumns + ['Fprev']], dtype=float)
    return theta, F_0_new, D_0_F_new, A_0_new, B_0_new

if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    d = 5
    D_0_F = np.array([1.0, 2.0, 1.0, 0.0, 1.0], dtype=float)
    F_0 = A_0 = B_0 = 0.0
    # F_0 = 0.0
    # A_0 = 0.0
    # B_0 = 1.0
    delta = 0.1
    eta = 0.2
    rho = 0.3

    theta = np.array([1, 0, 1, 0, 1], dtype=float)
    D = np.array([[2, 3, 4, 5], [3, 4, 5, 6]], dtype=float)

    D2 = [[i + 1] + [float(x) for x in arr] for i, arr in enumerate(D)]

    df = sqlContext.createDataFrame(D2, ['index', 'close-open', 'low', 'high', 'volume'])

    theta_spark = np.copy(theta)
    theta_python = np.copy(theta)

    print(loop(df, theta_spark, F_0, D_0_F, A_0, B_0, delta, eta, rho))
    print(get_F_A_B(D, theta_python, F_0, D_0_F, A_0, B_0, delta, eta, rho))
