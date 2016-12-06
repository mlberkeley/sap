import numpy as np

def get_F_A_B(D, theta, F_0, D_0_F, A_0, B_0, delta, eta, rho):
    '''
    D     : n x (d - 1) matrix of all the features for n timesteps.
            (d - 1) because dth feature is the F output of the previous timestep
            e.g. a row might be [(close-open), low, high, volume]
    theta : n x d matrix of coefficient, last parameter is the recurrence
    F_0   : initial F (trade value)
    D_0_F : (d x 1) initial dF_prev_dtheta
    A_0   : initial A (running exponential average of reward)
    B_0   : initial B (running exponential average of reward^2)
    delta : coefficient in reward function
    eta   : decay rate for running averages A and B
    rho   : update rate for theta
    >>> d = 5
    >>> D_0_F = np.zeros((d,), dtype=float)
    >>> F_0 = 0
    >>> A_0 = 0
    >>> B_0 = 1
    >>> delta = 0.1
    >>> eta = 0.2
    >>> rho = 0.3
    >>> theta = np.array([1, 0, 1, 0, 1], dtype=float)
    >>> D = np.array([[1, 2, 3, 4], [2, 3, 4, 5]], dtype=float)
    >>> get_F_A_B(D, theta, F_0, D_0_F, A_0, B_0, delta, eta, rho)
    '''
    # calculate F's
    F = []
    F_prev = F_0
    for D_i in D:
        F_i = get_F_i(theta, D_i, F_prev)
        F.append(F_i)
        F_prev = F_i

    # calculate rewards
    F_prev = F_0
    A_prev = A_0
    B_prev = B_0
    dF_prev_dtheta = D_0_F
    dU_dtheta = np.zeros(theta.shape)

    for F_i, D_i in zip(F, D):
        R_i = get_reward(F_i, F_prev, D_i, delta)

        # calculate A and B for differential sharpe ratio
        delta_A_i = R_i - A_prev
        delta_B_i = R_i * R_i - B_prev
        A_i = A_prev + eta * delta_A_i
        B_i = B_prev + eta * delta_B_i
        
        # dS_deta = (B_prev * delta_A_i - 0.5 * A_prev * delta_B_i) / (B_prev - A_prev * A_prev) ** 1.5
        dU_dR_i = (B_prev - A_prev * R_i) / (B_prev - A_prev * A_prev) ** 1.5
        

        # calculate dR_dtheta
        dR_dF_i = get_dR_dF(F_i, F_prev)
        dR_dF_prev = get_dR_dF_prev(D_i, dR_dF_i)
        dF_i_dtheta = get_dF_i_dtheta(D_i, F_prev)
        dR_i_dtheta = dR_dF_i * dF_i_dtheta + dR_dF_prev * dF_prev_dtheta

        dU_dtheta += dU_dR_i * dR_i_dtheta

        # advance timestep
        F_prev = F_i
        A_prev = A_i
        B_prev = B_i
        dF_prev_dtheta = dF_i_dtheta
    theta += rho * dU_dtheta
    return theta, F_prev, dF_prev_dtheta, A_prev, B_prev

def get_F_i(theta, D_i, F_prev):
    '''
    theta : model parameters
    D_i   : ith row in D
    F_prev: F at the (i-1)th time step
    '''
    return np.dot(theta[:-1], D_i) + theta[-1] * F_prev

def get_reward(F_i, F_prev, D_i, delta):
    return F_prev * D_i[0] - delta * abs(F_i - F_prev)

def get_dR_dF(F_i, F_prev):
    return -1 if F_prev < F_i else 1

def get_dR_dF_prev(D_i, dR_dF_i):
    return D_i[0] - dR_dF_i

def get_dF_i_dtheta(D_i, F_prev):
    return np.append(D_i, F_prev)

def get_dU_i_dtheta(dU_dR, dR_dF, dR_dF_prev, dF_dtheta, dF_prev_dtheta):
    return dU_dR * (dR_dF * dF_dtheta + dR_dF_prev * dF_prev_dtheta)