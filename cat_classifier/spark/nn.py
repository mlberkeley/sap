import numpy as np
import cPickle as pickle
from classifier import Classifier
from util.layers import *
from util.dump import *

""" STEP2: Build Two-layer Fully-Connected Neural Network """

class NNClassifier(Classifier):
  def __init__(self, D, H, W, K, iternum):
    Classifier.__init__(self, D, H, W, K, iternum)
    self.L = 100 # size of hidden layer

    """ Layer 1 Parameters """
    # weight matrix: [M * L]
    self.A1 = 0.01 * np.random.randn(self.M, self.L)
    # bias: [1 * L]
    self.b1 = np.zeros((1,self.L))

    """ Layer 3 Parameters """
    # weight matrix: [L * K]
    self.A3 = 0.01 * np.random.randn(self.L, K)
    # bias: [1 * K]
    self.b3 = np.zeros((1,K))

    """ Hyperparams """
    # learning rate
    self.rho = 1e-2
    # momentum
    self.mu = 0.9
    # reg strencth
    self.lam = 0.1
    # velocity for A1: [M * L]
    self.v1 = np.zeros((self.M, self.L))
    # velocity for A3: [L * K] 
    self.v3 = np.zeros((self.L, K))
    return

  def load(self, path):
    data = pickle.load(open(path + "layer1"))
    assert(self.A1.shape == data['w'].shape)
    assert(self.b1.shape == data['b'].shape)
    self.A1 = data['w']
    self.b1 = data['b']
    data = pickle.load(open(path + "layer3"))
    assert(self.A3.shape == data['w'].shape)
    assert(self.b3.shape == data['b'].shape)
    self.A3 = data['w']
    self.b3 = data['b']
    return

  def param(self):
    return [("A1", self.A1), ("b1", self.b1), ("A3", self.A3), ("b3", self.b3)]

  def forward(self, data):
    """
    INPUT:
      - data: RDD[(key, (image, class)) pairs]
    OUTPUT:
      - RDD[(key, (image, list of layers, class)) pairs]
    """
    """
    TODO: Implement the forward passes of the following layers
    Layer 1 : linear
    Layer 2 : ReLU
    Layer 3 : linear
    """
    A1 = self.A1
    b1 = self.b1
    A3 = self.A3
    b3 = self.b3

    return data.map(lambda (k, (x,    y)): (k, (x, [linear_forward(x, A1, b1)], y))) \
               .map(lambda (k, (x, l, y)): (k, (x, l + [ReLU_forward(l[0])], y))) \
               .map(lambda (k, (x, l, y)): (k, (x, l + [linear_forward(l[1], A3, b3)], y))) # replace it with your code

  def backward(self, data, count):
    """
    INPUT:
      - data: RDD[(image, list of layers, class) pairs]
    OUTPUT:
      - loss
    """
    """
    Implement softmax loss layer 
    """
    softmax = data.map(lambda (x, l, y): (x, l, softmax_loss(l[2], y))) \
                  .map(lambda (x, l, (L, df)): (x, l, (L/count, df/count)))
    """
    Compute the loss
    """
    L = softmax.map(lambda (x, l, (L, dLdl3)): L) \
               .reduce(lambda v1, v2: v1 + v2); # replace it with your code

    """ regularization """
    L += 0.5 * self.lam * (np.sum(self.A1*self.A1) + np.sum(self.A3*self.A3))

    """ Todo: Implement backpropagation for Layer 3 """
    bp_l3 = softmax.map(lambda (x, l, (L, dLdl3)): (x, l, linear_backward(dLdl3, l[1], self.A3)));

    """ Todo: Compute the gradient on A3 and b3 """
    dLdA3, dLdb3 = bp_l3.map(lambda (x, l, (dLdl2, dLdA3, dLdb3)): (dLdA3, dLdb3)) \
                        .reduce(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))

    """ Todo: Implement backpropagation for Layer 2 """
    bp_l2 = bp_l3.map(lambda (x, l, (dLdl2, dLdA3, dLdb3)): (x, ReLU_backward(dLdl2, l[0])));

    """ Todo: Implement backpropagation for Layer 1 """
    bp_l1 = bp_l2.map(lambda (x, dLdl1): linear_backward(dLdl1, x, self.A1));

    """ Todo: Compute the gradient on A1 and b1 """
    dLdX, dLdA1, dLdb1 = bp_l1.reduce(lambda v1, v2: (0, v1[1] + v2[1], v1[2] + v2[2]));

    """ regularization gradient """
    dLdA3 = dLdA3.reshape(self.A3.shape)
    dLdA1 = dLdA1.reshape(self.A1.shape)
    dLdA3 += self.lam * self.A3
    dLdA1 += self.lam * self.A1

    """ tune the parameter """
    self.v1 = self.mu * self.v1 - self.rho * dLdA1
    self.v3 = self.mu * self.v3 - self.rho * dLdA3
    self.A1 += self.v1
    self.A3 += self.v3
    self.b1 += - self.rho * dLdb1
    self.b3 += - self.rho * dLdb3

    return L
