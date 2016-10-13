import numpy as np
import cPickle as pickle
from classifier import Classifier
from util.layers import *
from util.dump import *

""" STEP1: Build Linear Classifier """

class LinearClassifier(Classifier):
  def __init__(self, D, H, W, K, iternum):
    Classifier.__init__(self, D, H, W, K, iternum)
    """ Parameters """
    # weight matrix: [M * K]
    self.A = 0.01 * np.random.randn(self.M, K)
    # bias: [1 * K]
    self.b = np.zeros((1,K))

    """ Hyperparams """
    # learning rate
    self.rho = 1e-5
    # momentum
    self.mu = 0.9
    # reg strength
    self.lam = 1e1
    # velocity for A: [M * K]
    self.v = np.zeros((self.M, K))
    return

  def load(self, path):
    data = pickle.load(open(path + "layer1"))
    assert(self.A.shape == data['w'].shape)
    assert(self.b.shape == data['b'].shape)
    self.A = data['w']
    self.b = data['b']
    return

  def param(self):
    return [("A", self.A), ("b", self.b)]
 
  def forward(self, data):
    """
    INPUT:
      - data: RDD[(key, (image, class)) pairs]
    OUTPUT:
      - RDD[(key, (image, list of layers, class)) pairs]
    """
    """ 
    Layer 1: linear 
    Todo: Implement the forward pass of Layer1
    """
    A = self.A
    b = self.b

    return data.map(lambda (k, (x, y)): (k, (x, [linear_forward(x, A, b)], y))) # Replace it with your code

  def backward(self, data, count):
    """
    INPUT:
      - data: RDD[(image, list of layers, class) pairs]
    OUTPUT:
      - loss
    """
    """ 
    softmax loss layer
    (image, score, class) pairs -> (image, (loss, gradient))
    """
    softmax = data.map(lambda (x, l, y): (x, softmax_loss(l[0], y))) \
                  .map(lambda (x, (L, df)): (x, (L/count, df/count)))
    """
    Todo: Compute the loss
    Hint: You need to reduce the RDD from 'softmax loss layer'
    """
    L = softmax.map(lambda (x, (L, dLdl1)): L) \
               .reduce(lambda v1, v2: v1 + v2)
 
    """ regularization: loss = 1/2 * lam * sum_nk(A_nk * A_nk) """
    L += 0.5 * self.lam * np.sum(self.A * self.A) 

    """ 
    Todo: Implement backpropagation for Layer 1 
    """
    bp_l1 = softmax.map(lambda (x, (L, dLdl1)): linear_backward(dLdl1, x, self.A))
    """
    Todo: Calculate the gradients on A & b
    Hint: You need to reduce the RDD from 'backpropagation for Layer 1'
          Also check the output of the backward function
    """
    dLdX, dLdA, dLdb = bp_l1.reduce(lambda v1, v2: (0, v1[1] + v2[1], v1[2] + v2[2]))

    """ regularization gradient """
    dLdA = dLdA.reshape(self.A.shape)
    dLdA += self.lam * self.A

    """ tune the parameter """
    self.v = self.mu * self.v - self.rho * dLdA
    self.A += self.v
    self.b += -self.rho * dLdb
   
    return L
