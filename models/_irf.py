# coding: utf-8
# 2025/03/01

import numpy as np

def irf(theta, b, c, a = 1.0, D=1.702, mode = '3pl', F=np, **kwargs):
    assert mode in ['2pl', '3pl'], 'Logistic Dichotomous IRT model should be one of two forms: 2pl, or 3pl'

    z = a * (theta-b)
    def sigmoid(z, F=np):
        return 1 / (1 + F.exp(-z))
    if mode == '3pl':
        irf = c + (1 - c) * sigmoid(D*z, F=F)
    if mode == '2pl':
        irf = sigmoid(D*z, F=F)
    return  irf
