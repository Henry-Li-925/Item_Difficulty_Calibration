# coding: utf-8
# 2025/03/01

class Model(object):
    def __init__(self, *args, **kwargs)->...:
        pass
    
    def train(self, *args, **kwargs)->...:
        return NotImplementedError

    def eval(self, *args, **kwargs)->...:
        return NotImplementedError

    def load(self, *args, **kwargs)->...:
        return NotImplementedError

    def save(self, *args, **kwargs)->...:
        return NotImplementedError 
