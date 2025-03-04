# coding: utf-8
# 2025/03/02

import pytest

from .conftest import conf, data
from models.irt import IRT

def test_train(data, conf, tmp_path):
    nuser, nitem = conf
    model = IRT(nuser, nitem)

    model.train(data, test_data = data)
    filepath = tmp_path/'mcd.pt'
    model.save(filepath)
    model.load(filepath)

def test_exception(data, conf, tmp_path):
    try:
        nuser, nitem = conf
        model = IRT(nuser, nitem, value_range = 5, a_range = 5)
        model.train(data, test_data = data)
        filepath = tmp_path/'mcd.pt'
        model.save(filepath)
        model.load(filepath)
    # 'Detect if value_range or a_range is too large such that theta/a/b may contain nan..'
    except ValueError: 
        print(ValueError)