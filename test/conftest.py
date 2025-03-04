# coding: utf-8
# 2025/03/02

import pytest

import random
import torch
from torch.utils.data import TensorDataset, DataLoader

# construct fixture dataset
@pytest.fixture
def conf():
    nuser = 100
    nitem = 20
    return nuser, nitem


@pytest.fixture
def data(conf):
    nuser, nitem = conf
    user_id = list(range(nuser))
    item_id = list(range(nitem))
    log = []
    for user in user_id:
        for item in item_id:
            response = random.randint(0,1)
            log.append((user, item, response))
    
    u_id, i_id, score = zip(*log) 
    
    dataset = TensorDataset(
        torch.tensor(u_id, dtype=torch.int64),
        torch.tensor(i_id, dtype=torch.int64),
        torch.tensor(score, dtype=torch.float)
        )
    batch_size = 24

    return DataLoader(dataset, batch_size=batch_size)
