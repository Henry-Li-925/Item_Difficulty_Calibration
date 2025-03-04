import numpy as np
import pandas as pd
import random
from tqdm import tqdm

import argparse

from sklearn.model_selection import RepeatedKFold

import torch
from torch.utils.data import random_split, DataLoader
from models.irt import IRT

from utils.constants import (
    ENTITY,
    PROJECT,
    DATA_PRO
)

from utils.dataset import LogDataSet

import wandb
import os
import sys
from pathlib import Path
import logging

def transform(dataset, batch_size, train_size, eval_size, **params):
    train_dataset, eval_dataset = random_split(dataset, [train_size, eval_size])
    return DataLoader(train_dataset, batch_size, **params), DataLoader(eval_dataset, batch_size, **params)




def main():
    parser = argparse.ArgumentParser(description='Item Difficulty Calibration Model Trainer')
    parser.add_argument('path', help="Path to csv file")
    parser.add_argument('architecture', type=str, default="3PLIRT",
                        help='Model architecture (default: 3PLIRT)')
    parser.add_argument("batch_size", type=int, default=256,
                         help='Batch size (default: 256)')
    parser.add_argument('lr', type=float, default=0.001, 
                        help='Learning rate (default: 0.001)')
    parser.add_argument('epoch', type=int, default=2,
                         help='Training epoch (default: 2)')
    parser.add_argument('seed', type=int, default=543,
                         help='Random seed (default: 543)')
    parser.add_argument('train_size', type=float, default=0.8,
                         help='Size of training set (default: 0.8)')
    parser.add_argument('value_range', type=int, default=10,
                         help='Value range of theta/b (default: 10)')
    parser.add_argument('a_range', type=int, default=10,
                         help='Value range of a (default: 10)')
    parser.add_argument('-b', '--binary', action='store_true',
                      help='Only train on dichotomously scored items')
    parser.add_argument('-cv', '--cross_valid', action='store_true',
                        help='Train on cross validations (default 5X5)')
    
    args = parser.parse_args()


    # Start a W&B Run
    run = wandb.init(
        entity=ENTITY,
        project=PROJECT,
        config=args,
    )

    # Access values from config dictionary and store them
    # into variables for readability
    path = wandb.config['path']
    architecture = wandb.config['architecture']
    batch_size = wandb.config['batch_size']
    lr = wandb.config['lr']
    epoch = wandb.config['epoch']
    seed = wandb.config['seed']
    value_range = wandb.config['value_range']
    a_range = wandb.config['a_range']
    train_size = wandb.config['train_size']
    binary = wandb.config['binary']
    cross_valid = wandb.config['cross_valid']

    # Set device to GPU if available
    if torch.cuda.is_available():       
        device = torch.device("cuda")
        print("Using GPU.")
    else:
        print("No GPU available, using the CPU instead.")
        device = torch.device("cpu")

    # Read data from the data_path
    data_path = os.path.join(DATA_PRO, path)
    if not Path(data_path).exists():
        print("Target file doesn't exist. Abort")
        raise SystemExit(1)
    df = pd.read_csv(data_path)

    # Subset dichotomously scored data if asked
    if binary:
        df = df[df.correctness.isin([0.0, 1.0])].reset_index(drop=True)
    
    # Set random seed
    generator = torch.Generator().manual_seed(seed)

    # Construct torch.Dataset
    data = LogDataSet(df)

    n_user = len(data.user_id2idx)
    n_item = len(data.item_id2idx)
    
    # train-test split
    train_ds, test_ds = random_split(data, [train_size, 1 - train_size], generator=generator)

    # Select model
    if architecture in ['3PLIRT']:
        Model = IRT


    if not cross_valid:
        train, eval = transform(train_ds, batch_size, train_size, 1 - train_size, shuffle=True)
        test = DataLoader(test_ds, batch_size, shuffle=True)

        logging.getLogger().setLevel(logging.INFO)

        model = Model(n_user, n_item, value_range, a_range)

        model.train(train, eval, epoch=epoch, device=device, lr=lr, wb_run=run)
        
        model.save('models/log/3PLirt.params')

        model.load('models/log/3PLirt.params')

        model.eval(test, wb_run=run)
        
        # Log an Artifact to W&B
        wandb.log_artifact(model)

        # Optional: save model at the end
        model.to_onnx()
        wandb.save("model.onnx")

if __name__ == '__main__':
    main()