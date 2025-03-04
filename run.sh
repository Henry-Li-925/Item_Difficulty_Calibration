#! /bin/bash

# wandb login
wandb login

# training on irt model; binary mode; no CV
python train.py qc23_log_pro.csv 3PLIRT 1024 0.01 10 42 0.8 3 3 -b 