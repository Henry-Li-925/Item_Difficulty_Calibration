#! /bin/bash

# 1. Get the cleaned problems
python utils/clean_corrupted.py

# 2. Get the problems with assistment mapping
python utils/clean_problems.py -m

# [Optional] Get the problems with assistment mapping and subset
# python clean_problems.py -m -s 50

