### Parse out all problem-student interactions in QC23
### Identify whether each student answer each problem more than once, if so, retain the last interaction.
### Output a mapping: ['problem_log_id', 'problem_id', 'student_user_id', 'correctness']

import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os
from clean_func import *
from constants import *

def main():
    parser = argparse.ArgumentParser(description='Cleaning interactions for QC23 Dataset')
    # 
    parser.add_argument('-i', '--int_count', type=str, default='',
                       help='Path to the csv file of interaction counts per problem')
    # compute mapping, when called, mapping from problem to students
    parser.add_argument('-m', '--mapping', action='store_true',
                      help='Mapping from problem to students')
    
    pbar = ProgressBar()

    dtypes_dict = {
        'problem_id': 'float64',
        'problem_log_id': 'float64',
        'student_user_id': 'float64',
        'correctness': 'float64'
    }

    ddf = dd.read_csv(os.path.join(DATA_PRO, QC23_clean + CSV),
                      encoding='utf-8',
                      engine='python',
                      blocksize="64MB",
                      on_bad_lines="skip",
                      dtype='object'))

    interaction_count = None