import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar
import argparse
import os
from clean_func import *
from constants import *

def main():
    parser = argparse.ArgumentParser(description='Cleaning Problems for QC23 Dataset')
    # providing the csv file of response counts per problem
    parser.add_argument('-r', '--res_count', type=str, default='',
                        help='Path to the csv file of response counts per problem')

    # compute mapping, when called, mapping from problem to assistment
    parser.add_argument('-m', '--mapping', action='store_true',
                      help='Mapping from problem to assistment')
    args = parser.parse_args()

    pbar = ProgressBar()

    dtypes_dict = {
        'problem_id': 'float64',
        'problem_log_id': 'float64',
        'student_user_id': 'float64',
        'assignment_id': 'float64',
        'assistment_id': 'float64',
        'problem_type_id': 'float64',
        'problem_order': 'float64',
        'correctness': 'float64'
    }

    ddf = dd.read_csv(os.path.join(DATA_PRO, QC23_clean + CSV), 
                      encoding='utf-8',
                      engine='python',
                      blocksize="64MB",
                      on_bad_lines="skip",
                      dtype='object')
    
    # load response counts
    response_counts = None
    if args.res_count:
        response_path = os.path.join(DATA_PRO, args.res_count)
        if not os.path.exists(response_path):
            print(f'Response counts file does not exist at {response_path}')
            return
        response_counts = pd.read_csv(response_path)
        print(f'Response counts loaded from {response_path}')
        print(response_counts.head())
    
    # verify operation seperately, using a partition
    verify = ddf.partitions[0].compute(assume_missing=True)
    verify = force_int64_expected_dtypes(filter_invalid_rows_convert_dtypes(verify), dtypes_dict).drop_duplicates().reset_index(drop=True)
    if verify.empty:
        print('Operation is empty. Exiting...')
        return
    
    print("Verification successful. Column dtypes:")
    print(verify.dtypes)
    print("\nSample data:")
    print(verify.head())

    # compute cleaned dataframe
    ddf = ddf.map_partitions(filter_invalid_rows_convert_dtypes,
                            meta=verify)
    ddf = ddf.map_partitions(force_int64_expected_dtypes,
                            expected_dtypes=dtypes_dict,
                            meta=verify)
    with pbar:
        print('Computing dataframe...')
        df = ddf.compute(assume_missing=True)
        print('Dataframe computed')


    if response_counts is None:
        # compute response counts
        response_counts = df.groupby(PROBLEM_ID)[PROBLEM_LOG_ID].count().reset_index().rename(columns={PROBLEM_LOG_ID: 'response_count'})
        print(f'Response counts computed for {len(response_counts)} problems')
        print(response_counts.head())

        # store response counts
        response_counts.to_csv(os.path.join(DATA_PRO, 'problem_response_counts.csv'), index=False)
        print('Saved as ' + os.path.join(DATA_PRO, 'problem_response_counts.csv'))

    # step 2: clean problems
    problems = df[[PROBLEM_ID,
                   ASSISTMENT_ID,
                   PROBLEM_TYPE,
                   PROBLEM_TYPE_ID,
                   PROBLEM_BODY
                   ]]
    problems = problems.drop_duplicates([PROBLEM_ID, ASSISTMENT_ID]).reset_index(drop=True)
    # merge with response counts
    problems = problems.merge(response_counts, 
                              on=PROBLEM_ID,
                              how='inner').dropna(subset=['response_count'])
    
    print(problems.head())
    # Statistics
    print(f'Total number of problems: {len(problems)}')
    print(f'Total number of assistments: {problems[ASSISTMENT_ID].nunique()}')


    # step 3: save problems
    print('Saving problems...')
    output_file = os.path.join(DATA_PRO, QC23_prob + CSV)
    problems.to_csv(output_file, index=False)
    print('Problems saved to: ' + output_file)
    
    # step 4: compute mapping
    if args.mapping:
        print('Computing problem to assistment mapping...')
        mapping = problems[[PROBLEM_ID, ASSISTMENT_ID]].drop_duplicates().sort_values(by=PROBLEM_ID).reset_index(drop=True)
        # statistics
        print("\nTotal unique mappings:", len(mapping))
        print("Total unique problems:", mapping[PROBLEM_ID].nunique())
        print("Total unique assistments:", mapping[ASSISTMENT_ID].nunique())
        print(mapping.head())
        # save mapping
        print('Saving problem to assistment mapping...')
        mapping.to_csv(os.path.join(DATA_PRO, 'problem_assistment_mapping.csv'), index=False)
        print('Problem to assistment mapping saved!')

if __name__ == "__main__":
    main()