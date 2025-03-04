import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar
import argparse
import os
from clean_func import *
from constants import *

def main():
    parser = argparse.ArgumentParser(description='Cleaning Pipeline for QC23 Dataset')
    # prompt program to clean the dataset, with two options: clean problems '-p' or clean interactions '-i', or they can be combined
    parser.add_argument('-p', '--problems', action='store_true',
                      help='Clean problems')
    parser.add_argument('-i', '--interactions', action='store_true',
                      help='Clean interactions')
    parser.add_argument('-pi', '--problems_interactions', action='store_true',
                      help='Clean problems and interactions')
    # providing the csv file of response counts per problem/student
    parser.add_argument('-prc', '--prob_res_count', type=str, default='',
                        help='Path to the csv file of response counts per problem')
    # parser.add_argument('-src', '--stud_res_count', type=str, default='',
    #                     help='Path to the csv file of response counts per student')

    # compute mapping
    parser.add_argument('-m', '--mapping', action='store_true',
                      help='Create mapping for problems/interactions')
    args = parser.parse_args()

    pbar = ProgressBar()

    ddf = dd.read_csv(os.path.join(DATA_PRO, QC23_clean + CSV), 
                      encoding='utf-8',
                      engine='python',
                      blocksize="64MB",
                      on_bad_lines="skip",
                      dtype='object')
    

    ddf = ddf[[PROBLEM_LOG_ID,
                STUDENT_USER_ID,
                ASSIGNMENT_ID,
                ASSIGNMENT_COMPLETION,
                ASSISTMENT_ID,
                PROBLEM_ID,
                SKILL_CODE,
                SKILL_NAME,
                SKILL_BUILDER,   
                SCAFFOLD,
                PROBLEM_START_TIME,
                PROBLEM_END_TIME,
                PROBLEM_TYPE_ID,
                PROBLEM_TYPE,
                PROBLEM_ORDER,
                PROBLEM_BODY,
                ANSWER_TEXT,
                CORRECTNESS]]

    # load counts
    prob_res_counts = None
    # stud_res_counts = None
    if args.prob_res_count:
        prob_res_path = os.path.join(DATA_PRO, args.prob_res_count)
        if not os.path.exists(prob_res_path):
            print(f'Response counts file does not exist at {prob_res_path}')
            return
        prob_res_counts = pd.read_csv(prob_res_path)
        print(f'Response counts loaded from {prob_res_path}')
        print(prob_res_counts.head())
    
    # if args.stud_res_count:
    #     stud_res_path = os.path.join(DATA_PRO, args.stud_res_count)
    #     if not os.path.exists(stud_res_path):
    #         print(f'Interaction counts file does not exist at {stud_res_path}')
    #         return
    #     stud_res_counts = pd.read_csv(stud_res_path)
    #     print(f'Interaction counts loaded from {stud_res_path}')
    #     print(stud_res_counts.head())
    
    # verify operation separately, using a partition
    verify = ddf.partitions[0].compute(assume_missing=True)
    verify = force_int64(filter_invalid_rows_convert_dtypes(verify, EXPECTED_DTYPES)).drop_duplicates().reset_index(drop=True)
    if verify.empty:
        print('Operation is empty. Exiting...')
        return
    
    print("Verification successful. Column dtypes:")
    print(verify.dtypes)
    print("\nSample data:")
    print(verify.head())

    # compute cleaned dataframe
    ddf = ddf.map_partitions(filter_invalid_rows_convert_dtypes,
                            expected_dtypes=EXPECTED_DTYPES,
                            meta=verify)
    ddf = ddf.map_partitions(force_int64,
                            meta=verify)
    with pbar:
        print('Computing dataframe...')
        df = ddf.compute(assume_missing=True)
        print('Dataframe computed')

    ### Checkpoint 1: deduplicate interactions
    print('Checkpoint 1: Deduplicating interactions...')
    # sort by start time
    df = df.sort_values([PROBLEM_START_TIME]).reset_index(drop=True)
    # keep the last occurrence for each student-problem pair
    df = df[~df.duplicated(subset=[STUDENT_USER_ID, PROBLEM_ID], keep='last')]
    # check if the deduplication is successful
    interaction_counts = df.groupby([STUDENT_USER_ID, PROBLEM_ID]).size()
    if interaction_counts[interaction_counts > 1].empty:
        print('Checkpoint 1: Deduplication successful')
    else:
        print('Checkpoint 1: Deduplication failed. Exiting...')
        return

    ### Checkpoint 2: Counting response counts
    print('Checkpoint 2: Counting Response Counts...')
    if prob_res_counts is None:
        # compute response counts per problem
        # each student only answer the problem once, since we deduplicated the interactions
        prob_res_counts = df.groupby(PROBLEM_ID)[STUDENT_USER_ID].count().reset_index().rename(columns={STUDENT_USER_ID: 'response_count'})
        print(f'Response counts computed for {len(prob_res_counts)} problems')
        print(prob_res_counts.head())

        # store response counts per problem
        prob_res_counts.to_csv(os.path.join(DATA_PRO, PROB_RESP_COUNT + CSV), index=False)
        print('Saved as ' + os.path.join(DATA_PRO, PROB_RESP_COUNT + CSV))
    
    # if stud_res_counts is None:
    #     # compute response counts per student
    #     stud_res_counts = df.groupby(STUDENT_USER_ID)[PROBLEM_ID].count().reset_index().rename(columns={PROBLEM_ID: 'response_count'})
    #     print(f'Response counts computed for {len(stud_res_counts)} students')
    #     print(stud_res_counts.head())
        
    #     # store response counts per student
    #     stud_res_counts.to_csv(os.path.join(DATA_PRO, STUD_RESP_COUNT + CSV), index=False)
    #     print('Saved as ' + os.path.join(DATA_PRO, STUD_RESP_COUNT + CSV))

    print('Checkpoint 2: Counting responses completed')
    

    # Checkpoint 3: Getting & Saving problems/interactions
    print('Checkpoint 3: Getting & Saving problems/interactions...')

    prob_cols = [PROBLEM_ID, 
                 ASSISTMENT_ID, 
                 PROBLEM_ORDER,
                 SKILL_CODE, 
                 SKILL_NAME, 
                 SKILL_BUILDER, 
                 SCAFFOLD, 
                 PROBLEM_TYPE, 
                 PROBLEM_TYPE_ID, 
                 PROBLEM_BODY
                 ]
    inter_cols = [PROBLEM_LOG_ID,
                  STUDENT_USER_ID,
                  ASSIGNMENT_ID,
                  ASSIGNMENT_COMPLETION,
                  PROBLEM_ID,
                  PROBLEM_START_TIME,
                  PROBLEM_END_TIME,
                  ANSWER_TEXT,
                  CORRECTNESS
                  ]
    
    problems = df[prob_cols].drop_duplicates().reset_index(drop=True)
    interactions = df[inter_cols].drop_duplicates().reset_index(drop=True)
        
    if args.problems or args.problems_interactions:   
        print(problems.head())
        # Statistics
        print(f'Total number of problems: {len(problems)}')
        print(f'Total number of assistments: {problems[ASSISTMENT_ID].nunique()}')

        print('Saving problems...')
        output_path = os.path.join(DATA_PRO, QC23_prob + CSV)
        problems.to_csv(output_path, index=False)
        print('Problems saved to: ' + output_path)

    if args.interactions or args.problems_interactions:
        print(interactions.head())
        # Statistics
        print(f'Total number of interactions: {len(interactions)}')
        print(f'Total number of students: {interactions[STUDENT_USER_ID].nunique()}')

        print('Saving interactions...')
        output_path = os.path.join(DATA_PRO, QC23_log + CSV)
        interactions.to_csv(output_path, index=False)
        print('Interactions saved to: ' + output_path)

    print('Checkpoint 3: Getting & Saving problems/interactions completed')

    # Checkpoint 4: compute mapping
    print('Checkpoint 4: Computing problem to assistment mapping...')
    if args.mapping:
        print('Computing problem to assistment mapping...')
        mapping = problems[[ASSISTMENT_ID, PROBLEM_ID, PROBLEM_ORDER]].drop_duplicates().sort_values([ASSISTMENT_ID, PROBLEM_ORDER]).reset_index(drop=True)
        # statistics
        print("\nTotal unique mappings:", len(mapping))
        print("Total unique problems:", mapping[PROBLEM_ID].nunique())
        print("Total unique assistments:", mapping[ASSISTMENT_ID].nunique())
        print(mapping.head())
        # save mapping
        print('Saving problem to assistment mapping...')
        mapping.to_csv(os.path.join(DATA_PRO, PROB_ASSIST_MAP + CSV), index=False)
        print('Problem to assistment mapping saved!')
    print('Checkpoint 4: Computing problem to assistment mapping completed')

if __name__ == "__main__":
    main()