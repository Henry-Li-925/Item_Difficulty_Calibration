import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar
import os
import gc
from clean_func import *
from constants import *


def main():

    # initialization with memory cleanup
    gc.collect()  # existing garbage collection

    pbar = ProgressBar()
    pbar.register()  # Register the progress bar globally


    # Step 1: Read CSV lazily (Dask infers correct dtypes)
    ddf = dd.read_csv(os.path.join(DATA_RAW, QC23_raw + CSV), 
                    dtype='object', # read everything as object first
                            encoding='utf-8', 
                            engine='python',
                            blocksize="64MB", 
                            on_bad_lines="skip")
    
    
    # Verify that the cleaning pipeline separately:
    verify_df = filter_invalid_rows_convert_dtypes(ddf.partitions[0].compute(), EXPECTED_DTYPES)
    verify_df = verify_df.dropna(subset=[SKILL_CODE, SKILL_NAME]).drop_duplicates()
    if verify_df.empty:
        print('No valid rows found. Exiting.')
        return
    print(verify_df)
    print('Columns: ', verify_df.columns)
    print('Dtypes: ', verify_df.dtypes)
    print('Valid rows found. Continuing...')

    # Step 2: Apply filtering lazily using map_partitions and drop duplicates and nas
    ddf_cleaned = ddf.map_partitions(filter_invalid_rows_convert_dtypes, 
                                     expected_dtypes=EXPECTED_DTYPES,
                                     meta=verify_df)
    print(ddf_cleaned.columns)
    ddf_cleaned = ddf_cleaned.dropna(subset=[SKILL_CODE, SKILL_NAME])
    ddf_cleaned = ddf_cleaned.drop_duplicates()


    # Step 3: compute and save
    with pbar:
        print('Computing...')
        df = ddf_cleaned.compute(assume_missing=True)
        print('Saving...')
        output_file = os.path.join(DATA_PRO, 'cleaned_' + QC23_raw + CSV)
        df.to_csv(output_file, index=False)

        print('Saved as ' + output_file)
    
    # Cleanup
    gc.collect()

if __name__ == "__main__":
    main()