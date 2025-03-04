import pandas as pd


def force_int64_expected_dtypes(df, expected_dtypes):
    df_clean = df.copy()

    for col, dtypes in expected_dtypes.items():
        if col not in df_clean.columns:
            continue
        if dtypes in ['float64', 'int64']:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')\
            # Then convert to float64. This is to avoid precision loss and preserve na values.
            df_clean[col] = df_clean[col].astype('float64')
    
    # Drop NaN values. This step is to make sure numeric columns can be converted to int64
    df_clean = df_clean.dropna()

    for col, dtypes in expected_dtypes.items():
        if col not in df_clean.columns:
            continue
        if dtypes == 'float64':
            if col != CORRECTNESS: # apart from correctness, all other numeric columns are int64
                # Finally, convert to int64. 
                df_clean[col] = df_clean[col].astype('int64')
    
    print(f'\tPartition filtered to {len(df_clean)} rows')
    return df_clean

def filter_invalid_rows_convert_dtypes(df, expected_dtypes):
    """Removes rows where any column does not match expected dtype."""
    try:
        valid_mask = pd.Series(True, index=df.index)

        for col, dtype in expected_dtypes.items():
            if col not in df.columns:
                continue

            if dtype in ["int64", "float64"]:
                numeric_mask = pd.to_numeric(df[col], errors="coerce")
                valid_mask &= numeric_mask.notna()
                if valid_mask.sum()==0: 
                    print(f'No valid rows found for column {col} with dtype {dtype}')
                    return pd.DataFrame(columns=df.columns)
            
        filtered_df = df[valid_mask].copy()

        # Convert to final dtypes
        for col, dtype in expected_dtypes.items():
            if col not in filtered_df.columns:
                continue
                
            if dtype in ["int64", "float64"]:
                filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")
                if dtype == "int64":
                    filtered_df[col] = filtered_df[col].astype("float64").astype("int64")
            elif dtype == "object":
                filtered_df[col] = filtered_df[col].astype(str)

        print(f'\tPartition filtered to {len(filtered_df)} rows')
        return filtered_df

    except Exception as e:
        print(f"Error in filter_invalid_rows: {str(e)}")
        return pd.DataFrame(columns=df.columns)