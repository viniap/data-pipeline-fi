"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.7
"""

import pandas as pd
from typing import List


def merge_dataframes(*dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merges the raw dataframes.

    Args:
        dfs: List of all raw dataframes.
    Returns:
        Merged dataframe.
    """

    """df_merged = pd.DataFrame()
    for df in dfs:
        df_merged = df_merged.join(df, how='outer')"""
    df_merged = pd.concat(dfs)

    return df_merged
