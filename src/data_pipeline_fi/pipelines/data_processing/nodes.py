"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.7
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from typing import List


def _convert_to_date(date: pd.Series) -> pd.Series:
    return pd.to_datetime(date)


def _convert_to_bool(boolean: pd.Series) -> pd.Series:
    # Create a boolean mask for non-NaN values
    mask = boolean.notnull()
    # Apply np.where function to masked column
    boolean.loc[mask] = np.where(boolean.loc[mask] == 'S', True, False)
    return boolean


def _convert_to_int(qt: pd.Series) -> pd.Series:
    # Replace non-finite values with NaN
    qt = qt.replace([np.inf, -np.inf], np.nan)
    # Convert to integer
    qt = np.floor(pd.to_numeric(qt, errors='coerce')).astype('Int64')
    return qt


def merge_dataframes(*dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merges the raw dataframes.

    Args:
        dfs: List of all raw dataframes.
    Returns:
        Merged dataframe.
    """

    # Concatenate all dataframes in one single dataframe
    df_merged = pd.concat(dfs)

    # Replace any blank cells that might exist with NaN
    df_merged = df_merged.replace(r'^\s*$', np.nan, regex=True)

    # Convert all date columns to datetime type
    dates = [x for x in list(df_merged.columns.values) if x[0:3] == "DT_"]
    for date in dates:
        df_merged[date] = _convert_to_date(df_merged[date])

    # Convert all S/N columns to bool type
    booleans = ["EMISSOR_LIGADO", "RISCO_EMISSOR", "TITULO_POSFX", "TITULO_CETIP", "TITULO_GARANTIA", "TITULO_POSFX",
                "INVEST_COLETIVO", "INVEST_COLETIVO_GESTOR"]
    for boolean in booleans:
        df_merged[boolean] = _convert_to_bool(df_merged[boolean])

    # Convert all quantities to integer
    qts = [x for x in list(df_merged.columns.values) if x[0:3] == "QT_"]
    for qt in qts:
        df_merged[qt] = _convert_to_int(df_merged[qt])

    # Convert CD_SELIC (float -> int -> str)
    df_merged["CD_SELIC"] = _convert_to_int(df_merged["CD_SELIC"])
    df_merged["CD_SELIC"] = df_merged["CD_SELIC"].astype(str).replace("<NA>", np.nan)

    # print(df_merged.info())

    return df_merged


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Merges the raw dataframes.

    Args:
        df: Merged dataframe.
    Returns:
        Aggregated dataframe.
    """

    # Remove all columns that have non-values quantity greater than 4 (to include DENOM_SOCIAL, which has 4 non-values)
    df = df[df.columns[df.count() > len(df.index)-5]]
    # Remove the 4 rows that has 4 non-values in DENOM_SOCIAL
    df = df.dropna()
    df = df.reset_index(drop=True)
    print(df.info())

    # Create a one-hot encoder object
    encoder = OneHotEncoder()

    # Fit the encoder to the TP_ATIVO column
    encoder.fit(df[['TP_ATIVO']])

    # Transform the TP_ATIVO column into a one-hot encoded matrix
    onehot = encoder.transform(df[['TP_ATIVO']]).toarray()

    # Create a new dataframe with the one-hot encoded matrix
    onehot_df = pd.DataFrame(onehot, columns=encoder.categories_[0])

    # Concatenate the original dataframe with the one-hot encoded dataframe
    new_df = pd.concat([df, onehot_df], axis=1)

    # Group the dataframe by CNPJ_FUNDO
    new_df = new_df.groupby('CNPJ_FUNDO').max().reset_index()

    # Drop the TP_ATIVO column
    new_df = new_df.drop(columns=["TP_ATIVO"])

    return new_df
