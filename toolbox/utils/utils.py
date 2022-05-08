from typing import List

import duckdb
import numpy as np
import pandas as pd


def calculate_ic(y_true: np.array, y_pred: np.array) -> float:
    """
    computes the information coefficient for the predicted and true variables.
    This function can be given to a sklearn.model_selection Hyper-parameter optimizer.

    Example use in sklearn:
        scoring = make_scorer(crossValIC, greater_is_better=True)

    :param y_true: the true value of the target
    :param y_pred: the predicted value of the target
    :return: the information coefficient of the y_pred
    """
    return np.corrcoef(y_true, y_pred)[0][1]


def factorize(df: pd.DataFrame, partition_by: List[str], exclude=None):
    """
    Factorizes each column of the given dataframe except for the partition_by columns and the exclude columns
    Will preserve indexes and period data types

    Calculates the centered zscore

    In future would like to winsorize at 2.5% and 97.5% percentiles but hard to do in sql

    Won't rename the columns will overwrite them

    :param df: the dataframe we are factorizing
    :param partition_by: What to partition by for calculating median and std will normally be date and sector
    :param exclude: columns to exclude in the factorization process
    """
    if exclude is None:
        exclude = []

    return _duck_db_edits(df, _factorize(df, partition_by, exclude))


def _factorize(df: pd.DataFrame, partition_by: List[str], exclude: List[str]):
    select = partition_by + exclude
    for col in set(df.columns) - set(partition_by) - set(exclude):
        select.append(
            f'({col} - median({col}) OVER factorize_partition) / stddev({col}) OVER factorize_partition AS {col}')
    sql = f"""SELECT {', '.join(select)}
                    FROM df
                    WINDOW factorize_partition AS (PARTITION BY {', '.join(partition_by)})
                    ORDER BY {', '.join(partition_by)}
                    """
    return sql


def rank(df: pd.DataFrame, partition_by: List[str], exclude=None, rank_type: str = 'percent_rank'):
    """
    Ranks each column of the given dataframe except for the partition_by columns and the exclude columns
    Will preserve indexes and period data types
    Won't rename the columns will overwrite them

    :param df: the dataframe we are factorizing
    :param partition_by: What to partition by for calculating rank will normally be date and sector
    :param exclude: columns to exclude in the ranking process
    :param rank_type: the type of rank we are performing
    """
    if exclude is None:
        exclude = []

    return _duck_db_edits(df, _rank(df, partition_by, exclude, rank_type))


def _rank(df: pd.DataFrame, partition_by: List[str], exclude: List[str], rank_type: str):
    select = partition_by + exclude
    for col in set(df.columns) - set(partition_by) - set(exclude):
        select.append(
            f"CASE WHEN {col} is NULL THEN NULL ELSE {rank_type}() OVER (PARTITION BY {', '.join(partition_by)} "
            f"ORDER BY {col}) END AS {col}")
    sql = f"""SELECT {', '.join(select)}
                        FROM df
                        ORDER BY {', '.join(partition_by)}
                        """
    return sql


def ntile(df: pd.DataFrame, partition_by: List[str], exclude=None):
    """
    Ntiles each column of the given dataframe except for the partition_by columns and the exclude columns
    Will preserve indexes and period data types
    Won't rename the columns will overwrite them

    :param df: the dataframe we are factorizing
    :param partition_by: What to partition by for calculating rank will normally be date and sector
    :param exclude: columns to exclude in the ranking process
    :param rank_type: the type of rank we are performing
    """
    if exclude is None:
        exclude = []

    return _duck_db_edits(df, _ntile(df, partition_by, exclude))


def _ntile(df, partition_by, exclude):
    select = partition_by + exclude
    for col in set(df.columns) - set(partition_by) - set(exclude):
        select.append(
            f"SELECT *, NTILE({col}) OVER(PARTITION BY {', '.join(partition_by)} ORDER BY {col} DESC) as {col} ")
    sql = f"""SELECT {', '.join(select)}
                            FROM df
                            ORDER BY {', '.join(partition_by)}
                            """
    return sql


def _duck_db_edits(df, sql):
    index_cols = None
    if not isinstance(df.index, pd.RangeIndex):
        index_cols = df.index.names
        df = df.reset_index()

    convert_to_period = []
    for col in df.columns:
        if isinstance(df[col].dtype, pd.PeriodDtype):
            df[col] = df[col].dt.to_timestamp()
            convert_to_period.append(col)

    df = duckdb.query(sql).df()
    for col in convert_to_period:
        df[col] = df[col].dt.to_period('D')
    df = df.set_index(index_cols) if index_cols else df
    return df


def ntile(factor: pd.Series, ntiles: int, ) -> pd.Series:
    """
    Universe relative Quantiles of a factor by day
    Around 100X faster than pandas groupby qcut

    pd.DataFrame of ntiled factor
        index: (pd.Period, _asset_id)
        Columns: (factor, ntile)
        Values: (factor value, Ntile corresponding to factor value)

    :param factor: same var as ntile_return_tearsheet
    :param ntiles: same var as ntile_return_tearsheet
    """
    factor = factor.to_frame('factor').reset_index()
    index_names = factor.columns.tolist()
    index_names.remove('factor')

    date_is_period = isinstance(factor.date.dtype, pd.core.dtypes.dtypes.PeriodDtype)
    if date_is_period:
        factor['date'] = factor['date'].dt.to_timestamp()

    sql_quantile = f"""SELECT *, NTILE({ntiles}) OVER(PARTITION BY date ORDER BY factor.factor DESC) as ntile
                            FROM factor
                            WHERE factor.factor IS NOT NULL"""
    con = duckdb.connect(':memory:')
    factor_ntile = con.execute(sql_quantile).df()

    if date_is_period:
        factor_ntile['date'] = factor_ntile['date'].dt.to_period(freq='D')

    factor_ntile = factor_ntile.set_index(index_names)
    return factor_ntile
