from typing import List, Optional, Tuple, Union

import pandas as pd
import pandas_market_calendars as mcal

from toolbox.db.api.sql_connection import SQLConnection
from toolbox.db.settings import DB_ADJUSTOR_FIELDS


def read_db_timeseries(table: str, assets: Union[List[Union[int, str]], str], search_by: str, fields: List[str],
                       start_date: str, end_date: str = '3000', adjust: bool = True,
                       freq: Optional[str] = 'D') -> pd.DataFrame:
    """
    Reads and returns raw data from the database
    :param assets: the assets we want to get data for, or a universe table
    :param search_by: the identifier we are searching by
    :param fields: the fields we are getting in our query
    :param table: the table we are searching must be prefixed by the schema
    :param start_date: the first date to get data on in '%Y-%m-%d' format
    :param end_date: the last date to get data on in '%Y-%m-%d' format
    :param adjust: should we adjust the pricing?
    :param freq: the frequency of the data for the period, if None is passed then will return a pandas timestamp
    :return: Pandas Dataframe Columns: fields; Index: ('date', search_by)
    """

    if adjust and table not in DB_ADJUSTOR_FIELDS:
        raise ValueError(f'Table {table} is not in DB_ADJUSTOR_FIELDS. '
                         f'Valid tables are {list(DB_ADJUSTOR_FIELDS.keys())}')

    select_col_sql = _create_columns_to_select_sql(table, search_by, fields, adjust)
    asset_filter_sql, assets_df = _create_asset_filter_sql(assets, search_by, start_date, end_date)

    sql_query = f"""
                SELECT {select_col_sql}
                FROM {table} AS data JOIN {asset_filter_sql} AS uni
                    ON data.{search_by} = uni.{search_by}
                WHERE data.date >= '{start_date}' AND data.date <= '{end_date}';
        """

    # hitting db
    con = SQLConnection().con
    out = con.execute(sql_query).fetchdf()
    con.close()

    # should we turn the timestamp to a period?
    return _figure_out_date(out, freq).set_index(['date', search_by])


def read_db_static(table: str, assets: Union[List[Union[int, str]], str], search_by: str, fields: List[str],
                   start_date: str = '1900', end_date: str = '3000') -> pd.DataFrame:
    """
    Reads static data fom the database.
    gets all variations of the static data
    if an asset has 3 changes of the static data then there will be 3 rows of static data for the asset
    :param table: the table we are searching must be prefixed by the schema
    :param assets: the assets we want to get data for, or a universe table
    :param search_by: the identifier we are searching by
    :param fields: the fields we are getting in our query
    :param start_date: the first date to get data on in '%Y-%m-%d' format, defaults to 1900
    :param end_date: the last date to get data on in '%Y-%m-%d' format, defaults to 3000
    :return: Pandas Dataframe Columns: fields; Index: 'date', search_by
    """
    select_col_sql = _create_columns_to_select_sql(table, search_by, fields, False, False)
    asset_filter_sql, assets_df = _create_asset_filter_sql(assets, search_by, start_date, end_date)

    # making the full query
    sql_query = f"""
                SELECT {select_col_sql}, min(data.date) AS min_date, max(data.date) AS max_data
                FROM {table} AS data JOIN {asset_filter_sql} AS uni
                    ON data.{search_by} = uni.{search_by}
                WHERE data.date >= '{start_date}' AND data.date <= '{end_date}' 
                GROUP BY {select_col_sql};
                """

    # hitting db
    con = SQLConnection().con
    out = con.execute(sql_query).fetchdf()
    con.close()

    return out.set_index(search_by)


def read_sparse_data(table: str, assets: Union[List[any], pd.Series, str], search_by: str, fields: List[str],
                     start_date: str = '1990', end_date: str = '3000', adjust: bool = True,
                     ffill_limit=90, trading_calendar='NYSE', **kwargs) -> pd.DataFrame:
    """
    reads in sparse data like fundamental data and resamples it to daily data
    :param table: the table we are searching must be prefixed by the schema
    :param assets: the assets we want to get data for, or a universe table
    :param search_by: the identifier we are searching by
    :param fields: the fields we are getting in our query
    :param start_date: the first date to get data on in '%Y-%m-%d' format, defaults to 1900
    :param end_date: the last date to get data on in '%Y-%m-%d' format, defaults to 3000
    :return: Pandas Dataframe Columns: fields; Index: 'date', search_by
    :param adjust: should we adjust the pricing?
    :param ffill_limit: the limit for the ffill call
    :param trading_calendar: the trading calendar we should reindex the ffilled data by
    :param kwargs: keyword args to get passed to resample
    :return: Pandas Dataframe Columns: fields; Index: 'date', search_by
    """

    tbl = read_db_timeseries(table=table, assets=assets, search_by=search_by, fields=fields, start_date=start_date,
                             end_date=end_date, adjust=adjust, freq=None)

    cal = mcal.get_calendar(trading_calendar).valid_days(start_date=start_date, end_date=end_date).to_series().tolist()
    return tbl.unstack().resample('D', **kwargs).ffill(ffill_limit).reindex(cal).stack()


def read_universe_table(table: str, fields: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    grabs timeseries from a universe table
    :param table: the universe table to query
    :param fields: to full from the table
    :param start_date: first date to query on
    :param end_date: last date to query on
    :return: Pandas Dataframe Columns: fields; Index: 'date', fields
    """
    select_col_sql = _create_columns_to_select_sql(table=table, search_by='date', fields=fields, adjust=False,
                                                   use_date=False).replace('data.', '')
    sql_query = f"""
                    SELECT {select_col_sql}
                    FROM  universe.{table}
                    WHERE date >= '{start_date}' AND date <= '{end_date}' 
                    """

    # hitting db
    con = SQLConnection()
    out = con.execute(sql_query).fetchdf()
    con.close()

    return out


def _create_columns_to_select_sql(table: str, search_by: str, fields: List[str], adjust: bool,
                                  use_date: bool = True) -> str:
    """
    Creates sql code for the columns we want to get data for and adjusts the data when necessary
    :param table: the table we are searching must be prefixed by the schema
    :param search_by: the identifier we are searching by
    :param fields: the fields we are getting in our query
    :param adjust: should we adjust the pricing?
    :return:
    """
    table_adj = DB_ADJUSTOR_FIELDS.get(table)

    if use_date:
        columns_to_select = ['data.date', 'data.' + search_by]
    else:
        columns_to_select = ['data.' + search_by]

    for field in fields:
        if adjust and field in table_adj['fields_to_adjust']:
            columns_to_select.append(f'data.{field} {table_adj["operation"]} data.{table_adj["adjustor"]} AS '
                                     f'{field}')
        else:
            columns_to_select.append('data.' + field)

    return ', '.join(set(columns_to_select))


def _create_asset_filter_sql(assets: Union[List[Union[int, str]], str], search_by: str, start_date: str,
                             end_date: str) -> Tuple[str, Optional[pd.DataFrame]]:
    """
    Makes the sql code to filter table by assets
    param assets: the assets we want to get data for, or a universe table
    :param search_by: the identifier we are searching assets by
    :param start_date: the first date to get data on in '%Y-%m-%d' format, only used if assets is a universe table
    :param end_date: the last date to get data on in '%Y-%m-%d' format, only used if assets is a universe table
    :return:
    """

    if isinstance(assets, str):
        sql_code = f"""(SELECT DISTINCT {search_by}
                    FROM universe.{assets}
                    WHERE date >= \'{start_date}\' AND date <= \'{end_date}\')"""
        return sql_code, None

    elif isinstance(assets, pd.Series):
        assets_out = assets.to_frame(search_by)
    elif isinstance(assets, List):
        assets_out = pd.DataFrame(assets, columns=[search_by])
    else:
        raise ValueError(f'Assets type: {type(assets)} not recognised')

    return 'assets_df', assets_out


def _figure_out_date(df, freq) -> pd.DataFrame:
    """
    should the date columns be a timestamp or a period
    :param df: the frame with the date column
    :param freq: the freq of the period. If None then timestamp is returned
    :return: df with date logic applied
    """
    if freq:
        df['date'] = df['date'].dt.to_period(freq)
    return df


def merge_compustat_crsp_tables():
    """
    currently just holding code
    :return:
    """
    # join crsp onto linking table
    sql_query = f"""
        SELECT *
        FROM crsp.security_daily as sd 
            JOIN ccm.crsp_compustat_link as link 
                ON sd.permno = link.lpermno
        where sd.date >= link.LINKDT and  -- data after link start
              sd.date < link.LINKENDDT and  -- data before link end
              link.LINKTYPE  in ('LC', 'LU') and  -- link is good 
              link.LINKPRIM = 'P'  and -- link is for a primary share    
              link.fic  = 'USA' and  -- filtering for only the USA
              sd.date > '1980' 
              ;
    """

    # join compustat onto linking table
    sql_query = f"""
        SELECT fa.gvkey
        FROM compustat.fundamental_annual as fa
            JOIN ccm.crsp_compustat_link as link 
                ON fa.gvkey = link.gvkey
        where fa.date >= link.LINKDT and  -- data after link start
              fa.date < link.LINKENDDT and  -- data before link end
              link.LINKTYPE  in ('LC', 'LU') and  -- link is good 
              link.LINKPRIM = 'P'  and -- link is for a primary share    
              link.fic  = 'USA' and  -- filtering for only the USA
              fa.date > '2000-01-01' 
              ;
    """
