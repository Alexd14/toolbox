from typing import List, Optional, Union

import pandas as pd
import pandas_market_calendars as mcal

from toolbox.db.api.sql_connection import SQLConnection
from toolbox.db.settings import DB_ADJUSTOR_FIELDS


class QueryConstructor:
    """
    constructs dynamic queries to go and hit the database

    make assets = None query whole db

    Functionality:
        possibly cache the data in a feather file
    """

    def __init__(self, sql_con: SQLConnection = None, freq: Optional[str] = 'D'):
        """
        :param sql_con: the connection to the database, if non is passed then will use default SQLConnection
        :param freq: frequency for the period, if None then return a Timestamp
        """
        self._con: SQLConnection = sql_con if sql_con else SQLConnection()

        self._query_string = {'select': '', 'from': '', 'where': '', 'group_by': ''}
        self._df_options = {'freq': freq, 'index': []}

    @property
    def raw_sql(self):
        """
        returns the raw sql query the user has created
        """
        where_clause = 'WHERE ' + self._query_string['where'] if self._query_string['where'] != '' else ''
        group_by_clause = 'GROUP BY ' + self._query_string['group_by'] if self._query_string['group_by'] != '' else ''

        query_string = f"""
                    SELECT {self._query_string['select']}
                    FROM {self._query_string['from']}
                    {where_clause}
                    {group_by_clause}
                """

        return query_string

    @property
    def df(self):
        """
        executes the sql query that the user has created
        """
        raw_df = self._con.execute(self.raw_sql).fetchdf()
        self._con.close()

        return self._make_df_changes(raw_df)

    def _make_df_changes(self, raw_df):
        """
        makes the changes to the dataframe query specified by self._df_options
        :param raw_df: the dataframe we are applying the changes to
        """
        if self._df_options['freq']:
            raw_df['date'] = raw_df['date'].dt.to_period(self._df_options['freq'])

        raw_df = raw_df.set_index(self._df_options['index']) if self._df_options['index'] else raw_df

        return raw_df

    def query_timeseries_table(self, table: str, fields: List[str], assets: Optional[Union[List[any], str]],
                               search_by: str, start_date: str, end_date: str = '3000', adjust: bool = True):
        """
        constructs a query to get timeseries data from the database
        :param assets: the assets we want to get data for, or a universe table
        :param search_by: the identifier we are searching by
        :param fields: the fields we are getting in our query
        :param table: the table we are searching must be prefixed by the schema
        :param start_date: the first date to get data on in '%Y-%m-%d' format
        :param end_date: the last date to get data on in '%Y-%m-%d' format
        :param adjust: should we adjust the pricing?
        :return: Pandas Dataframe Columns: fields; Index: ('date', search_by)
        """

        if adjust and table.lower() not in DB_ADJUSTOR_FIELDS:
            raise ValueError(f'Table {table} is not in DB_ADJUSTOR_FIELDS. '
                             f'Valid tables are {list(DB_ADJUSTOR_FIELDS.keys())}')

        self._query_string['select'] = self._create_columns_to_select_sql(table, search_by, fields, adjust)

        asset_filter_sql = self._create_asset_filter_sql(assets, search_by, start_date, end_date)
        self._query_string['from'] = f"""{table} AS data JOIN {asset_filter_sql} AS uni
                                            ON data.{search_by} = uni.{search_by}"""

        self._query_string['where'] = f"""data.date >= '{start_date}' AND data.date <= '{end_date}'"""

        self._df_options['index'] = ['date', search_by]

        return self

    def query_static_table(self, table: str, fields: List[str], assets: Optional[Union[List[any], str]],
                           search_by: str, start_date: str = '1900', end_date: str = '3000'):
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
        select_col_sql = self._create_columns_to_select_sql(table, search_by, fields, False, False)
        asset_filter_sql = self._create_asset_filter_sql(assets, search_by, start_date, end_date)

        self._query_string['select'] = f"""{select_col_sql}, min(data.date) AS min_date, max(data.date) AS max_data"""
        self._query_string['from'] = f"""{table} AS data JOIN {asset_filter_sql} AS uni
                                            ON data.{search_by} = uni.{search_by}"""
        self._query_string['where'] = f"""data.date >= '{start_date}' AND data.date <= '{end_date}'"""
        self._query_string['group_by'] = select_col_sql

        self._df_options['index'] = [search_by]
        self.set_freq(None)

        return self

    def query_universe_table(self, table: str, fields: List[str], start_date: str, end_date: str,
                             index: List[str] = None):
        """
        makes sql query for a timeseries of a universe table
        :param table: the universe table to query
        :param fields: to full from the table
        :param start_date: first date to query on
        :param end_date: last date to query on
        :param index: what should the index of the returned frame be
        :return: Pandas Dataframe Columns: fields; Index: 'date', fields
        """
        select_col_sql = self._create_columns_to_select_sql(table=table, search_by='date', fields=fields, adjust=False,
                                                            use_date=False).replace('data.', '')
        self._query_string['select'] = select_col_sql
        self._query_string['from'] = f"""universe.{table}"""
        self._query_string['where'] = f"""date >= '{start_date}' AND date <= '{end_date}'"""

        if index:
            self._df_options['index'] = index

        return self

    def distinct(self):
        """
        will make add a distinct keyword to the select clause of a query
        """
        self._query_string['select'] = 'DISTINCT ' + self._query_string['select']
        return self

    def set_freq(self, freq: Optional[str]):
        """
        sets the freq to apply to the date column
        freq of None will return a timestamp
        :param freq: the freq to set
        """
        self._df_options['freq'] = freq
        return self

    def set_calendar(self, calendar: str = 'NYSE'):
        """
        sets the trading calendar to reindex the query by
        :param calendar: trading calendar we are resampling to
        """
        #start_date = self._query_string['from'].
        trading_cal = mcal.get_calendar(
            calendar).valid_days(start_date='1970', end_date=pd.to_datetime('today')).to_series().to_frame(
            'trading_day')

        self._con.con.register('trading_cal', trading_cal)

        self._query_string['from'] += """INNER JOIN trading_cal ON data.date = trading_cal.trading_day"""
        return self

    def resample(self, calendar: str):
        """
        will resample any data down to daily data with the specified calendar
        :param calendar: trading calendar we are resampling to
        """
        return self

    def add_ccm_link(self, have: str):
        """
        will add a column for permno or gvkey and iid, using the ccm linker table
        :param have: the table we currently have 'cstat' or 'crsp'
        """
        return self

    def _create_asset_filter_sql(self, assets: Union[List[Union[int, str]], str], search_by: str, start_date: str,
                                 end_date: str) -> str:
        """
        Makes the sql code to filter table by assets
        param assets: the assets we want to get data for, or a universe table
            if not table then will register the passed assets as a view so they can be refrenced by the query
        :param search_by: the identifier we are searching assets by
        :param start_date: the first date to get data on in '%Y-%m-%d' format, only used if assets is a universe table
        :param end_date: the last date to get data on in '%Y-%m-%d' format, only used if assets is a universe table
        :return: sql query for getting to the assets we want
        """

        if isinstance(assets, str):
            sql_code = f"""(SELECT DISTINCT {search_by}
                        FROM universe.{assets}
                        WHERE date >= \'{start_date}\' AND date <= \'{end_date}\')"""
            return sql_code

        elif isinstance(assets, pd.Series):
            self._con.con.register('assets_df', (assets.to_frame(search_by)))
        elif isinstance(assets, List):
            self._con.con.register('assets_df', pd.DataFrame(assets, columns=[search_by]))
        else:
            raise ValueError(f'Assets type: {type(assets)} not recognised')

        return 'assets_df'

    @staticmethod
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
        table_adj = DB_ADJUSTOR_FIELDS.get(table.lower())

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
