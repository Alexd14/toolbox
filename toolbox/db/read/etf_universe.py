import glob
import os.path
import pandas as pd

from typing import Union
import pandas_market_calendars as mcal

from toolbox.db.settings import CACHE_DIRECTORY
from toolbox.db.api.sql_connection import SQLConnection

MAP_ETF_SYMBOL_ID = {}


class ETFUniverse:
    """
    CLass to build universes from etf holdings.
    Will cache the universes in parquet files to be read into duckdb instances
    """

    def __init__(self, con: SQLConnection = None):
        """
        :param con: A connection to the database, if not passed then will open a new write connection.
            if the there is universe creation going on then must pass a write connection
        """
        self._con = con if con else SQLConnection(read_only=False)

    def get_universe_df(self, ticker: str = None, crsp_portno: int = None, start_date: str = '2000',
                        end_date: str = '2023') -> Union[pd.DataFrame, str]:
        """
        gets the universe constitutes for an etf
        either ticker or crsp_portno must be passed but not both
        :param ticker: the ticker of the etf we are getting holdings for
        :param crsp_portno: the crsp_portno of the etf we are getting holdings for
        :param start_date: the date to start getting holdings for YYYY-MM-DD, no effect when caching
        :param end_date: the date to stop getting holdings YYYY-MM-DD, no effect when caching
        :return: pd.Dataframe index: int_range; columns: date, permno;
        """

        self._input_checks(ticker=ticker, crsp_portno=crsp_portno)

        asset_id = self._get_crsp_portno(ticker=ticker, crsp_portno=crsp_portno)

        if not self._is_cached_etf(crsp_portno=asset_id):
            etf_uni = self._cache_etf(crsp_portno=asset_id)

        else:
            etf_uni = self._get_cached_etf(crsp_portno=asset_id)

        return etf_uni[(etf_uni['date'] > start_date) & (etf_uni['date'] < end_date)]

    def get_universe_path(self, ticker: str = None, crsp_portno: int = None):
        """
        gets the SQL code to read cached universe constitutes for an etf
        either ticker or crsp_portno must be passed but not both
        if etf isn't cached then will cache the etf
        :param ticker: the ticker of the etf we are getting holdings for
        :param crsp_portno: the crsp_portno of the etf we are getting holdings for
        :return: SQL code to read the cached parquet
        """
        self._input_checks(ticker=ticker, crsp_portno=crsp_portno)

        asset_id = self._get_crsp_portno(ticker=ticker, crsp_portno=crsp_portno)

        if not self._is_cached_etf(crsp_portno=asset_id):
            self._cache_etf(crsp_portno=asset_id)

        return self._get_cached_path(asset_id)

    def get_universe_path_parse(self, to_parse):
        """
        wraper that parses a string for get_universe_path, can tell if user passed a symbol or crsp_portno
        format:
            ticker:
                'ETF_TICKER_SPY'
            crsp_portno:
                'ETF_5648362'
        """
        to_parse = to_parse.upper()
        if 'ETF_TICKER' in to_parse:
            return self.get_universe_path(ticker=to_parse.split('_')[-1])
        elif 'ETF_' in to_parse:
            return self.get_universe_path(crsp_portno=to_parse.split('_')[-1])
        else:
            raise ValueError(f"Can't parse {to_parse}")

    def _cache_etf(self, crsp_portno) -> pd.DataFrame:
        """
        gets and caches an etf holdings query
        will cache etf in temp directory of the computer
        :return: pd.Dataframe index: int_range; columns: date, permno;
        """

        sql_for_holdings = f"""
                SELECT DISTINCT date, permno 
                FROM crsp.portfolio_holdings
                WHERE crsp_portno = {crsp_portno} AND 
                    permno IS NOT NULL
               """
        raw_etf_holdings = self._con.execute(sql_for_holdings).fetchdf()

        df_of_holdings = raw_etf_holdings.set_index('date').groupby('date')['permno'].apply(
            lambda grp: list(grp.value_counts().index))

        end_date = pd.to_datetime('now').date().strftime('%Y-%m-%d')

        trading_cal = mcal.get_calendar(
            'NYSE').valid_days(start_date=df_of_holdings.index.min(),
                               end_date=end_date).tz_localize(None).to_series().to_frame('date')

        universe = trading_cal.join(df_of_holdings).ffill()

        relational_format = [(row[0], permno) for row in universe.values for permno in row[1]]
        uni_df = pd.DataFrame(relational_format, columns=['date', 'permno'])

        self._cache_helper(uni_df=uni_df, crsp_portno=crsp_portno)

        return uni_df

    def _get_crsp_portno(self, ticker, crsp_portno) -> int:
        """
        if ticker is not none then will map the ticker to a crsp_portno
        :return: crsp_portno passed or the crsp_portno mapped to the symbol
        """
        if crsp_portno:
            return crsp_portno

        mapped_id = self._con.execute(f"""SELECT distinct crsp_portno 
                                         FROM crsp.fund_summary 
                                         WHERE ticker = '{ticker}' AND
                                            crsp_portno IS NOT NULL""").fetchall()
        self._con.close()

        if len(mapped_id) == 0:
            raise ValueError(f"Ticker '{ticker}' is not valid cant map to crsp_portno")

        if len(mapped_id) > 1:
            raise ValueError(f"Ticker '{ticker}' mapped to {len(mapped_id)} crsp_portno's {mapped_id}. "
                             f"Please specify the crsp_portno to build this etf's history")

        return int(mapped_id[0][0])

    @staticmethod
    def _input_checks(ticker, crsp_portno) -> None:
        """
        input check for ticker and crsp_portno
        """
        if ticker is None and crsp_portno is None:
            raise ValueError('Must pass a ticker or crsp_portno')

        if ticker is not None and crsp_portno is not None:
            raise ValueError('Must pass a ticker or crsp_portno, not both!')

    def _is_cached_etf(self, crsp_portno) -> bool:
        """
        is a etf cached?
        """
        return os.path.isfile(self._get_cached_path(crsp_portno))

    def _cache_helper(self, uni_df, crsp_portno) -> None:
        """
        Writes a parquet file to the user specified temp directory on a computer
        """
        path = self._get_cached_path(crsp_portno)
        uni_df.to_parquet(path)
        print(f'Cached {crsp_portno} in {path}')

    def _get_cached_etf(self, crsp_portno) -> pd.DataFrame:
        """
        returns a dataframe of the cached universe
        """
        return pd.read_parquet(self._get_cached_path(crsp_portno))

    @staticmethod
    def _get_cached_path(crsp_portno):
        """
        :return: path to the cached file
        """
        return f'{CACHE_DIRECTORY}/etf_uni_{crsp_portno}.parquet'


def clear_cache():
    """
    Clears all files in the CACHE_DIRECTORY path
    """

    files = glob.glob(f'{CACHE_DIRECTORY}/*')
    for f in files:
        os.remove(f)
    print('Cleared Cache')
