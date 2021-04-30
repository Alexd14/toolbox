import pandas as pd
from typing import List, Optional
import pandas_market_calendars as mcal

from toolbox.utils.handle_data import handle_duplicates


class ConstituteAdjustment:
    """
    provides the functionality of correctly identifying on what day which asset should be in/not in the data set based
    on given constitute data
    """

    def __init__(self):
        """
        empty constructor for ConstituteAdjustment
        self.__index_constitutes_factor: holds the index constitutes for the factor in a MultiIndex of date, symbol
        self.__index_constitutes_pricing: holds the index constitutes for the pricing in a MultiIndex of date, symbol
        """
        self.__index_constitutes_factor: Optional[pd.MultiIndex] = None
        self.__index_constitutes_pricing: Optional[pd.MultiIndex] = None

    def add_index_info(self, index_constitutes: pd.DataFrame, start_date: pd.Timestamp = None,
                       end_date: pd.Timestamp = None, date_format: str = '') -> None:
        """
        adds constitute data to the ConstituteAdjustment object.
        creates and stores a pandas multiIndex index with (date, symbol)
        every date a symbol exists in the equity index it will be in the multiIndex
        method has no side effects on passed data. creates a deep copy of indexConstitutes
        If there are duplicate symbols then a Value error will be raised

        Creates a prices and factors index.
        factors index is simply the range of "from" to "thru"
        Prices indexes have period of the "from" field (df) to end_date param (avoids bias)

        :param index_constitutes: a pandas data frame containing index component information.
                                MUST HAVE COLUMNS: 'symbol' representing the symbol,
                                                   'from' start trading date on the index,
                                                   'thru' end trading date on the index,
                                If 'from', 'thru' are not pd.TimeStamps than a date_format MUST BE PASSED.
                                if no date_format is passed its assumed that they are in a pd.TimeStamp object

        :param start_date: The first date we want to get data for, needs to have tz of UTC
        :param end_date: The last first date we want to get data for, needs to have tz of UTC
        :param date_format: if fromCol AND thruCol are both strings then the format to parse them in to dates
        :return: None
        """
        # making sure date and symbol are in the columns
        _check_columns(['symbol', 'from', 'thru'], index_constitutes.columns)

        # setting a copy of indexConstitutes so we dont mutate anything
        index_constitutes: pd.DataFrame = index_constitutes.copy()[['symbol', 'from', 'thru']]

        # will throw an error if there sre duplicate symbols
        handle_duplicates(df=index_constitutes[['symbol']], out_type='ValueError', name='The column symbols',
                          drop=False)

        # seeing if we have to convert from and thru to series of timestamps
        if date_format != '':
            index_constitutes['from'] = pd.to_datetime(index_constitutes['from'], format=date_format) \
                .dt.tz_localize('UTC')
            index_constitutes['thru'] = pd.to_datetime(index_constitutes['thru'], format=date_format) \
                .dt.tz_localize('UTC')

        relevant_cal = mcal.get_calendar('NYSE').valid_days(start_date=start_date, end_date=end_date).to_series()

        # making a list of series to eventually concat
        indexes_factor: List[pd.Series] = []
        indexes_pricing: List[pd.Series] = []

        for row in index_constitutes.iterrows():
            symbol = row[1]['symbol']

            # getting the relevant dates for the factor
            date_range_factors: pd.Series = relevant_cal.loc[row[1]['from']: row[1]['thru']]
            # pricing data is only concerned with entrance bc of return calculation
            date_range_prices: pd.Series = relevant_cal.loc[row[1]['from']:]

            # converting to frame and then stacking gives us a df with the index we are making, also speed improvement
            indexes_factor.append(
                date_range_factors.to_frame(symbol).stack()
            )
            indexes_pricing.append(
                date_range_prices.to_frame(symbol).stack()
            )

        # getting the index of the concatenated Series
        self.__index_constitutes_factor = pd.concat(indexes_factor).index.set_names(['date', 'symbol'])
        self.__index_constitutes_pricing = pd.concat(indexes_pricing).index.set_names(['date', 'symbol'])

    def adjust_data_for_membership(self, data: pd.DataFrame, contents: str, date_format: str = '') -> pd.DataFrame:
        """
        adjusts the data set accounting for when assets are a member of the index defined in add_index_info.
        add_index_info must be declared before this method
        this method has no side effects on passed data
        Must contain columns named 'symbol', 'date' otherwise can have as may columns as desired

        factor:
            Ex: AAPl joined S&P500 on 2012-01-01 and leaves 2015-01-01. GOOGL joined S&P500 on 2014-01-01 and is still
            in the index at the time of end_date passed in add_index_info. When passing data to the
            adjust_data_for_membership method it will only return AAPL factor data in range
            2012-01-01 to 2015-01-01 and google data in the range of 2014-01-01 to the end_date.
        pricing:
            keeping with the example above we would get AAPL pricing data in range 2012-01-01 to end_date and google
            data in the range of 2014-01-01 to the end_date.

        :param data: a pandas dataframe to be filtered.
                    Must have a default index.
                    Must have columns named 'symbol', 'date'
        :param contents: Is the data set, "pricing" or "factor".
        :param date_format: the format of the date column if the date column is a string.
        :return: a indexed data frame adjusted for index constitutes
        """

        # if the add_index_info is not defined then throw error
        if self.__index_constitutes_pricing is None:
            raise ValueError('Index constitutes are not set')

        # making sure date and symbol are in the columns
        _check_columns(['date', 'symbol'], data.columns)

        # setting a copy of data so we dont mutate anything
        data: pd.DataFrame = data.copy()
        # dropping duplicates and throwing a warning if there are any
        data = handle_duplicates(df=data, out_type='Warning', name='Data', drop=True)

        # seeing if we have to convert from and thru to series of timestamps
        if date_format != '':
            try:
                data['date'] = pd.to_datetime(data['date'], format=date_format).dt.tz_localize('UTC')
            except TypeError:
                data['date'] = pd.to_datetime(data['date'], format=date_format).dt.tz_convert('UTC')

        data.set_index(['date', 'symbol'], inplace=True)

        # join is about 40% faster then reindexing
        if contents == 'pricing':
            return self.__index_constitutes_pricing.to_frame().join(data).drop(['date', 'symbol'], axis=1)
        if contents == 'factor':
            return self.__index_constitutes_factor.to_frame().join(data).drop(['date', 'symbol'], axis=1)
        else:
            raise ValueError(
                f'Representation {contents} is not recognised. Valid arguments are "pricing", "factor"')

    @property
    def factor_components(self) -> Optional[pd.MultiIndex]:
        """
        :return: Mutable list of tuples which represent the factor index constitutes
        """
        return self.__index_constitutes_factor

    @property
    def pricing_components(self) -> Optional[pd.MultiIndex]:
        """
        :return: Mutable list of tuples which represent the pricing index constitutes
        """
        return self.__index_constitutes_pricing


def _check_columns(needed: List[str], given_cols: pd.Index) -> None:
    """
    helper to check if the required columns are present
    raises value error if a col in needed is not in givenCols
    :param needed: list of needed columns
    :param given_cols: df.columns of the given data
    """
    for col in needed:
        if col not in given_cols:
            raise ValueError(f'Required column \"{col}\" is not present')
