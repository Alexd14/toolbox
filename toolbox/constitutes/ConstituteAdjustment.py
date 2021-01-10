import pandas as pd
from typing import List, Tuple
import pandas_market_calendars as mcal

from toolbox.utils.HandleData import handle_duplicates


class ConstituteAdjustment:
    """
    provides the functionality of correctly identifying on what day which asset should be in/not in the data set based
    on given constitute data
    """

    def __init__(self):
        """
        empty constructor for ConstituteAdjustment
        self.__index_constitutes_factor: holds the index constitutes for the factor in a List[Tuples[pd.DateTime, str]]
        self.__index_constitutes_pricing: holds the index constitutes for the pricing in a List[Tuples[pd.DateTime, str]]
        """
        self.__index_constitutes_factor: List[Tuple[any, any]] = []
        self.__index_constitutes_pricing: List[Tuple[any, any]] = []

    def add_index_info(self, index_constitutes: pd.DataFrame, pad: int, starting_date: pd.Timestamp = None,
                       ending_date: pd.Timestamp = None, date_format: str = '') -> None:
        """
        adds constitute data to the ConstituteAdjustment object.
        creates and stores a pandas multiIndex index with (date, symbol)
        every date a symbol exists in the equity index it will be in the multiIndex
        method has no side effects on passed data. creates a deep copy of indexConstitutes
        If there are duplicate symbols then a Value error will be raised
        ASSUMES DATES ARE IN TZ: UTZ

        :param index_constitutes: a pandas data frame containing index component information.
                                MUST HAVE COLUMNS: 'symbol' representing the symbol,
                                                   'from' start trading date on the index,
                                                   'thru' end trading date on the index,
                                If 'from', 'thru' are not pd.TimeStamps than a date_format MUST BE PASSED.
                                if no date_format is passed its assumed that they are in a pd.TimeStamp object

        :param pad: the max return length for the pricing data. There will be bias in the data of this is too short
        :param starting_date: The first date we want to get data for, needs to have tz of UTC
        :param ending_date: The last first date we want to get data for, needs to have tz of UTC
        :param date_format: if fromCol AND thruCol are both strings then the format to parse them in to dates
        :return: None
        """
        # making sure date and symbol are in the columns
        _check_columns(['symbol', 'from', 'thru'], index_constitutes.columns)

        # setting a copy of indexConstitutes so we dont mutate anything
        index_constitutes: pd.DataFrame = index_constitutes.copy()[['symbol', 'from', 'thru']]

        # will throw an error if there sre duplicate symbols
        handle_duplicates(df=index_constitutes[['symbol']], out_type='ValueError', name='The column symbols', drop=False)

        # seeing if we have to convert from and thru to series of timestamps
        if date_format != '':
            index_constitutes['from'] = pd.to_datetime(index_constitutes['from'], format=date_format).dt.tz_localize('UTC')
            index_constitutes['thru'] = pd.to_datetime(index_constitutes['thru'], format=date_format).dt.tz_localize('UTC')

        # making the calendar so we can just slice it every iteration instead of making a new one
        relevant_cal = mcal.get_calendar('NYSE').valid_days(start_date=starting_date, end_date=ending_date).to_series()

        # making a list of tuples to quickly index the data
        indexes_factor: List[Tuple[any, any]] = []
        indexes_pricing: List[Tuple[any, any]] = []

        for row in index_constitutes.iterrows():
            # startIndex: int = relevant_cal.index.get_loc(row[1]['from'])
            # endIndex: int = relevant_cal.index.get_loc(row[1]['thru'])

            # date_range_factors: pd.Series = relevant_cal.iloc[startIndex: endIndex]
            # date_range_prices: pd.Series = relevant_cal.iloc[startIndex: endIndex + pad]

            # getting the relevant dates
            date_range_factors: pd.Series = relevant_cal.loc[row[1]['from']: row[1]['thru']]

            # getting data by indexes to ensure that there is no mismatch for the pad period.
            # will lilley have to be speed up in the future
            date_range_prices: pd.Series = relevant_cal.loc[row[1]['from']: row[1]['thru'] + pd.Timedelta(pad)]

            # setting the symbol
            date_range_factors.loc[:] = row[1]['symbol']
            date_range_prices.loc[:] = row[1]['symbol']
            indexes_factor.extend(list(zip(date_range_factors.index, date_range_factors)))
            indexes_pricing.extend(list(zip(date_range_prices.index, date_range_prices)))

        self.__index_constitutes_factor = indexes_factor
        self.__index_constitutes_pricing = indexes_pricing

    def adjust_data_for_membership(self, data: pd.DataFrame, representation: str, date_format: str = '') -> pd.DataFrame:
        """
        adjusts the data set accounting for when assets are a member of the index defined in add_index_info.
        add_index_info must be declared before this method
        this method has no side effects on passed data
        Must contain columns named 'symbol', 'date' otherwise can have as may columns as desired

        Ex: AAPl joined S&P500 on 2012-01-01 and leaves 2015-01-01. GOOGL joined S&P500 on 2014-01-01 and is still in
        the index today. When passing data to the adjust_data_for_membership method it will only return AAPL data in range
        2012-01-01 to 2015-01-01 and google data in the range of 2014-01-01 to the current day.

        :param data: a pandas dataframe to be filtered.
                    Must have a default index.
                    Must have columns named 'symbol', 'date'
        :param representation: Is the data set, "pricing" or "factor".
        :param date_format: the format of the date column if the date column is a string.
        :return: a indexed data frame adjusted for index constitutes
        """

        # if the add_index_info is not defined then throw error
        if not self.__index_constitutes_pricing:
            raise ValueError('Index constitutes are not set')

        # making sure date and symbol are in the columns
        _check_columns(['date', 'symbol'], data.columns)

        # setting a copy of data so we dont mutate anything
        data: pd.DataFrame = data.copy()
        # dropping duplicates and throwing a warning if there are any
        data = handle_duplicates(df=data, out_type='Warning', name='Data', drop=True)

        # seeing if we have to convert from and thru to series of timestamps
        if date_format != '':
            data['date'] = pd.to_datetime(data['date'], format=date_format).dt.tz_localize('UTC')

        data.set_index(['date', 'symbol'], inplace=True)

        # add functionality to show hub much data is missing
        if representation == 'pricing':
            return data.reindex(index=self.__index_constitutes_pricing)
        elif representation == 'factor':
            return data.reindex(index=self.__index_constitutes_factor)
        else:
            raise ValueError(
                f'Representation {representation} is not recognised. Valid arguments are "pricing", "factor"')

    @property
    def factor_components(self) -> List[Tuple[any, any]]:
        """
        :return: Mutable list of tuples which represent the factor index constitutes
        """
        return self.__index_constitutes_factor

    @property
    def pricing_components(self) -> List[Tuple[any, any]]:
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
