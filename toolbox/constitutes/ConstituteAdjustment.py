import pandas as pd
from typing import List, Tuple
import pandas_market_calendars as mcal

from toolbox.utils.HandleData import handleDuplicates


class ConstituteAdjustment:
    """
    takes in constitute data on a index
    provides the functionality of correctly identifying on what day which asset should be in/not in the data set
    """

    def __init__(self):
        """
        empty constructor for ConstituteAdjustment
        self.__indexConstitutesFactor: holds the index constitutes for the factor in a List[Tuples[pd.DateTime, str]]
        self.__indexConstitutesPricing: holds the index constitutes for the pricing in a List[Tuples[pd.DateTime, str]]
        """
        self.__indexConstitutesFactor: List[Tuple[any, any]] = []
        self.__indexConstitutesPricing: List[Tuple[any, any]] = []

    def addIndexInfo(self, indexConstitutes: pd.DataFrame, pad: int, startingDate: pd.Timestamp = None,
                     endingDate: pd.Timestamp = None, dateFormat: str = '') -> None:
        """
        initializes the ConstituteAdjustment object.
        creates and stores a pandas multiIndex index with (date, symbol)
        every date a symbol exists in the equity index it will be in the multiIndex
        method has no side effects. creates a deep copy of indexConstitutes
        If there are duplicate symbols then a Value error will be raised
        ASSUMES DATES ARE IN TZ: UTZ

        :param indexConstitutes: a pandas data frame containing index component information.
                                MUST HAVE COLUMNS: 'symbol' representing the symbol,
                                                   'from' start trading date on the index,
                                                   'thru' end trading date on the index,
                                If 'from', 'thru' are not pd.TimeStamps than a dateFormat MUST BE PASSED.
                                if no dateFormat is passed its assumed that they are in a pd.TimeStamp object

        :param pad: the max return length for the pricing data. there will be bias in the data of this is too short
        :param startingDate: The first date we want to get data for, needs to have tz of UTC
        :param endingDate: The last first date we want to get data for, needs to have tz of UTC
        :param dateFormat: if fromCol AND thruCol are both strings then the format to parse them in to dates
        :return: None
        """
        # making sure date and symbol are in the columns
        _checkColumns(['symbol', 'from', 'thru'], indexConstitutes.columns)

        # setting a copy of indexConstitutes so we dont mutate anything
        indexConstitutes: pd.DataFrame = indexConstitutes.copy()[['symbol', 'from', 'thru']]

        # will throw an error if there sre duplicate symbols
        handleDuplicates(df=indexConstitutes[['symbol']], outType='ValueError', name='The column symbols', drop=False)

        # seeing if we have to convert from and thru to series of timestamps
        if dateFormat != '':
            indexConstitutes['from'] = pd.to_datetime(indexConstitutes['from'], format=dateFormat).dt.tz_localize('UTC')
            indexConstitutes['thru'] = pd.to_datetime(indexConstitutes['thru'], format=dateFormat).dt.tz_localize('UTC')

        # making the calendar so we can just slice it every iteration instead of making a new one
        relevantCal = mcal.get_calendar('NYSE').valid_days(start_date=startingDate, end_date=endingDate).to_series()

        # making a list of tuples to quickly index the data
        indexesFactor: List[Tuple[any, any]] = []
        indexesPricing: List[Tuple[any, any]] = []

        for row in indexConstitutes.iterrows():
            # getting the relevant dates
            dateRangeFactors: pd.Series = relevantCal.loc[row[1]['from']: row[1]['thru']]
            dateRangePrices: pd.Series = relevantCal.loc[row[1]['from']: row[1]['thru'] + pd.Timedelta(days=pad)]
            # setting the symbol
            dateRangeFactors.loc[:] = row[1]['symbol']
            dateRangePrices.loc[:] = row[1]['symbol']
            indexesFactor.extend(list(zip(dateRangeFactors.index, dateRangeFactors)))
            indexesPricing.extend(list(zip(dateRangePrices.index, dateRangePrices)))

        self.__indexConstitutesFactor = indexesFactor
        self.__indexConstitutesPricing = indexesPricing

    def adjustDataForMembership(self, data: pd.DataFrame, representation: str, dateFormat: str = '') -> pd.DataFrame:
        """
        adjusts the data for when they are a member of the equity index defined in addIndexInfo.
        addIndexInfo must be declared before this method
        this method has no side effects
        Must contain columns named 'symbol', 'date' otherwise can have as may columns as desired

        Ex: AAPl joined S&P500 on 2012-01-01 and leaves 2015-01-01. GOOGL joined S&P500 on 2014-01-01 and is still in
        the index today. When passing data to the adjustDataForMembership method it will only return AAPL data in range
        2012-01-01 to 2015-01-01 and google data in the range of 2014-01-01 to the current day.

        :param data: a pandas dataframe to be filtered.
                    Must have a default index.
                    Must have columns named 'symbol', 'date'
        :param representation: Is the data "pricing" or "factor".
        :param dateFormat: the format of the date column if the date column is a string.
        :return: a indexed data frame adjusted for index constitutes
        """

        # if the addIndexInfo is not defined then throw error
        if not self.__indexConstitutesPricing:
            raise ValueError('Index constitutes are not set')

        # making sure date and symbol are in the columns
        _checkColumns(['date', 'symbol'], data.columns)

        # setting a copy of data so we dont mutate anything
        data: pd.DataFrame = data.copy()
        # dropping duplicates and throwing a warning if there are any
        data = handleDuplicates(df=data, outType='Warning', name='Data', drop=True)

        # seeing if we have to convert from and thru to series of timestamps
        if dateFormat != '':
            data['date'] = pd.to_datetime(data['date'], format=dateFormat).dt.tz_localize('UTC')

        data.set_index(['date', 'symbol'], inplace=True)

        # add functionality to show hub much data is missing
        if representation == 'pricing':
            return data.reindex(index=self.__indexConstitutesPricing)
        elif representation == 'factor':
            return data.reindex(index=self.__indexConstitutesFactor)
        else:
            raise ValueError(
                f'Representation {representation} is not recognised. Valid arguments are "pricing", "factor"')

    @property
    def components(self) -> List[Tuple[any, any]]:
        """
        :return: Mutable list of tuples which represent the index constitutes
        """
        return self.__indexConstitutesFactor


def _checkColumns(needed: List[str], givenCols: pd.Index) -> None:
    """
    helper to check if the required columns are present
    raises value error if a col in needed is not in givenCols
    :param needed: list of needed columns
    :param givenCols: df.columns of the given data
    """
    for col in needed:
        if col not in givenCols:
            raise ValueError(f'Required column \"{col}\" is not present')
