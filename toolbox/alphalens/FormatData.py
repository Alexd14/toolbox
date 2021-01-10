import pandas as pd

from toolbox.utils.HandleData import handleDuplicates, makeNanInfSummary


def priceFormatForAlphalens(data: pd.DataFrame, factor: str, dateFormat: str = '') -> pd.DataFrame:
    """
    formats the price data into the expected format by get_clean_factor_and_forward_returns
    out format of the data frame: index: 'date', columns: 'symbols'
    data must contain 'date', 'symbol', can take in a dataframe with unlimited columns
    given df the 2 required columns names: 'date', 'symbol'

    does not mutate the given dataframe

    :param data: the data to be turned into the format expected by prices feild in get_clean_factor_and_forward_returns
    :param factor: the name of the factor column in the passed data
    :param dateFormat: the format to parse the date column in pd.datetime
    `   pass None if no date conversion is wanted
    :return: data frame with data in format required by factor field in get_clean_factor_and_forward_returns
    """
    data: pd.DataFrame = data.copy()

    _checkColumns(data)
    _convertToDateTime(data, dateFormat)

    pivotTable: pd.DataFrame = data.pivot_table(index='date', columns='symbol', values=factor)
    pivotTable.index = pd.DatetimeIndex(pivotTable.index)

    return pivotTable


def factorFormatForAlphalens(data: pd.DataFrame, factor: str, dateFormat: str = '',
                             maxLoss: float = .1) -> pd.DataFrame:
    """
    formats the alpha factor data into the expected format by get_clean_factor_and_forward_returns
    data must contain 'date', 'symbol', can take in a dataframe with unlimited columns
    out format of the data frame: index: ('date', 'symbols'), columns: 'singleFactor'
    given df the 2 required columns names: 'date', 'symbol'

    does not mutate the given data frame

    :param data: the data to be turned into the format expected by factor field in get_clean_factor_and_forward_returns
    :param factor: the name of the factor column in the passed data
    :param dateFormat: the format to parse the date column in pd.datetime
    `   pass None if no date conversion is wanted
    :param maxLoss: the decimal percent of the factor that can be nan or infinity before we throw an error
    :return: data frame with data in required format by factor field in get_clean_factor_and_forward_returns
    """
    data: pd.DataFrame = data.copy()

    _checkColumns(data)
    _convertToDateTime(data, dateFormat)

    # setting the index
    alphaFactor = data[['date', 'symbol', factor]].set_index(['date', 'symbol'])
    # dropping duplicates and printing a warning
    alphaFactor = handleDuplicates(df=alphaFactor, outType='Warning', name='Given Factor', drop=True)
    # making a nan and inf summary along with dropping nan's
    alphaFactor = makeNanInfSummary(df=alphaFactor, maxLoss=maxLoss)

    return alphaFactor


def _checkColumns(data: pd.DataFrame) -> None:
    """
    checking to make sure the columns contain 'date' & 'symbol'
    :param data: the data frame to check
    :return: Void, throws ValueError if the columns are bad
    """
    # checking for the columns 'date' & 'symbol'
    for needed in ['date', 'symbol']:
        if needed not in data.columns:
            raise ValueError('given df must have required columns \'date\' \'symbol\'')


def _convertToDateTime(data: pd.DataFrame, dateFormat: str) -> None:
    """
    MUTATES the given dataframe
    converts the date column to a pd.dateTime object.
    If the dateFormat is a empty string then nothing is changed
    :param data: the data frame to have the date chamged
    :param dateFormat: the format of the date time string
    :return: Void
    """

    if dateFormat != '':
        data['date'] = pd.to_datetime(data['date'], format=dateFormat, utc=True).dt.date
