import pandas as pd
import numpy as np


def handleDuplicates(df: pd.DataFrame, outType: str, name: str, drop: bool = False, ):
    """
    Checking to see if there are duplicates in the given data frame
    if there are duplicates outType will be used
        Ex: give a Warning or raise ValueError
    :param df: The data we are checking
    :param name: the name of the data to give as output
    :param outType: what to do do if there are duplicates. Currently supports "Warning", "ValueError"
    :param drop: boolean to drop the duplicates or not
        if False no data frame will be returned and vice verse
        this param will not matter if outType is a ValueError
    :return: the given df with duplicates dropped according to drop
    """
    # seeing if there are duplicates in the factor
    dups = df.duplicated()
    if dups.any():
        amountOfDups = dups.sum()
        outString = f'{name} is {round(amountOfDups / len(df), 3)} duplicates, {amountOfDups} rows\n'
        if outType == 'Warning':
            Warning(outString)
        elif outType == 'ValueError':
            raise ValueError(outString)
        else:
            raise ValueError(f'outType {outType} not recognised')

        # dropping the duplicates
        if drop:
            return df[~df.index.duplicated(keep='first')]

    if drop:
        return df


def makeNanInfSummary(df: pd.DataFrame, maxLoss: float) -> pd.DataFrame:
    """
    makes a summary fot the the amount of nan and infinity values in the given data frame
    will throw a ValueError if the percent of nan and inf is greater than the given threshold
    prints a summary of the nan's and inf of there are any
    :param df: the data frame we are checking
    :param maxLoss: max decimal percent of nan and inf we are allowing the df to contain
    :return: pandas data frame with the nan and inf dropped
    """
    dfNumpy = df.to_numpy()
    nanArray = np.isnan(dfNumpy)
    finiteArray = np.logical_or(np.isinf(dfNumpy), np.isneginf(dfNumpy))

    if nanArray.any() or (not finiteArray.all()):
        factorLength = len(df)
        amountNan = nanArray.sum()
        amountInf = finiteArray.sum()
        totalPercentDropped = (amountNan + amountInf) / factorLength

        outString = f'Dropped {round(totalPercentDropped * 100, 2)}% of data. ' \
                    f'{round((amountNan / factorLength) * 100, 2)}% due to nan, ' \
                    f'{round((amountInf / factorLength) * 100, 2)}% of inf values. Threshold: {maxLoss * 100}%\n'

        if totalPercentDropped > maxLoss:
            raise ValueError('Exceeded Nan Infinity Threshold. ' + outString)

        # print out string as a summary
        print(outString)

        # dropping the nans and the infinity values
        df = df.replace([np.inf, -np.inf], np.nan).dropna()

    else:
        print('Dropped 0% of data')

    return df
