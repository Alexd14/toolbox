import gc

import pandas as pd
import numpy as np

from abc import ABC, abstractmethod
from typing import Generator, Tuple, List

from tqdm import tqdm

from toolbox.utils.SliceHolder import SliceHolder


class ModelWrapper(ABC):
    @abstractmethod
    def fitModel(self, trainFeatures: pd.DataFrame, trainTarget: pd.Series) -> any:
        """
        Wraps a model for use by the calcMlFactor function.
        Fits a model to the given features. then returns the fit model.
        If the fit model does not contain a "predict" method then predict mut be overwritten.

        :param trainFeatures: the features to train the model on
            Must have the same index as trainTarget
        :param trainTarget: the target for the trainFeatures.
            Must have the same index as trainFeatures
        :return: a model fit to the given features and targets
        """
        pass

    @staticmethod
    def transformData(trainFeatures: pd.DataFrame, trainTarget: pd.Series, predict: pd.DataFrame) -> Tuple[
        pd.DataFrame, pd.DataFrame]:
        """
        *** Do not fit any transformations on the predict data. That WILL result in lookahead Bias.***
        Only manipulate the predict data with transformations fit with the trainFeatures

        This method is used to preprocess the data before the training, and predicting data is passed to the model

        The indexes must not be changed. However columns can be dropped and altered.
        Any change to the trainTarget must also be done to the predict data.

        Example use: fit a PCA to the trainFeatures then transform the trainFeatures and predict data using said PCA.
                or use RFE to reduce dimensionality

        :param trainFeatures: the features to train the model on
        :param trainTarget: the target for the trainFeatures
        :param predict: The data to make predictions on
        :return: the transformed (trainFeatures, predict) with no index changes.
        """
        return trainFeatures, predict

    def predict(self, trainFeatures: pd.DataFrame, trainTarget: pd.Series, predict: pd.DataFrame) -> pd.Series:
        """
        fits a model to the given training data and then makes predictions with the fitted model
        fits a model by calling "fitModel".
        assumes the "fitModel" returns a model with a "predict" method.

        :param trainFeatures: the features to train the model on
            Must have the same index as trainTarget
        :param trainTarget: the target for the trainFeatures.
            Must have the same index as trainFeatures
        :param predict: The data to make predictions on
        :return: a Tuple of pandas Series with the predictions and a float what s the
        """
        if not trainFeatures.index.equals(trainTarget.index):
            raise ValueError('The index for the features and target is different')

        # allowing the user to adjust the data before fitting, assuming that the user does not mess up the indexes
        transformedFeatures, transformedPredict = self.transformData(trainFeatures, trainTarget, predict)

        # fitting and making predictions with user defined model
        model: any = self.fitModel(transformedFeatures, trainTarget)
        predicted: pd.Series = pd.Series(data=model.predict(transformedPredict), index=predict.index)

        # freeing up memory
        del model, trainFeatures, trainTarget, predict, transformedFeatures, transformedPredict
        gc.collect()

        return predicted


def calcMlFactor(model: ModelWrapper, features: pd.DataFrame, target: pd.Series, evalDays: int, refitEvery: int,
                 expanding: int = None, rolling: int = None) -> pd.Series:
    """
    Calculates an alpha factor using a ML factor combination method.
    The model is fit and predictions are made in a ModelWrapper
    This function organizes the data so the model can make unbiased predictions
    on what would have been point in time data.

    this function assumes that the data passed has all trading days in it (first level of index).
    Ex if the the data is missing for one day then we will miss a

    :param model: the ModelWrapper that will be used to make predictions.
    :param features: the features to train the model on
        there cannot be null values
        must have a multi index of (pd.Timestamp, symbol)
    :param target: the target we are going to fit the model to
        there cannot be null values
        must have a multi index of (pd.Timestamp, symbol)
    :param evalDays: IF INCORRECT THERE WILL BE LOOK AHEAD BIAS
        the amount of days it takes to know the predictions outcome
        this number should simply be the length of return we are trying to predict
    :param refitEvery: the amount of consecutive days to predict using a single model
        this is essentially saying refit the model every x days
    :param expanding: the minimum amount of days of data to train on
        if rollingDays is passed then this should not be passed
        if this value is passed then the model will be trained with an expanding window of data
    :param rolling: the amount of rolling days to fit a model to
        if minTrainDays is passed then this should not be passed
    :return: pandas series of predictions. The index will be the same as "features"
    """

    featuresCopy: pd.DataFrame = features.copy().sort_index()
    targetCopy: pd.Series = target.copy().sort_index()

    if (np.isnan(featuresCopy.values).any()) or (not np.isfinite(featuresCopy.values).all()):
        raise ValueError('There are nan or inf values in the features')
    if (np.isnan(targetCopy.values).any()) or (not np.isfinite(targetCopy.values).all()):
        raise ValueError('There are nan or inf values in the target')
    if not isinstance(featuresCopy.index, pd.MultiIndex):
        raise ValueError('Features and target must have a pd.MultiIndex of (pd.Timestamp, str)')
    if not isinstance(featuresCopy.index.get_level_values(0), pd.DatetimeIndex):
        raise ValueError('Features and target must have index level 0 of pd.DatetimeIndex')
    if not featuresCopy.index.equals(targetCopy.index):
        raise ValueError('The index for the features and target is different')

    trainPredictSlices: Generator[Tuple[SliceHolder, SliceHolder], None, None] = \
        generateIndexes(featuresCopy.index, evalDays, refitEvery, expanding, rolling)

    mlAlpha: List[pd.Series] = []
    for trainSlice, predictSlice in tqdm(trainPredictSlices):
        featuresTrain = featuresCopy.loc[trainSlice.getStart():trainSlice.getEnd()]
        targetTrain = targetCopy.loc[trainSlice.getStart():trainSlice.getEnd()]
        predict = featuresCopy.loc[predictSlice.getStart():predictSlice.getEnd()]
        mlAlpha.append(model.predict(featuresTrain, targetTrain, predict))

    del featuresCopy, targetCopy
    gc.collect()

    return pd.concat(mlAlpha)


def generateIndexes(dataIndex: pd.MultiIndex, evalDays: int, refitEvery: int, expanding: int = None,
                    rolling: int = None) -> Generator[Tuple[SliceHolder, SliceHolder], None, None]:
    """
    generates the slice index's for the training and predicting periods.
    function is designed to work with dates in level 0 however this is not enforced anywhere

    :param dataIndex: MultiIndex of the data we are generating int index's for
    :param evalDays: IF INCORRECT THERE WILL BE LOOK AHEAD BIAS
        the amount of days it takes to know the predictions outcome
        this number should simply be the length of return we are trying to predict
    :param refitEvery: the amount of consecutive days to predict using a single model
        this is essentially saying refit the model every x days
    :param expanding: the minimum amount of days of data to train on
        if rollingDays is passed then this should not be passed
        if this value is passed then the model will be trained with an expanding window of data
    :param rolling: the amount of rolling days to fit a model to
        if minTrainDays is passed then this should not be passed
    :return: a generator with each iteration containing a tuple of two SliceHolders of dates.
            Slice One: training indexes
            Slice Two: predicting indexes
    """

    if (evalDays < 1) or (refitEvery < 1):
        raise ValueError('evalDays and/or refitEvery must be greater than zero')
    if rolling is not None and (rolling < 1):
        raise ValueError('rolling must be greater than zero')
    if expanding is not None and (expanding < 1):
        raise ValueError('expanding must be greater than zero')
    if (not bool(expanding)) and (not bool(rolling)):
        raise ValueError('minTrainDays or rollingDays must be defined')
    if bool(expanding) & bool(rolling):
        raise ValueError('minTrainDays and rollingDays can not both be defined')

    dates: np.array = dataIndex.get_level_values(0).drop_duplicates().to_series()

    startPlace = expanding if expanding else rolling
    # dont have to ceil this bc it wont matter with a < operator
    amountOfLoops: float = (len(dates) - startPlace - evalDays) / refitEvery

    i: int = 0
    while i < amountOfLoops:
        # .loc[] is inclusive in a slice, so everything here is inclusive
        trainEndIndex: int = (i * refitEvery) + (startPlace - 1)
        trainStartIndex: int = trainEndIndex - rolling + 1 if rolling else 0
        trainSlice: SliceHolder = SliceHolder(dates[trainStartIndex], dates[trainEndIndex])

        predictStartIndex: int = trainEndIndex + evalDays + 1
        predictEndIndex: int = predictStartIndex + refitEvery - 1
        # accounting for when the ending predicted index is out of bounds on the last loop
        if predictEndIndex >= len(dates) - 1:
            predictEndIndex: int = len(dates) - 1

        predictSlice: SliceHolder = SliceHolder(dates[predictStartIndex], dates[predictEndIndex])

        i += 1
        yield trainSlice, predictSlice

