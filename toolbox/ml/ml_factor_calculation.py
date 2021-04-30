import gc

import pandas as pd
import numpy as np

from typing import Generator, Tuple, List

from tqdm import tqdm

from toolbox.ml.model_wrapper import ModelWrapper
from toolbox.utils.slice_holder import SliceHolder


def calc_ml_factor(model: ModelWrapper, features: pd.DataFrame, target: pd.Series, eval_days: int, refit_every: int,
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
    :param eval_days: IF INCORRECT THERE WILL BE LOOK AHEAD BIAS
        the amount of days it takes to know the predictions outcome
        this number should simply be the length of return we are trying to predict
    :param refit_every: the amount of consecutive days to predict using a single model
        this is essentially saying refit the model every x days
    :param expanding: the minimum amount of days of data to train on
        if rollingDays is passed then this should not be passed
        if this value is passed then the model will be trained with an expanding window of data
    :param rolling: the amount of rolling days to fit a model to
        if minTrainDays is passed then this should not be passed
    :return: pandas series of predictions. The index will be the same as "features"
    """

    features_copy: pd.DataFrame = features.copy().sort_index()
    target_copy: pd.Series = target.copy().sort_index()

    if not np.isfinite(features_copy.values).all():
        raise ValueError('There are nan or inf values in the features')
    if not np.isfinite(target_copy.values).all():
        raise ValueError('There are nan or inf values in the target')
    if not isinstance(features_copy.index, pd.MultiIndex):
        raise ValueError('Features and target must have a pd.MultiIndex of (pd.Timestamp, str)')
    if not isinstance(features_copy.index.get_level_values(0), pd.DatetimeIndex):
        raise ValueError('Features and target must have index level 0 of pd.DatetimeIndex')
    if not features_copy.index.equals(target_copy.index):
        raise ValueError('The index for the features and target is different')

    train_predict_slices: Generator[Tuple[SliceHolder, SliceHolder], None, None] = \
        generate_indexes(features_copy.index, eval_days, refit_every, expanding, rolling)

    ml_alpha: List[pd.Series] = []
    for train_slice, predict_slice in tqdm(train_predict_slices):
        features_train = features_copy.loc[train_slice.start:train_slice.end]
        target_train = target_copy.loc[train_slice.start:train_slice.end]
        predict = features_copy.loc[predict_slice.start:predict_slice.end]
        ml_alpha.append(model.predict(features_train, target_train, predict))

    del features_copy, target_copy
    gc.collect()

    return pd.concat(ml_alpha)


def generate_indexes(data_index: pd.MultiIndex, eval_days: int, refit_every: int, expanding: int = None,
                     rolling: int = None) -> Generator[Tuple[SliceHolder, SliceHolder], None, None]:
    """
    generates the slice index's for the training and predicting periods.
    function is designed to work with dates in level 0 however this is not enforced anywhere

    :param data_index: MultiIndex of the data we are generating int index's for
    :param eval_days: IF INCORRECT THERE WILL BE LOOK AHEAD BIAS
        the amount of days it takes to know the predictions outcome
        this number should simply be the length of return we are trying to predict
    :param refit_every: the amount of consecutive days to predict using a single model
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

    if (eval_days < 1) or (refit_every < 1):
        raise ValueError('eval_days and/or refit_every must be greater than zero')
    if rolling is not None and (rolling < 1):
        raise ValueError('rolling must be greater than zero')
    if expanding is not None and (expanding < 1):
        raise ValueError('expanding must be greater than zero')
    if (not bool(expanding)) and (not bool(rolling)):
        raise ValueError('minTrainDays or rollingDays must be defined')
    if bool(expanding) & bool(rolling):
        raise ValueError('minTrainDays and rollingDays can not both be defined')

    dates: np.array = data_index.get_level_values(0).drop_duplicates().to_numpy()

    start_place = expanding if expanding else rolling
    # dont have to ceil this bc it wont matter with a < operator
    amount_of_loops: float = (len(dates) - start_place - eval_days) / refit_every

    i: int = 0
    while i < amount_of_loops:
        # .loc[] is inclusive in a slice, so everything here is inclusive
        train_end_index: int = (i * refit_every) + (start_place - 1)
        train_start_index: int = train_end_index - rolling + 1 if rolling else 0
        train_slice: SliceHolder = SliceHolder(dates[train_start_index], dates[train_end_index])

        predict_start_index: int = train_end_index + eval_days + 1
        predict_end_index: int = predict_start_index + refit_every - 1
        # accounting for when the ending predicted index is out of bounds on the last loop
        if predict_end_index >= len(dates) - 1:
            predict_end_index: int = len(dates) - 1

        predict_slice: SliceHolder = SliceHolder(dates[predict_start_index], dates[predict_end_index])

        i += 1
        yield train_slice, predict_slice
