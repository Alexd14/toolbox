import gc
import pandas as pd

from typing import Tuple

from abc import ABC, abstractmethod


class ModelWrapper(ABC):
    """
    Wraps a model for calc_ml_factor.
    """
    @abstractmethod
    def fit_model(self, train_features: pd.DataFrame, train_target: pd.Series) -> any:
        """
        Wraps a model for use by the calcMlFactor function.
        Fits a model to the given features. then returns the fit model.
        If the fit model does not contain a "predict" method then predict mut be overwritten.

        :param train_features: the features to train the model on
            Must have the same index as train_target
        :param train_target: the target for the train_features.
            Must have the same index as train_features
        :return: a model fit to the given features and targets
        """
        pass

    @staticmethod
    def transform_data(train_features: pd.DataFrame, train_target: pd.Series, predict: pd.DataFrame) -> \
            Tuple[pd.DataFrame, pd.DataFrame]:
        """
        *** Do not fit any transformations on the predict data. That WILL result in lookahead Bias.***
        Only manipulate the predict data with transformations fit with the train_features

        This method is used to preprocess the data before the training, and predicting data is passed to the model

        The indexes must not be changed. However columns can be dropped and altered.
        Any change to the train_target must also be done to the predict data.

        Example use: fit a PCA to the train_features then transform the train_features and predict data using said PCA.
                or use RFE to reduce dimensionality

        :param train_features: the features to train the model on
        :param train_target: the target for the train_features
        :param predict: The data to make predictions on
        :return: the transformed (train_features, predict) with no index changes.
        """
        return train_features, predict

    def predict(self, train_features: pd.DataFrame, train_target: pd.Series, predict: pd.DataFrame) -> pd.Series:
        """
        fits a model to the given training data and then makes predictions with the fitted model
        fits a model by calling "fitModel".
        assumes the "fitModel" returns a model with a "predict" method.

        :param train_features: the features to train the model on
            Must have the same index as train_target
        :param train_target: the target for the train_features.
            Must have the same index as train_features
        :param predict: The data to make predictions on
        :return: a Tuple of pandas Series with the predictions and a float what s the
        """
        # checks the index but is very slow
        # if not train_features.index.equals(train_target.index):
        #     raise ValueError('The index for the features and target is different')

        # allowing the user to adjust the data before fitting, assuming that the user does not mess up the indexes
        transformed_features, transformedPredict = self.transform_data(train_features, train_target, predict)

        # fitting and making predictions with user defined model
        model: any = self.fit_model(transformed_features, train_target)
        predicted: pd.Series = pd.Series(data=model.predict(transformedPredict), index=predict.index)

        del model, train_features, train_target, predict, transformed_features, transformedPredict
        gc.collect()

        return predicted
