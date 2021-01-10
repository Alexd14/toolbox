import numpy as np


def calculate_ic(yTrue: np.array, yPred: np.array) -> float:
    """
    computes the information coefficient for the predicted and true variables.
    This function can be given to a sklearn.model_selection Hyper-parameter optimizer.

    Example use in sklearn:
        scoring = make_scorer(crossValIC, greater_is_better=True)

    :param yTrue: the true value of the target
    :param yPred: the predicted value of the target
    :return: the information coefficient of the y_pred
    """
    return np.corrcoef(yPred.argsort().argsort(), yTrue.argsort().argsort())[0][1]