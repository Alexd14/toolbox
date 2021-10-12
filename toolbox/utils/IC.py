import numpy as np


def calculate_ic(y_true: np.array, y_pred: np.array) -> float:
    """
    computes the information coefficient for the predicted and true variables.
    This function can be given to a sklearn.model_selection Hyper-parameter optimizer.

    Example use in sklearn:
        scoring = make_scorer(crossValIC, greater_is_better=True)

    :param y_true: the true value of the target
    :param y_pred: the predicted value of the target
    :return: the information coefficient of the y_pred
    """
    return np.corrcoef(y_true, y_pred)[0][1]
