import statsmodels.api as sm
import numpy as np


class Regression:
    def __init__(self):
        pass

    @staticmethod
    def _run_regression(x: np.ndarray, y: np.ndarray):
        x_ = sm.add_constant(x)
        model = sm.OLS(y, x_).fit()
        return model