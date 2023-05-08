from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
from typing import Dict, Tuple


class RegressionResult:
    def __init__(self, result: LinearRegression, data: pd.DataFrame):
        self.result = result
        self.data = data


class QuantileEmploymentCountsRegression:
    def __init__(self, msa_occ_employment_tables_for_quantiles: Dict[int, pd.DataFrame], productivity_data: pd.DataFrame):
        self.msa_occ_employment_tables_for_quantiles = msa_occ_employment_tables_for_quantiles
        self.productivity_data = productivity_data
        self.n_quantiles = len(msa_occ_employment_tables_for_quantiles)

    def regress_productivity_on_employment_quantile_counts(self, quantile_index: int):
        x = self.msa_occ_employment_tables_for_quantiles[quantile_index].values.flatten()
        y = self.productivity_data.values.flatten()
        regression_data = self.preprocess_regression_data(x=x, y=y)
        lr = self.run_regression(regression_data=regression_data)
        return lr

    def regress_productivity_on_employment_quantile_counts_for_specific_occupations(self, occs: np.ndarray, quantile_index: int):
        x = self.msa_occ_employment_tables_for_quantiles[quantile_index][occs].values.flatten()
        y = self.productivity_data[occs].values.flatten()
        regression_data = self.preprocess_regression_data(x=x, y=y)
        lr = self.run_regression(regression_data=regression_data)
        return lr

    @staticmethod
    def run_regression(regression_data: pd.DataFrame) -> RegressionResult:
        X = regression_data['x'].values.reshape(-1, 1)
        y = regression_data['y'].values
        lr = LinearRegression(fit_intercept=True)
        lr.fit(X=X, y=y)

        prediction = lr.predict(X)
        residual = y - prediction
        regression_data['residual'] = residual
        regression_data['prediction'] = prediction
        regression_data['influence'] = QuantileEmploymentCountsRegression.get_influence_observations(x=X, y=y, residuals=residual)
        return RegressionResult(result=lr, data=regression_data)

    @staticmethod
    def preprocess_regression_data(x: np.ndarray, y: np.ndarray) -> pd.DataFrame:
        data_array = np.array([x, y]).T
        regression_data = pd.DataFrame(data=data_array, columns=['x', 'y'])
        regression_data.dropna(inplace=True)
        regression_data = QuantileEmploymentCountsRegression.rescale_regression_data(regression_data=regression_data)
        return regression_data

    @staticmethod
    def get_influence_observations(x: np.ndarray, y: np.ndarray, residuals: np.ndarray) -> pd.DataFrame:
        x_ = np.vstack([np.ones(len(y)), x.reshape(1,-1)[0]]).T

        xtx_inv = np.linalg.inv(x_.T @ x_)
        # Fancy trick to get the pi_s fast.
        # See https://stackoverflow.com/questions/14758283/is-there-a-numpy-scipy-dot-product-calculating-only-the-diagonal-entries-of-the
        p_is = (x_ @ xtx_inv * x_).sum(axis=1)

        temp = xtx_inv @ x_.T
        influence = np.linalg.norm(((-1/(1-p_is)) * residuals) * temp, axis=0)
        return influence

    @staticmethod
    def rescale_regression_data(regression_data: pd.DataFrame) -> pd.DataFrame:
        regression_data['x'] = np.log(1 + regression_data['x'].values)
        regression_data['y'] = np.log(1 + regression_data['y'].values)
        return regression_data
