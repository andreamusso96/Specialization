from EmploymentRegression.QuantileEmploymentCountsRegression import QuantileEmploymentCountsRegression
import plotly.graph_objs as go
import numpy as np
from typing import List, Dict, Any


class PlotRegressionResult:
    def __init__(self, regression: QuantileEmploymentCountsRegression):
        self.regression = regression
        self.fig = go.Figure()
        self.quantiles = np.arange(1, self.regression.n_quantiles + 1) / self.regression.n_quantiles

    def plot(self, occupations: Dict[Any, List[str]] = None, title: str = None):
        if occupations is None:
            self._productivity_on_quantile_employment_counts()
        else:
            for label in occupations:
                self._productivity_on_quantile_employment_counts_for_specific_occupations(occupations=occupations[label], label=label)

        self._layout(title=title)
        self.fig.show(renderer='browser')

    def _productivity_on_quantile_employment_counts(self):
        regression_results = np.array([self.regression.regress_productivity_on_employment_quantile_counts(quantile_index=quantile_index) for quantile_index in range(self.regression.n_quantiles)])
        quantile_coefficients = np.array([reg_res.coef_[0] for reg_res in regression_results])
        trace = go.Scatter(x=self.quantiles, y=quantile_coefficients, name=f'Average quantile coefficient')
        self.fig.add_trace(trace)
        self._layout()
        self.fig.show(renderer='browser')

    def _productivity_on_quantile_employment_counts_for_specific_occupations(self, occupations: List[str] or np.ndarray, label: str):
        regression_results = [self.regression.regress_productivity_on_employment_quantile_counts_for_specific_occupations(occs=occupations, quantile_index=quantile_index) for quantile_index in range(self.regression.n_quantiles)]
        quantile_coefficients = np.array([reg_res.result.coef_[0] for reg_res in regression_results])
        trace = go.Scatter(x=self.quantiles, y=quantile_coefficients, name=label)
        self.fig.add_trace(trace)

    def _layout(self, title: str):
        self._button()
        self.fig.update_layout(title=title)
        self.fig.update_xaxes(title_text='Quantile')
        self.fig.update_yaxes(title_text='Average quantile coefficient')

    def _button(self):
        self.fig.update_layout(dict(updatemenus=[
            dict(
                type="buttons",
                direction="left",
                buttons=list([
                    dict(
                        args=["visible", "legendonly"],
                        label="Deselect All",
                        method="restyle"
                    ),
                    dict(
                        args=["visible", True],
                        label="Select All",
                        method="restyle"
                    )
                ]),
                pad={"r": 10, "t": 10},
                showactive=False,
                x=1,
                xanchor="right",
                y=1.1,
                yanchor="top"
            ),
        ]
        ))