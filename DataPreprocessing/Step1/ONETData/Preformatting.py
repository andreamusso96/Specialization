from typing import List


import numpy as np
import pandas as pd


class PreFormatter:
    def __init__(self, data: pd.DataFrame, column_names: List[str], scales: List[str]):
        self.data = data
        self._column_names = column_names
        self._scales = scales

    def format(self):
        self._drop_unnecessary_columns()
        self._drop_unnecessary_scales()
        self._group_data()
        return self.data

    def _drop_unnecessary_columns(self):
        self.data.drop(columns=[col for col in self.data.columns if col not in self._column_names], inplace=True)

    def _drop_unnecessary_scales(self):
        self.data = self.data.loc[self.data['Scale ID'].isin(self._scales)].copy()

    def _group_data(self):
        pass


class EducationDataPreFormatter(PreFormatter):
    def __init__(self, education_data: pd.DataFrame, column_names: List[str]):
        super().__init__(data=education_data,
                         column_names=column_names,
                         scales=['RL', 'RW', 'PT', 'OJ'])

    def _group_data(self):
        grouped_data = self.data.groupby(by=['O*NET-SOC Code', 'Element ID', 'Scale ID'])
        # Aggregation invariant columns
        cols_invariant = ['Title', 'Element Name', 'Scale Name']

        def identity_map(x): return x[x.index[0]]

        agg_invariant_cols = {col: identity_map for col in cols_invariant}

        # Aggregation sum columns
        def expectation(x): return np.average(self.data.loc[x.index, 'Category'].astype(float), weights=x.astype(float))
        agg_expectation = {'Data Value': expectation}

        # Aggregate data
        agg_specs = {**agg_invariant_cols, **agg_expectation}
        aggregated_data = grouped_data.agg(agg_specs)
        self.data = aggregated_data.reset_index()

