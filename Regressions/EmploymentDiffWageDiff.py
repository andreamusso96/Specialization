from typing import Dict, Tuple

import pandas as pd
import numpy as np

from DataPreprocessing.Step3.EmploymentByOccDistance import EmploymentByOccDistance
from Regressions.Regression import Regression


class EmploymentDiffWageDiff(Regression):
    def __init__(self, omsa_data: pd.DataFrame, emp_by_occ_dist: EmploymentByOccDistance):
        super().__init__()
        self.omsa_data = omsa_data
        self.emp_by_occ_dist = emp_by_occ_dist

    def run(self):
        table_wage_growth = self._table_wage_growth__occ_loc_by_year()
        emp_growth_within_dist_ranges = self._emp_growth_within_dist_range()

        result = {}
        for dist_range in emp_growth_within_dist_ranges:
            table_emp_growth = emp_growth_within_dist_ranges[dist_range]
            result[dist_range] = self._run_regression(x=table_emp_growth.values.flatten(), y=table_wage_growth.values.flatten())
            
        return result

    def _table_wage_growth__occ_loc_by_year(self) -> pd.DataFrame:
        table_wage__occ_loc_by_year = self.omsa_data.pivot(index=['occ_code', 'cbsa_fips'], columns='year', values='h_mean').fillna(method='bfill', axis=1).fillna(method='ffill', axis=1)
        table_wage_growth__occ_loc_by_year = table_wage__occ_loc_by_year.pct_change(axis=1, periods=1)
        table_wage_growth__occ_loc_by_year.drop(columns=table_wage_growth__occ_loc_by_year.columns[0], inplace=True)
        return table_wage_growth__occ_loc_by_year

    def _emp_growth_within_dist_range(self) -> Dict[Tuple[float, float], pd.DataFrame]:
        emp_growth_within_dist_ranges = {}
        for dist_range in self.emp_by_occ_dist.data:
            emp_within_dist_range = self.emp_by_occ_dist.data[dist_range]
            emp_within_dist_range.where(emp_within_dist_range > 0, 1, inplace=True)
            emp_growth_within_dist_range = emp_within_dist_range.pct_change(axis=1, periods=1)
            emp_growth_within_dist_range.drop(columns=emp_within_dist_range.columns[0], inplace=True)
            emp_growth_within_dist_ranges[dist_range] = emp_growth_within_dist_range

        return emp_growth_within_dist_ranges


if __name__ == '__main__':
    from DataPreprocessing.Step2.OMSAData import OMSAData
    from DataPreprocessing.Step3.EmploymentByOccDistance import EmploymentByOccDistance
    from DataPreprocessing.Step3.SpecializationIndex import SpecializationIndex
    omsa_data = OMSAData()
    omsa_data.load()
    emp_by_occ_dist = EmploymentByOccDistance(quantiles=np.array([0, 0.25, 0.5, 0.75, 1]))
    emp_by_occ_dist.load()
    res = EmploymentDiffWageDiff(omsa_data=omsa_data.data, emp_by_occ_dist=emp_by_occ_dist).run()



