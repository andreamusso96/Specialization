from typing import Dict, Tuple

import pandas as pd

from DataPreprocessing.Step2.EmploymentByOccDistance import EmploymentByOccDistance
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
        table_wage__occ_loc_by_year = self.omsa_data.pivot(index=['occ_code', 'cbsa_fips'], columns='year', values='h_mean')
        table_wage_growth__occ_loc_by_year = table_wage__occ_loc_by_year.pct_change(axis=1, periods=1)
        return table_wage_growth__occ_loc_by_year

    def _emp_growth_within_dist_range(self) -> Dict[Tuple[float, float], pd.DataFrame]:
        emp_growth_within_dist_ranges = {}
        for dist_range in self.emp_by_occ_dist.emp_within_dist_ranges:
            emp_within_dist_range = self.emp_by_occ_dist.emp_within_dist_ranges[dist_range]
            emp_growth_within_dist_range = emp_within_dist_range.pct_change(axis=1, periods=1)
            emp_growth_within_dist_ranges[dist_range] = emp_growth_within_dist_range

        return emp_growth_within_dist_ranges



