from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

from DataPreprocessing.Step2.OccupationNetwork import OccupationNetwork
from DataPreprocessing.Step2.OMSAData import OMSAData


class EmploymentByOccDistanceCalculator:
    def __init__(self, omsa_data: OMSAData, occ_network: OccupationNetwork, quantiles: np.ndarray):
        self.omsa_data = omsa_data
        self.occ_network = occ_network
        self.quantiles = quantiles

    def compute(self) -> Dict[Tuple[float, float], pd.DataFrame]:
        self.emp_within_dist_ranges = self._emp_within_dist_ranges(occ_codes=self.occ_network.get_all_occ_codes())
        return self.emp_within_dist_ranges

    def _emp_within_dist_ranges(self, occ_codes: List[str]) -> Dict[Tuple[float, float], pd.DataFrame]:
        emp_within_dist_ranges = {}
        distance_ranges = self.occ_network.get_distance_distribution_quantiles(quantiles=self.quantiles)
        for i in range(len(distance_ranges) - 1):
            a = distance_ranges[i]
            b = distance_ranges[i+1]
            emp_within_dist_range = self._table_emp_within_dist_range__occ_loc_by_year(occ_codes=occ_codes, a=a, b=b)
            emp_within_dist_ranges[(self.quantiles[i], self.quantiles[i+1])] = emp_within_dist_range

        return emp_within_dist_ranges

    def _table_emp_within_dist_range__occ_loc_by_year(self, occ_codes: List[str], a: float, b: float) -> pd.DataFrame:
        emp_within_dist_range = []
        for occ_code in tqdm(occ_codes):
            emp_within_dist_range_from_occ = self._table_emp_within_dist_range_from_occ__loc_by_year(occ_code=occ_code, a=a, b=b)
            emp_within_dist_range_from_occ.index = pd.MultiIndex.from_product(iterables=[[occ_code], emp_within_dist_range_from_occ.index])
            emp_within_dist_range.append(emp_within_dist_range_from_occ)

        emp_within_dist_range = pd.concat(emp_within_dist_range)
        return emp_within_dist_range

    def _table_emp_within_dist_range_from_occ__loc_by_year(self, occ_code: str, a: float, b: float) -> pd.DataFrame:
        occs_within_dist_range_from_occ = self.occ_network.get_occ_codes_within_distance_interval(code=occ_code, a=a, b=b)
        emp_data = self.omsa_data.data.loc[self.omsa_data.data['occ_code'].isin(occs_within_dist_range_from_occ)]
        grouped_emp_data = emp_data.groupby(by=['cbsa_fips', 'year']).agg({'tot_emp': 'sum'}).reset_index()
        emp_within_dist_range = grouped_emp_data.pivot(columns='year', index='cbsa_fips', values='tot_emp').fillna(method='bfill', axis=1).fillna(0)
        return emp_within_dist_range