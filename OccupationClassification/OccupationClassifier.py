import pandas as pd
import numpy as np
from typing import Dict, List


class OccupationClassifier:
    def __init__(self, occ_codes: pd.DataFrame, occ_msa_data: pd.DataFrame, occ_characteristics_data: pd.DataFrame):
        self.occ_codes = occ_codes
        self.occ_msa_data = occ_msa_data
        self.occ_characteristics_data = occ_characteristics_data

    def get_occupations_grouped_by_digit_precision(self, n_digits: int) -> Dict[str, List[str]]:
        initial_digits_occ_code = self.occ_codes['occ_code'].str[:n_digits].unique()
        occupations_with_digit_precision = {}
        for d in initial_digits_occ_code:
            occupations_with_digit_precision[d] = self.occ_codes.loc[self.occ_codes['occ_code'].str.startswith(d)]['occ_code'].values.tolist()
        return occupations_with_digit_precision

    def get_occupations_grouped_by_characteristic(self, n_quantiles: int, characteristic_id: str) -> Dict[float, List[str]]:
        characteristic_info = self.occ_characteristics_data[self.occ_characteristics_data['Element ID'] == characteristic_id]
        quantiles = np.quantile(characteristic_info['Data Value'].values.flatten(), np.linspace(0, 1, n_quantiles + 1))
        occupations_grouped_by_education_level = {}
        for index_q_low in range(n_quantiles):
            index_q_high = index_q_low + 1
            q_min = quantiles[index_q_low]
            q_max = quantiles[index_q_high]
            occupations_grouped_by_education_level[index_q_low] = characteristic_info.loc[(q_min < characteristic_info['Data Value']) & (characteristic_info['Data Value'] <= q_max)]['O*NET-SOC Code'].values.tolist()

        return occupations_grouped_by_education_level

