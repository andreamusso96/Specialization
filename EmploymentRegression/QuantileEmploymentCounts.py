import pandas as pd
import numpy as np
from typing import Dict


# noinspection PyInterpreter
class QuantileEmploymentCounts:
    def __init__(self, occupation_network: pd.DataFrame, occupation_msa_data: pd.DataFrame):
        self.occupation_network = occupation_network
        self.msa_occ_employment_table = occupation_msa_data.pivot(index='area', columns='occ_code', values='tot_emp').fillna(value=0)
        self.occ_codes = list(self.msa_occ_employment_table.columns)

    def get_msa_occ_employment_tables(self, n_quantiles: int) -> Dict[int, pd.DataFrame]:
        msa_occ_tables = {}
        for index_q_low in range(n_quantiles):
            index_q_high = index_q_low + 1
            msa_occ_employment_table_in_quantile = self.get_msa_occ_employment_table_in_quantile(n_quantiles, index_q_low, index_q_high)
            msa_occ_tables[index_q_low] = msa_occ_employment_table_in_quantile
        return msa_occ_tables

    def get_msa_occ_employment_table_in_quantile(self, n_quantiles: int, index_q_low: int, index_q_high: int):
        employment_counts = []
        for occ in self.occ_codes:
            q_min, q_max = self._get_quantile_range(occ=occ, n_quantiles=n_quantiles, index_q_low=index_q_low, index_q_high=index_q_high)
            employment_counts.append(self.get_employment_counts_in_quantile_for_focal_occ(occ, q_min, q_max))
        employment_counts = pd.concat(employment_counts, axis=1)
        return employment_counts

    def get_employment_counts_in_quantile_for_focal_occ(self, focal_occ: str, q_min: float, q_max: float) -> pd.DataFrame:
        occ_codes_in_quantile = self.get_occ_codes_in_quantile_for_focal_occ(focal_occ, q_min, q_max)
        employment_counts = self.msa_occ_employment_table[occ_codes_in_quantile].sum(axis=1).to_frame()
        employment_counts.rename(columns={0: focal_occ}, inplace=True)
        return employment_counts

    def get_occ_codes_in_quantile_for_focal_occ(self, focal_occ: str, q_min: float, q_max: float) -> np.ndarray:
        similarity_to_focal_occ = self.occupation_network[focal_occ].values
        occ_indices_in_quantile = np.argwhere((q_min < similarity_to_focal_occ) & (similarity_to_focal_occ <= q_max))
        occ_codes_in_quantile = np.array(self.occupation_network.index)[occ_indices_in_quantile].flatten()
        return occ_codes_in_quantile

    def _get_quantile_range(self, occ: str, n_quantiles: int, index_q_low: int, index_q_high: int) -> tuple:
        quantiles = np.quantile(self.occupation_network[occ].values.flatten(), np.linspace(0, 1, n_quantiles + 1))
        q_min = quantiles[index_q_low]
        q_max = quantiles[index_q_high]
        return q_min, q_max

