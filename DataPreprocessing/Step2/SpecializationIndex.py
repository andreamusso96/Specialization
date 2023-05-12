import numpy as np
import pandas as pd
from tqdm import tqdm

from DataPreprocessing.Step2.OccupationNetwork import OccupationNetwork
from DataPreprocessing.Step2.OMSAData import OMSAData


class SpecializationIndex:
    def __init__(self, omsa_data: OMSAData, occ_network: OccupationNetwork):
        self.omsa_data = omsa_data
        self.occ_network = occ_network
        self.specialization_index_occs_cbsas = None
        self.specialization_index_cbsas = None

    def compute(self):
        self.specialization_index_occs_cbsas = self._compute_specialization_index_occs_cbsas()
        self.specialization_index_cbsas = self._compute_specialization_index_cbsas()
        return self.specialization_index_occs_cbsas, self.specialization_index_cbsas

    def _compute_specialization_index_cbsas(self) -> pd.DataFrame:
        cbsas = self.omsa_data.data['cbsa_fips'].unique()
        specialization_indices = []
        for cbsa_fips in cbsas:
            occ_probs_cbsa = self._occ_probs_cbsa(cbsa_fips=cbsa_fips)
            occ_specialization_cbsa = self.specialization_index_occs_cbsas[self.specialization_index_occs_cbsas['cbsa_fips'] == cbsa_fips].pivot(index='occ_code', columns='year', values='specialization_index')
            specialization_cbsa_year = (occ_probs_cbsa * occ_specialization_cbsa).sum(axis=0).to_frame(name='specialization_index').reset_index(names='year')
            specialization_cbsa_year['cbsa_fips'] = cbsa_fips
            specialization_indices.append(specialization_cbsa_year)

        specialization_indices_df = pd.concat(specialization_indices, ignore_index=True)
        specialization_indices_df.sort_values(by=['cbsa_fips', 'year'], inplace=True, ignore_index=True)
        return specialization_indices_df

    def _compute_specialization_index_occs_cbsas(self) -> pd.DataFrame:
        specialization_indices = []
        cbsas = self.omsa_data.data['cbsa_fips'].unique()
        occ_codes = self.occ_network.get_all_occ_codes()
        for cbsa_fips in tqdm(cbsas):
            occ_probs_cbsa = self._occ_probs_cbsa(cbsa_fips=cbsa_fips)
            years = occ_probs_cbsa.columns
            for occ_code in occ_codes:
                for year in years:
                    specialization_index_occ_cbsa = self._compute_specialization_index_occ_cbsa(occ_code=occ_code, occ_probs_cbsa=occ_probs_cbsa[year].values)
                    specialization_indices.append({'occ_code': occ_code, 'cbsa_fips': cbsa_fips, 'year': year, 'specialization_index': specialization_index_occ_cbsa})

        specialization_index_df = pd.DataFrame(specialization_indices)
        specialization_index_df.sort_values(by=['occ_code', 'cbsa_fips', 'year'], inplace=True, ignore_index=True)
        return specialization_index_df

    def _compute_specialization_index_occ_cbsa(self, occ_code: str, occ_probs_cbsa: np.ndarray):
        occ_dist_vector = self.occ_network.network.loc[occ_code].values
        expected_distance = occ_probs_cbsa.dot(occ_dist_vector)
        return expected_distance

    def _occ_probs_cbsa(self, cbsa_fips: str) -> pd.DataFrame:
        emp_counts_non_zero_occs = self._emp_counts_non_zero_occs(cbsa_fips=cbsa_fips)
        all_occs = self.occ_network.get_all_occ_codes_as_dataframe()
        emp_counts_all_occs = all_occs.merge(emp_counts_non_zero_occs.reset_index(), on='occ_code', how='left').fillna(0).set_index('occ_code')
        occ_probs = emp_counts_all_occs / emp_counts_all_occs.sum(axis=0)
        return occ_probs

    def _emp_counts_non_zero_occs(self, cbsa_fips: str) -> pd.DataFrame:
        emp_counts_cbsa = self.omsa_data.data.loc[self.omsa_data.data['cbsa_fips'] == cbsa_fips][['occ_code', 'tot_emp', 'year']]
        emp__occ_by_year = emp_counts_cbsa.pivot(index='occ_code', columns='year', values='tot_emp').fillna(method='bfill', axis=1).fillna(0)
        return emp__occ_by_year



