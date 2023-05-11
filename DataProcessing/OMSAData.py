import pandas as pd

from DataPreprocessing.DataIO import DataIO


class OMSAData:
    def __init__(self):
        self.omsa_data_file = DataIO.processed_omsa_data_file()
        self.dtypes = {'cbsa_fips': int, 'cbsa_name': str, 'prim_state': str, 'occ_code': str, 'occ_title': str, 'year': int,
                  'tot_emp': float,
                  'emp_prse': float, 'h_mean': float, 'a_mean': float, 'mean_prse': float, 'h_pct10': float,
                  'h_pct25': float, 'h_median': float,
                  'h_pct75': float, 'h_pct90': float, 'a_pct10': float, 'a_pct25': float, 'a_median': float,
                  'a_pct75': float, 'a_pct90': float}
        self.data = None

    def load(self) -> pd.DataFrame:
        self.data = pd.read_csv(self.omsa_data_file, dtype=self.dtypes)
        self.data = self.data.astype(dtype={'cbsa_fips': str})
        return self.data

