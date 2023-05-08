from typing import Dict, Set
from config import DATA_PATH
import pandas as pd
import numpy as np


class CrosswalkFile:
    def __init__(self):
        self.file_name = 'cbsa_crosswalk.xls'
        self.file_path = f'{DATA_PATH}/OriginalData/OMSAData/{self.file_name}'
        self.data = None

    def load(self):
        self.data = pd.read_excel(io=self.file_path, header=2, skipfooter=4, sheet_name=0)
        return self.data


class ConsistentCBSACrosswalk:
    def __init__(self):
        self.file = CrosswalkFile()
        self.data = self.file.load()
        self.map_metro_division_to_cbsa = self._get_metro_division_to_cbsa_fips_map()
        self.map_cbsa_code_to_name = self._get_map_cbsa_code_to_name()
        self.cbsa_codes = self._get_cbsa_codes()
        self.different_codes = []

    def get_consistent_cbsa_code(self, code: int):
        if code in self.map_metro_division_to_cbsa:
            return self.map_metro_division_to_cbsa[code]
        else:
            if code not in self.cbsa_codes:
                return np.nan
            else:
                return code

    def get_consistent_cbsa_name(self, code: int) -> str:
        assert code in self.cbsa_codes
        return self.map_cbsa_code_to_name[code]

    def _get_metro_division_to_cbsa_fips_map(self) -> Dict[int, int]:
        _cbsa_code_and_metro_division_code = self.data[['CBSA Code', 'Metropolitan Division Code']].dropna(how='any')
        my_map = dict(zip(_cbsa_code_and_metro_division_code['Metropolitan Division Code'].astype(int),
                          _cbsa_code_and_metro_division_code['CBSA Code'].astype(int)))
        return my_map

    def _get_map_cbsa_code_to_name(self) -> Dict[int, str]:
        _cbsa_code_and_title = self.data[['CBSA Code', 'CBSA Title']].dropna(how='any')
        my_map = dict(zip(_cbsa_code_and_title['CBSA Code'].astype(int),
                          _cbsa_code_and_title['CBSA Title']))
        return my_map

    def _get_cbsa_codes(self) -> Set[int]:
        return set(self.data['CBSA Code'].unique().astype(int).tolist())


if __name__ == '__main__':
    f = CrosswalkFile()
    f.load()