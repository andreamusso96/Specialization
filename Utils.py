import pandas as pd

class Data:
    def __init__(self, occ_codes: pd.DataFrame, occ_network: pd.DataFrame, occ_msa_data: pd.DataFrame, occ_char_data: pd.DataFrame):
        self.occ_codes = occ_codes
        self.occ_network = occ_network
        self.occ_msa_data = occ_msa_data
        self.occ_char_data = occ_char_data