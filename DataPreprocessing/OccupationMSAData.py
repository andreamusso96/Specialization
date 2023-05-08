from DataIO import OriginalDataIO, ProcessedDataIO
import pandas as pd
import numpy as np


class OccupationMSADataPipeline:
    @staticmethod
    def process_occupation_msa_data():
        # Load _data
        occupation_msa_data = OriginalDataIO.load_occupation_msa_data()

        # Clean _data
        cleaner = Cleaner(occupation_msa_data=occupation_msa_data)
        occupation_msa_data = cleaner.clean_data()

        # Save _data
        ProcessedDataIO.save_occupation_msa_data(occupation_msa_data=occupation_msa_data)
        return occupation_msa_data


class Cleaner:
    # Here we created a class to avoid copying this large dataset multiple times
    def __init__(self, occupation_msa_data: pd.DataFrame):
        self.data = occupation_msa_data

    def clean_data(self):
        self._remove_unnecessary_columns()
        self._remove_rows_with_missing_values()
        self._remove_hashtag_for_high_paying_occupations()
        self._remove_occupations_not_present_in_o_star_net_data()
        return self.data

    def _remove_unnecessary_columns(self):
        self.data.drop(columns=['naics', 'naics_title', 'i_group', 'own_code', 'o_group', 'emp_prse', 'pct_total', 'h_mean', 'h_pct10', 'h_pct25', 'h_median', 'h_pct75', 'h_pct90', 'annual', 'hourly'], inplace=True)

    def _remove_rows_with_missing_values(self):
        self.data.replace(to_replace='*', value=np.nan, inplace=True)
        self.data.replace(to_replace='**', value=np.nan, inplace=True)
        self.data.dropna(axis=0, how='any', inplace=True)

    def _remove_hashtag_for_high_paying_occupations(self):
        for col in self.data.columns:
            if col.startswith('h_'):
                self.data[col] = self.data[col].apply(lambda x: self._convert_hashtags(x=x, float_val=150)).copy()
            elif col.startswith('a_'):
                self.data[col] = self.data[col].apply(lambda x: self._convert_hashtags(x=x, float_val=250000)).copy()

    def _remove_occupations_not_present_in_o_star_net_data(self):
        occ_codes_in_both_datasets = ProcessedDataIO.load_occupations_codes_present_in_both_datasets()
        self.data = self.data[self.data['occ_code'].isin(occ_codes_in_both_datasets['occ_code'].values)].copy()

    @staticmethod
    def _convert_hashtags(x: str or float, float_val: float) -> float:
        if x == '#':
            return float_val
        else:
            return x


if __name__ == '__main__':
    OccupationMSADataPipeline.process_occupation_msa_data()
