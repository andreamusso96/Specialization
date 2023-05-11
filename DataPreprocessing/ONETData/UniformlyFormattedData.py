from typing import List, Tuple

import pandas as pd
import numpy as np

from DataPreprocessing.ONETData.Preformatting import PreFormatter, EducationDataPreFormatter
from DataPreprocessing.ONETData.RawData import RawDataONET
from DataPreprocessing.SOC.ConsistentCrosswalk import ConsistentSOCCrosswalk
from DataPreprocessing.SOC.Utils import SOCVersion


class MinMaxScaler:
    def __init__(self, scales_data: pd.DataFrame):
        self.scales_data = scales_data

    def scale(self, data):
        def scale(x):
            scale_min, scale_max = self.get_scale_min_and_max(scale_id=x['Scale ID'])
            val = float(x['Data Value'])
            return self._min_max_scale(val=val, scale_min=scale_min, scale_max=scale_max)

        data['Data Value'] = data.apply(scale, axis=1)
        return data

    def get_scale_min_and_max(self, scale_id: str) -> Tuple[float, float]:
        scale_info = self.scales_data[self.scales_data['Scale ID'] == scale_id]
        scale_name = scale_info['Scale Name'].values[0]
        if self._is_category_scale(scale_name=scale_name):
            categories = scale_name.split('(')[1].replace(')', '').replace('Categories', '').split('-')
            scale_min = float(categories[0])
            scale_max = float(categories[1])
        else:
            scale_max = scale_info['Maximum'].values[0]
            scale_min = scale_info['Minimum'].values[0]

        return scale_min, scale_max

    @staticmethod
    def _is_category_scale(scale_name: str) -> bool:
        if 'Categories' in scale_name:
            return True
        else:
            return False

    @staticmethod
    def _min_max_scale(val: float, scale_min: float, scale_max: float) -> float:
        return (val - scale_min) / (scale_max - scale_min)


class UniformlyFormattedData:
    def __init__(self, raw_data: RawDataONET, occ_crosswalk: ConsistentSOCCrosswalk):
        self.year = raw_data.year
        self.raw_data = raw_data
        self.occ_crosswalk = occ_crosswalk
        self.scaler = MinMaxScaler(scales_data=self.raw_data.get_scales_data())
        self._column_names = ['O*NET-SOC Code', 'Title', 'Element ID', 'Element Name', 'Scale ID', 'Scale Name',
                           'Data Value']
        self.data = None

    def load(self):
        self._preformat()
        self._concat_data()
        self._set_occ_codes_to_reduced_form()
        self._drop_occ_codes_not_in_crosswalk()
        self._set_consistent_occ_code()
        self._set_consistent_occ_title()
        self._group_duplicate_consistent_soc_codes()
        self._rescale()
        self._drop_scale_columns()
        self._sort()

    def _preformat(self):
        education_data = EducationDataPreFormatter(education_data=self.raw_data.get_education_data(), column_names=self._column_names + ['Category']).format()
        other_data_raw = [self.raw_data.get_abilities_data(), self.raw_data.get_knowledge_data(), self.raw_data.get_work_activities_data(), self.raw_data.get_skills_data()]
        other_data = [PreFormatter(data=data, column_names=self._column_names, scales=['IM']).format() for data in other_data_raw]
        self.data = other_data + [education_data]

    def _concat_data(self):
        self.data = pd.concat(self.data)

    def _set_occ_codes_to_reduced_form(self):
        def reduced_form_code(x): return str(x.split('.')[0])
        self.data['O*NET-SOC Code'] = self.data['O*NET-SOC Code'].apply(reduced_form_code)

    def _drop_occ_codes_not_in_crosswalk(self):
        def occ_code_in_crosswalk(code: str) -> bool:
            return self.occ_crosswalk.soc_code_in_crosswalk(soc_code=code, soc_version=SOCVersion.soc_2018)
        codes_in_crosswalk = self.data['O*NET-SOC Code'].apply(occ_code_in_crosswalk)
        self.data.drop(index=self.data[~codes_in_crosswalk].index, inplace=True)

    def _set_consistent_occ_code(self):
        def onet_code_to_consistent_code(code: str) -> str:
            return self.occ_crosswalk.get_consistent_soc_code(soc_code=code, soc_version=SOCVersion.soc_2018)
        self.data['O*NET-SOC Code'] = self.data['O*NET-SOC Code'].apply(onet_code_to_consistent_code)

    def _set_consistent_occ_title(self):
        def consistent_code_to_consistent_title(code: str) -> str:
            return self.occ_crosswalk.get_consistent_soc_title(consistent_soc_code=code)

        self.data['Title'] = self.data['O*NET-SOC Code'].apply(consistent_code_to_consistent_title)

    def _group_duplicate_consistent_soc_codes(self):
        # Group by consistent soc code, cbsa_fips, and year
        grouped_data = self.data.groupby(['O*NET-SOC Code', 'Element ID'])
        self.data = self._aggregate_grouped_data(grouped_data=grouped_data)

    def _aggregate_grouped_data(self, grouped_data):
        # Aggregation invariant columns
        cols_invariant = ['Title', 'Element Name', 'Scale ID', 'Scale Name']
        def identity_map(x): return x[x.index[0]]
        agg_invariant_cols = {col: identity_map for col in cols_invariant}

        # Aggregation sum columns
        def mean_(x): return np.mean(x.astype(float))
        agg_mean_cols = {'Data Value': mean_}

        # Aggregate data
        agg_specs = {**agg_invariant_cols, **agg_mean_cols}
        aggregated_date = grouped_data.agg(agg_specs)
        return aggregated_date.reset_index()

    def _rescale(self):
        self.data = self.scaler.scale(data=self.data)

    def _drop_scale_columns(self):
        self.data.drop(columns=['Scale ID', 'Scale Name'], inplace=True)

    def _sort(self):
        self.data.sort_values(by=['O*NET-SOC Code', 'Element ID'], inplace=True)




if __name__ == '__main__':
    from DataPreprocessing.SOC.CrosswalkData import CrosswalkData
    r = RawDataONET(year=2019)
    r.load()
    crosswalk_data = CrosswalkData()
    crosswalk_data.load()
    occ_crosswalk = ConsistentSOCCrosswalk(crosswalk_data)
    f = UniformlyFormattedData(raw_data=r, occ_crosswalk=occ_crosswalk)
    f.load()
