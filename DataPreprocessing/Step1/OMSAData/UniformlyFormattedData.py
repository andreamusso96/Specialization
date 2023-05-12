from typing import Tuple

import pandas as pd
import numpy as np
from tqdm import tqdm

from DataPreprocessing.Step1.OMSAData.RawData import RawDataOMSA
from DataPreprocessing.Step1.SOC.ConsistentCrosswalk import ConsistentSOCCrosswalk, CrosswalkData
from DataPreprocessing.Step1.CBSA.CBSACrosswalk import ConsistentCBSACrosswalk


class YearSpecificDataAttributes:
    @staticmethod
    def get_high_wage_values(year: int) -> Tuple[int, int]:
        if 2005 <= year <= 2007:
            hourly_wage = 70
            annual_wage = 145_600
        elif 2008 <= year <= 2010:
            hourly_wage = 80
            annual_wage = 166_400
        elif 2011 <= year <= 2015:
            hourly_wage = 90
            annual_wage = 187_200
        elif 2016 <= year:
            hourly_wage = 100
            annual_wage = 208_000
        else:
            raise ValueError(f'Year {year} not supported.')

        return hourly_wage, annual_wage


class UniformlyFormattedDataGeneralYear:
    def __init__(self, raw_data: RawDataOMSA, occ_crosswalk: ConsistentSOCCrosswalk, cbsa_crosswalk: ConsistentCBSACrosswalk):
        self.year = raw_data.year
        self.data = raw_data.data.values()
        self.raw_data = raw_data
        self.occ_crosswalk = occ_crosswalk
        self.cbsa_crosswalk = cbsa_crosswalk
        self.soc_version = self.occ_crosswalk.get_soc_version(year=self.year)
        self._column_names = ['cbsa_fips', 'cbsa_name', 'prim_state', 'occ_code', 'occ_title', 'year', 'tot_emp',
                              'emp_prse', 'h_mean', 'a_mean', 'mean_prse', 'h_pct10', 'h_pct25', 'h_median', 'h_pct75', 'h_pct90',
                              'a_pct10', 'a_pct25', 'a_median', 'a_pct75', 'a_pct90']

    def load(self):
        self._concat_data()
        self._set_year()
        self._set_column_names()
        self._drop_unnecessary_columns()
        self._set_prim_state()
        self._drop_rows_with_missing_values()
        self._drop_non_detailed_occupations()
        self._drop_occ_codes_not_in_crosswalk()
        self._set_high_wage_values()
        self._set_column_types()
        self._set_consistent_soc_codes()
        self._set_consistent_soc_titles()
        self._group_duplicate_consistent_soc_codes()
        self._set_consistent_cbsa_codes()
        self._set_consistent_cbsa_names()
        self._group_duplicate_consistent_cbsa_codes()
        self._cbsa_fips_to_string()
        self._sort_data()

    def _concat_data(self):
        self.data = pd.concat([d for d in self.data], axis=0, join='outer', ignore_index=True)

    def _set_year(self):
        self.data['year'] = self.year

    def _set_column_names(self):
        rename_columns = {old_name: self._get_new_column_name(col=old_name) for old_name in self.data.columns}
        self.data.rename(columns=rename_columns, inplace=True)

    def _set_prim_state(self):
        # In the base case we do nothing. For 2019 (see UniformlyFormattedData2019) we compute the primary state from the cbsa_name
        pass

    def _get_new_column_name(self, col: str) -> str:
        cname = col.lower()
        if cname in self._column_names:
            return cname
        else:
            return self.process_special_column_names(cname=cname)

    def _drop_unnecessary_columns(self):
        self.data.drop(columns=[col_name for col_name in self.data.columns if col_name.startswith('drop_')],
                       inplace=True)

    def _drop_rows_with_missing_values(self):
        self.data.where(cond=(self.data != '***'), other=np.nan, inplace=True)
        self.data.where(cond=(self.data != '**'), other=np.nan, inplace=True)
        self.data.where(cond=(self.data != '*'), other=np.nan, inplace=True)
        self.data.dropna(axis=0, how='any', inplace=True, ignore_index=True)

    def _drop_non_detailed_occupations(self):
        detailed_occupations = self.data['occ_code'].apply(self._is_detailed_occupation)
        self.data.drop(index=self.data[~detailed_occupations].index, inplace=True)

    def _drop_occ_codes_not_in_crosswalk(self):
        def occ_code_in_crosswalk(code: str) -> bool:
            return self.occ_crosswalk.soc_code_in_crosswalk(soc_code=code, soc_version=self.soc_version)
        codes_in_crosswalk = self.data['occ_code'].apply(occ_code_in_crosswalk)
        self.data.drop(index=self.data[~codes_in_crosswalk].index, inplace=True)

    def _set_high_wage_values(self):
        hourly, annual = YearSpecificDataAttributes.get_high_wage_values(year=self.year)
        for col in self.data.columns:
            if col.startswith('h_'):
                self.data[col] = np.where(self.data[col] == '#', hourly, self.data[col])
            elif col.startswith('a_'):
                self.data[col] = np.where(self.data[col] == '#', annual, self.data[col])

    def _set_column_types(self):
        self.data['cbsa_fips'] = self.data['cbsa_fips'].astype(int)

    def _set_consistent_soc_codes(self):
        def code_to_consistent_code(code: str) -> str:
            return self.occ_crosswalk.get_consistent_soc_code(soc_code=code, soc_version=self.soc_version)
        self.data['occ_code'] = self.data['occ_code'].apply(code_to_consistent_code)

    def _set_consistent_soc_titles(self):
        def code_to_consistent_title(code: str) -> str:
            return self.occ_crosswalk.get_consistent_soc_title(consistent_soc_code=code)
        self.data['occ_title'] = self.data['occ_code'].apply(code_to_consistent_title)

    def _set_consistent_cbsa_codes(self):
        self.data['cbsa_fips'] = self.data['cbsa_fips'].astype(int).apply(self.cbsa_crosswalk.get_consistent_cbsa_code)
        self.data.dropna(subset=['cbsa_fips'], inplace=True, ignore_index=True)

    def _set_consistent_cbsa_names(self):
        self.data['cbsa_name'] = self.data['cbsa_fips'].astype(int).apply(self.cbsa_crosswalk.get_consistent_cbsa_name)

    def _group_duplicate_consistent_soc_codes(self):
        # Group by consistent soc code, cbsa_fips, and year
        grouped_data = self.data.groupby(['occ_code', 'cbsa_fips'])
        self.data = self._aggregate_grouped_data(grouped_data=grouped_data)

    def _group_duplicate_consistent_cbsa_codes(self):
        grouped_data = self.data.groupby(['cbsa_fips', 'occ_code'])
        self.data = self._aggregate_grouped_data(grouped_data=grouped_data)

    def _aggregate_grouped_data(self, grouped_data):
        # Aggregation invariant columns
        cols_invariant = ['cbsa_name', 'prim_state', 'occ_title', 'year']

        def identity_map(x): return x[x.index[0]]

        agg_invariant_cols = {col: identity_map for col in cols_invariant}

        # Aggregation sum columns
        def sum_(x): return np.sum(x.astype(float))
        agg_sum_cols = {'tot_emp': sum_}

        # Aggregation weighted mean column
        cols_weighted_mean = ['emp_prse', 'h_mean', 'a_mean', 'mean_prse', 'h_pct10', 'h_pct25', 'h_median', 'h_pct75',
                              'h_pct90', 'a_pct10', 'a_pct25', 'a_median', 'a_pct75', 'a_pct90']

        def weighted_avg(x): return np.average(x.astype(float), weights=self.data.loc[x.index, 'tot_emp'].astype(float))

        agg_weighted_mean_cols = {col: weighted_avg for col in cols_weighted_mean}

        # Aggregate data
        agg_specs = {**agg_invariant_cols, **agg_sum_cols, **agg_weighted_mean_cols}
        aggregated_date = grouped_data.agg(agg_specs)
        return aggregated_date.reset_index()

    def _cbsa_fips_to_string(self):
        self.data['cbsa_fips'] = self.data['cbsa_fips'].astype(int).astype(str)

    def _sort_data(self):
        self.data.sort_values(by=['cbsa_fips', 'occ_code', 'year'], inplace=True)

    @staticmethod
    def process_special_column_names(cname: str) -> str:
        if cname == 'area_name' or cname == 'area_title':
            return 'cbsa_name'
        elif cname == 'area':
            return 'cbsa_fips'
        else:
            return f'drop_{cname}'

    @staticmethod
    def _is_detailed_occupation(code: str) -> bool:
        return not code[-2:] == '00'


class UniformlyFormattedData2019(UniformlyFormattedDataGeneralYear):
    def __init__(self, raw_data: RawDataOMSA, occ_crosswalk: ConsistentSOCCrosswalk, cbsa_crosswalk: ConsistentCBSACrosswalk):
        super().__init__(raw_data=raw_data, occ_crosswalk=occ_crosswalk, cbsa_crosswalk=cbsa_crosswalk)

    def _set_prim_state(self):
        cbsa_names = self.data['cbsa_name'].values
        prim_states = [self._extract_prim_state_from_cbsa_name(cbsa_name=cbsa_name) for cbsa_name in cbsa_names]
        self.data['prim_state'] = prim_states

    @staticmethod
    def _extract_prim_state_from_cbsa_name(cbsa_name: str):
        states = cbsa_name.split(',')[1]
        prim_state = states.split('-')[0]
        return prim_state.strip()


class UniformlyFormattedData:
    def __init__(self, year_start: int = 2005, year_end: int = 2021):
        self.years = list(range(year_start, year_end + 1))
        self.data = None

    def load(self) -> pd.DataFrame:
        occ_crosswalk = self._load_occ_crosswalk()
        cbsa_crosswalk = self._load_cbsa_crosswalk()
        formatted_data = []
        for year in tqdm(self.years):
            raw_data = RawDataOMSA(year=year)
            raw_data.load()
            formatted_data.append(self._format_data(raw_data=raw_data, occ_crosswalk=occ_crosswalk, cbsa_crosswalk=cbsa_crosswalk))

        self.data = pd.concat([f.data for f in formatted_data], axis=0, join='outer', ignore_index=True)
        return self.data

    @staticmethod
    def _format_data(raw_data: RawDataOMSA, occ_crosswalk: ConsistentSOCCrosswalk,
                     cbsa_crosswalk: ConsistentCBSACrosswalk):
        if raw_data.year == 2019:
            formatted_data_year = UniformlyFormattedData2019(raw_data=raw_data, occ_crosswalk=occ_crosswalk,
                                                             cbsa_crosswalk=cbsa_crosswalk)
        else:
            formatted_data_year = UniformlyFormattedDataGeneralYear(raw_data=raw_data, occ_crosswalk=occ_crosswalk, cbsa_crosswalk=cbsa_crosswalk)

        formatted_data_year.load()
        return formatted_data_year

    @staticmethod
    def _load_occ_crosswalk():
        crosswalk_data = CrosswalkData()
        crosswalk_data.load()
        occ_crosswalk = ConsistentSOCCrosswalk(crosswalk_data)
        return occ_crosswalk

    @staticmethod
    def _load_cbsa_crosswalk():
        cbsa_crosswalk = ConsistentCBSACrosswalk()
        return cbsa_crosswalk
