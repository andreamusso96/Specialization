from typing import List, Set, Tuple

import numpy as np
import pandas as pd

from DataPreprocessing.Step1.SOC.CrosswalkData import CrosswalkData
from DataPreprocessing.Step1.SOC.Utils import Columns, SOCVersion


class ConsistentSOCCodes:
    def __init__(self, crosswalk_data: CrosswalkData):
        self._crosswalk_data = crosswalk_data
        self.codes = self._get_consistent_soc_codes()

    def _get_consistent_soc_codes(self) -> Set[str]:
        """

        Consistent SOC codes are built as follows. Occupations that are split in the 2000-2010 _crosswalk_data are
        given the 2000 SOC code. Occupations that are split in the 2010-2018 _crosswalk_data are given the 2010 SOC code.
        All other occupations are given the 2018 SOC code.

        :return: Set of consistent SOC codes
        """

        # Get all codes that are split in the 2000-2010 _crosswalk_data
        crosswalk_2000_2010 = self._crosswalk_data.soc2000_to_soc2010.data
        codes_split_in_2000_2010_crosswalk = self._get_elements_appearing_twice_or_more(
            crosswalk_2000_2010[Columns.soc_2000_code].values)
        indices_rows_to_keep = ~crosswalk_2000_2010[Columns.soc_2000_code].isin(codes_split_in_2000_2010_crosswalk)
        all_other_codes_in_2000_2010_crosswalk = self._crosswalk_data.soc2000_to_soc2010.data[indices_rows_to_keep][
            Columns.soc_2010_code].values

        # Get all codes that are split in the 2010-2018 _crosswalk_data
        crosswalk_2010_2018 = self._crosswalk_data.soc2010_to_soc2018.data
        codes_split_in_2010_2018_crosswalk = self._get_elements_appearing_twice_or_more(
            crosswalk_2010_2018[Columns.soc_2010_code].values)
        indices_rows_to_keep = ~crosswalk_2010_2018[Columns.soc_2010_code].isin(
            list(codes_split_in_2010_2018_crosswalk) + list(codes_split_in_2000_2010_crosswalk))
        all_other_codes_in_2010_2018_crosswalk = list(
            self._crosswalk_data.soc2010_to_soc2018.data[indices_rows_to_keep][Columns.soc_2018_code].values)

        # Get consistent codes
        codes_2000_consistent = codes_split_in_2000_2010_crosswalk
        codes_2010_consistent = list(set(codes_split_in_2010_2018_crosswalk).intersection(set(all_other_codes_in_2000_2010_crosswalk)))
        codes_2018_consistent = all_other_codes_in_2010_2018_crosswalk
        consistent_codes = set(codes_2000_consistent + codes_2010_consistent + codes_2018_consistent)
        return consistent_codes

    @staticmethod
    def _get_elements_appearing_twice_or_more(a: np.ndarray) -> List[str]:
        unique, counts = np.unique(a, return_counts=True)
        return list(unique[counts >= 2])


class ConsistentSOCCrosswalkMapper:
    def __init__(self, consistent_soc_crosswalk_data: pd.DataFrame):
        self._data = consistent_soc_crosswalk_data
        self._code_to_consistent_code_map = self._get_code_to_consistent_code_map()
        self._consistent_code_to_title_map = self._get_consistent_code_to_title_map()

    def get_consistent_soc_code(self, soc_code: str, soc_version: str) -> str:
        return self._code_to_consistent_code_map[soc_version][soc_code]

    def get_consistent_soc_title(self, consistent_soc_code: str) -> str:
        return self._consistent_code_to_title_map[consistent_soc_code]

    def soc_code_in_crosswalk(self, soc_code: str, soc_version: str) -> bool:
        return soc_code in self._code_to_consistent_code_map[soc_version]

    def _get_code_to_consistent_code_map(self):
        my_map = dict.fromkeys([SOCVersion.soc_2000, SOCVersion.soc_2010, SOCVersion.soc_2018, SOCVersion.hybrid_2010_2011, SOCVersion.hybrid_2019_2020])
        for soc_version in my_map:
            codes = self._get_soc_codes(soc_version=soc_version)
            consistent_codes = self._data[Columns.consistent_soc_code].values
            my_map[soc_version] = dict(zip(codes, consistent_codes))
        return my_map

    def _get_consistent_code_to_title_map(self):
        my_map = dict(zip(self._data[Columns.consistent_soc_code].values, self._data[Columns.consistent_soc_title].values))
        return my_map

    def _get_soc_codes(self, soc_version: str) -> np.ndarray:
        col_name = [c for c in self._data.columns if soc_version in c and 'code' in c][0]
        return self._data[col_name].values


class ConsistentSOCCrosswalk:
    def __init__(self, crosswalk_data: CrosswalkData):
        self._crosswalk_data = crosswalk_data
        self._consistent_soc_codes = ConsistentSOCCodes(crosswalk_data=crosswalk_data)
        self.data = self._get_consistent_soc_crosswalk()
        self.mapper = ConsistentSOCCrosswalkMapper(consistent_soc_crosswalk_data=self.data)

    def get_consistent_soc_code(self, soc_code: str, soc_version: str) -> str:
        return self.mapper.get_consistent_soc_code(soc_code=soc_code, soc_version=soc_version)

    def get_consistent_soc_title(self, consistent_soc_code: str) -> str:
        return self.mapper.get_consistent_soc_title(consistent_soc_code=consistent_soc_code)

    def soc_code_in_crosswalk(self, soc_code: str, soc_version: str) -> bool:
        return self.mapper.soc_code_in_crosswalk(soc_code=soc_code, soc_version=soc_version)

    def _get_consistent_soc_crosswalk(self) -> pd.DataFrame:
        consistent_soc_crosswalk_data = self._crosswalk_data.data.copy()
        consistent_soc_crosswalk_data[Columns.consistent_soc_code] = None
        consistent_soc_crosswalk_data[Columns.consistent_soc_title] = None

        for index, row in self._crosswalk_data.data.iterrows():
            consistent_code, consistent_title = self._extract_consistent_code_and_title_from_row(row)
            consistent_soc_crosswalk_data.at[index, Columns.consistent_soc_code] = consistent_code
            consistent_soc_crosswalk_data.at[index, Columns.consistent_soc_title] = consistent_title

        return consistent_soc_crosswalk_data

    def _extract_consistent_code_and_title_from_row(self, row) -> Tuple[str, str]:
        """

        For each row, the corresponding consistent SOC code is the SOC code in the row that is also in the set of
        consistent SOC codes. Typically, there is only one such code. However, in a few exceptional cases, there are
        two such codes. In these cases, the choice of code is described in the docstring of the method
        _select_consistent_soc_code.

        :param row: A row of the _crosswalk_data dataframe between the various versions of the SOC
        :return: the consistent SOC code and its title

        """
        codes = [row[Columns.soc_2000_code], row[Columns.soc_2010_code], row[Columns.soc_2018_code],
                 row[Columns.hybrid_2010_2011_code], row[Columns.hybrid_2019_2020_code]]
        titles = [row[Columns.soc_2000_title], row[Columns.soc_2010_title], row[Columns.soc_2018_title],
                  row[Columns.hybrid_2010_2011_title], row[Columns.hybrid_2019_2020_title]]
        code_candidates = set(codes).intersection(self._consistent_soc_codes.codes)
        consistent_code = self._select_consistent_soc_code(code_candidates, codes)
        consistent_title = titles[codes.index(consistent_code)]

        return consistent_code, consistent_title

    @staticmethod
    def _select_consistent_soc_code(code_candidates: Set[str], codes: List[str]) -> str:
        """
        Selects the consistent SOC code from the set of candidates. If there is only one candidate, it is returned.
        This is the typical behavior. However, in a limited number of cases, there are two candidates. This occurs
        when an occupation o1 in 2000 is split into two occupations o2,o3 in 2010. o3 is a new occupation, but o2
        is an occupation that already existed in 2000.

        For example, occupation 13-1079 (Human Resources, Training, and Labor Relations Specialists) in 2000
        was split into occupation 13-1071 (Employment, Recruitment, and Placement Specialists)
        and occupation 13-1075 (Labor Relations Specialists) in 2010. However, occupation 13-1071
        already existed in 2000.

        In this case, o1, o2 and o3 are all mapped to the o2 code.
        This is equivalent to merging o1 and o2 in 2000 and then keeping o2 as unique id of the tree that descends
        from splits of o1 and o2 later on.

        :param code_candidates: Set of candidate codes
        :param codes: List of codes of the various SOC versions
        :return: Consistent SOC code
        """
        assert 1 <= len(code_candidates) <= 2
        if len(code_candidates) == 1:
            return list(code_candidates)[0]
        else:
            assert codes[2] in code_candidates
            return codes[2]

    @staticmethod
    def get_soc_version(year: int):
        if 2000 < year <= 2009:
            return SOCVersion.soc_2000
        elif year == 2010 or year == 2011:
            return SOCVersion.hybrid_2010_2011
        elif 2011 < year <= 2018:
            return SOCVersion.soc_2010
        elif year == 2019 or year == 2020:
            return SOCVersion.hybrid_2019_2020
        elif 2020 < year:
            return SOCVersion.soc_2018
        else:
            raise ValueError(f'Invalid year: {year}')
