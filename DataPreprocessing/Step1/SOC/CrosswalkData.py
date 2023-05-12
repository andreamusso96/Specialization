import pandas as pd

from config import DATA_PATH
from DataPreprocessing.Step1.SOC.Utils import Columns


class CrosswalkFile:
    def __init__(self, file_name: str):
        self.file_path = f'{DATA_PATH}/OriginalData/SOC/{file_name}'
        self.data = None

    def _read_excel(self):
        pass

    def _format_columns(self):
        pass

    def _drop_rows(self):
        pass

    def _code_to_title_map(self):
        pass

    def load(self):
        self.data = self._read_excel()
        self._format_columns()
        self._drop_rows()
        self._code_to_title_map()
        return self.data


class SOC2000toSOC2010(CrosswalkFile):
    def __init__(self):
        file_name = 'soc_2000_to_2010_crosswalk.xls'
        super().__init__(file_name=file_name)

    def _read_excel(self):
        df = pd.read_excel(io=self.file_path, header=6, sheet_name=0)
        return df

    def _format_columns(self):
        col_map = {'2000 SOC code': Columns.soc_2000_code, '2000 SOC title': Columns.soc_2000_title,
                   '2010 SOC code': Columns.soc_2010_code, '2010 SOC title': Columns.soc_2010_title}
        self.data.rename(columns=col_map, inplace=True)

    def _drop_rows(self):
        self.data.dropna(axis=0, how='all', inplace=True)


class SOC2010toSOC2018(CrosswalkFile):
    def __init__(self):
        super().__init__(file_name='soc_2010_to_2018_crosswalk.xlsx')

    def _read_excel(self):
        df = pd.read_excel(io=self.file_path, header=8, sheet_name=0)
        return df

    def _format_columns(self):
        col_map = {'2010 SOC Code': Columns.soc_2010_code, '2010 SOC Title': Columns.soc_2010_title,
                   '2018 SOC Code': Columns.soc_2018_code, '2018 SOC Title': Columns.soc_2018_title}
        self.data.rename(columns=col_map, inplace=True)


class Hybrid2010and2011(CrosswalkFile):
    def __init__(self):
        super().__init__(file_name='2010_and_2011_oes_classification.xls')

    def _read_excel(self):
        df = pd.read_excel(io=self.file_path, header=9, sheet_name=1)
        return df

    def _format_columns(self):
        col_map = {'OES 2010 code': Columns.hybrid_2010_2011_code, 'OES 2010 Title ': Columns.hybrid_2010_2011_title,
                   '2000 SOC code': Columns.soc_2000_code, 'SOC 2000 Title': Columns.soc_2000_title,
                   '2010 SOC code': Columns.soc_2010_code, 'SOC 2010 Title': Columns.soc_2010_title}
        self.data.drop(columns=['Longer 2010 Titles', 'Notes'], inplace=True)
        self.data.rename(columns=col_map, inplace=True)

    def _drop_rows(self):
        self.data.dropna(axis=0, how='any', inplace=True)


class Hybrid2019and2020(CrosswalkFile):
    def __init__(self):
        super().__init__(file_name='oes_2019_hybrid_structure.xlsx')

    def _read_excel(self):
        df = pd.read_excel(io=self.file_path, header=5, sheet_name=0)
        return df

    def _format_columns(self):
        col_map = {'OES 2019 Estimates Code': Columns.hybrid_2019_2020_code,
                   'OES 2019 Estimates Title': Columns.hybrid_2019_2020_title,
                   '2010 SOC Code': Columns.soc_2010_code, '2010 SOC Title': Columns.soc_2010_title,
                   '2018 SOC Code ': Columns.soc_2018_code, '2018 SOC Title': Columns.soc_2018_title}
        self.data.drop(columns=['OES 2018 Estimates Code', 'OES 2018 Estimates Title', 'NOTES'], inplace=True)
        self.data.rename(columns=col_map, inplace=True)


class CrosswalkData:
    def __init__(self):
        self.soc2000_to_soc2010 = SOC2000toSOC2010()
        self.soc2010_to_soc2018 = SOC2010toSOC2018()
        self.hybrid_2010_2011 = Hybrid2010and2011()
        self.hybrid_2019_2020 = Hybrid2019and2020()
        self._crosswalk_files = [self.hybrid_2019_2020, self.hybrid_2010_2011, self.soc2010_to_soc2018,
                                 self.soc2000_to_soc2010]
        self.data = None

    def _load_crosswalk_files(self):
        for crosswalk_file in self._crosswalk_files:
            crosswalk_file.load()

    def _merge_crosswalk_files(self):
        merged_crosswalks = self.hybrid_2019_2020.data.merge(self.hybrid_2010_2011.data, how='outer',
                                                             on=Columns.soc_2010_code, suffixes=('', '_drop'),
                                                             copy=True)
        merged_crosswalks.drop(columns=[col for col in merged_crosswalks.columns if col.endswith('_drop')],
                               inplace=True)
        merged_crosswalks.dropna(axis=0, how='any', inplace=True)
        return merged_crosswalks

    def load(self):
        self._load_crosswalk_files()
        self.data = self._merge_crosswalk_files()
        return self.data