import pandas as pd

from DataPreprocessing.DataIO import DataIO
from DataPreprocessing.Step1.RawData import RawData


class RawDataONET(RawData):
    def __init__(self, year: int):
        folder = DataIO.raw_onet_data_folder(year=year)
        super().__init__(year=year, folder=folder)

    def _get_data(self, filename_startswith: str) -> pd.DataFrame:
        return [self.data[file_name] for file_name in self.data if file_name.startswith(filename_startswith)][0]

    def get_education_data(self):
        return self._get_data(filename_startswith='education')

    def get_scales_data(self):
        return self._get_data(filename_startswith='scales')

    def get_abilities_data(self):
        return self._get_data(filename_startswith='abilities')

    def get_knowledge_data(self):
        return self._get_data(filename_startswith='knowledge')

    def get_work_activities_data(self):
        return self._get_data(filename_startswith='work_activities')

    def get_skills_data(self):
        return self._get_data(filename_startswith='skills')


if __name__ == '__main__':
    r = RawDataONET(year=2019)
    r.load()
    print(r)
