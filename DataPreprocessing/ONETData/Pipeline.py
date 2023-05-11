import pandas as pd
from tqdm import tqdm

from DataPreprocessing.DataIO import DataIO
from DataPreprocessing.ONETData.RawData import RawDataONET
from DataPreprocessing.ONETData.UniformlyFormattedData import UniformlyFormattedData
from DataPreprocessing.SOC.ConsistentCrosswalk import ConsistentSOCCrosswalk, CrosswalkData


class Pipeline:
    def __init__(self, year_start: int = 2019, year_end: int = 2019, path_save=None):
        self.years = list(range(year_start, year_end+1))
        if path_save is None:
            self.path_save = DataIO.processed_onet_data_file()
        else:
            self.path_save = path_save

    def run(self) -> pd.DataFrame:
        formatted_data = self._load_formatted_data()
        data = pd.concat([f.data for f in formatted_data], axis=0, join='outer', ignore_index=True)
        data.to_excel(self.path_save, index=False)
        return data

    def _load_formatted_data(self):
        occ_crosswalk = self._load_occ_crosswalk()
        formatted_data = []
        for year in tqdm(self.years):
            raw_data = RawDataONET(year=year)
            raw_data.load()
            formatted_data_year = UniformlyFormattedData(raw_data=raw_data, occ_crosswalk=occ_crosswalk)
            formatted_data_year.load()
            formatted_data.append(formatted_data_year)

        return formatted_data

    @staticmethod
    def _load_occ_crosswalk():
        crosswalk_data = CrosswalkData()
        crosswalk_data.load()
        occ_crosswalk = ConsistentSOCCrosswalk(crosswalk_data)
        return occ_crosswalk


if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()