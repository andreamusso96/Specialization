import pandas as pd
from tqdm import tqdm

from DataPreprocessing.DataIO import DataIO
from DataPreprocessing.OMSAData.RawData import RawDataOMSA
from DataPreprocessing.OMSAData.UniformlyFormattedData import UniformlyFormattedData, UniformlyFormattedData2019
from DataPreprocessing.SOC.ConsistentCrosswalk import ConsistentSOCCrosswalk, CrosswalkData
from DataPreprocessing.OMSAData.CBSACrosswalk import ConsistentCBSACrosswalk


class Pipeline:
    def __init__(self, year_start: int = 2005, year_end: int = 2021, path_save: str=None):
        self.years = list(range(year_start, year_end+1))
        if path_save is None:
            self.path_save = DataIO.processed_omsa_data_file()
        else:
            self.path_save = path_save

    def run(self) -> pd.DataFrame:
        formatted_data = self._load_formatted_data()
        data = pd.concat([f.data for f in formatted_data], axis=0, join='outer', ignore_index=True)
        data.to_csv(self.path_save, index=False)
        return data

    def _load_formatted_data(self):
        occ_crosswalk = self._load_occ_crosswalk()
        cbsa_crosswalk = self._load_cbsa_crosswalk()
        formatted_data = []
        for year in tqdm(self.years):
            raw_data = RawDataOMSA(year=year)
            raw_data.load()
            formatted_data.append(self._format_data(raw_data=raw_data, occ_crosswalk=occ_crosswalk, cbsa_crosswalk=cbsa_crosswalk))

        return formatted_data

    @staticmethod
    def _format_data(raw_data: RawDataOMSA, occ_crosswalk: ConsistentSOCCrosswalk, cbsa_crosswalk: ConsistentCBSACrosswalk):
        if raw_data.year == 2019:
            formatted_data_year = UniformlyFormattedData2019(raw_data=raw_data, occ_crosswalk=occ_crosswalk,  cbsa_crosswalk=cbsa_crosswalk)
        else:
            formatted_data_year = UniformlyFormattedData(raw_data=raw_data, occ_crosswalk=occ_crosswalk, cbsa_crosswalk=cbsa_crosswalk)

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


if __name__ == '__main__':
    pipeline = Pipeline()
    data = pipeline.run()
