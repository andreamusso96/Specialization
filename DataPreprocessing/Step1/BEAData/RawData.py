from DataPreprocessing.Step1.RawData import RawData
from DataPreprocessing.DataIO import DataIO


class RawDataBEA:
    def __init__(self, file_path: str, header: int, skipfooter: int):
        self.file_path = file_path
        self.header = header
        self.skipfooter = skipfooter
        self.data = None

    def load(self):
        self.data = DataIO.load(file_path=self.file_path, header=self.header, skipfooter=self.skipfooter)


class RawDataBEAGDP(RawDataBEA):
    def __init__(self):
        super().__init__(file_path=DataIO.bea_gdp_file(), header=3, skipfooter=5)


class RawDataBEAPopulation(RawDataBEA):
    def __init__(self):
        super().__init__(file_path=DataIO.bea_population_income_file(), header=3, skipfooter=10)