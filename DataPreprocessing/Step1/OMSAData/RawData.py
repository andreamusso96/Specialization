from DataPreprocessing.DataIO import DataIO
from DataPreprocessing.Step1.RawData import RawData


class RawDataOMSA(RawData):
    def __init__(self, year: int):
        assert 2005 <= year <= 2021
        folder = DataIO.raw_omsa_data_folder(year=year)
        super().__init__(year=year, folder=folder, file_name_starts_with="msa")




