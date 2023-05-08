from config import DATA_PATH
from DataPreprocessing.RawData import RawData


class RawDataOMSA(RawData):
    def __init__(self, year: int):
        assert 2005 <= year <= 2021
        folder = DATA_PATH + f'/OriginalData/OMSAData/oesm{str(year)[2:]}ma'
        super().__init__(year=year, folder=folder, file_name_starts_with="msa")




