
from DataPreprocessing.DataIO import DataIO


class SpecializationIndex:
    def __init__(self):
        self.cbsa = None
        self.occ_cbsa = None

    def load(self):
        self.cbsa = DataIO.load(file_path=DataIO.specialization_index_cbsas_file(), index_col=0)
        self.occ_cbsa = DataIO.load(file_path=DataIO.specialization_index_occs_cbsas_file(), index_col=0)


if __name__ == '__main__':
    s = SpecializationIndex()
    s.load()
    print(s.cbsa)
    print(s.occ_cbsa)