from DataPreprocessing.DataIO import DataIO


class BEAData:
    def __init__(self):
        self.data = None

    def load(self):
        self.data = DataIO.load(file_path=DataIO.processed_bea_data_file())
        return self.data