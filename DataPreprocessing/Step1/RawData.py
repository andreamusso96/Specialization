import os

import pandas as pd

from DataPreprocessing.DataIO import DataIO


class RawData:
    def __init__(self, year: int, folder: str, file_name_starts_with: str = ""):
        self.year = year
        self.folder = folder
        self.file_name_starts_with = file_name_starts_with
        self.data = None

    def load(self):
        self.data = DataIO.load_folder(folder=self.folder, file_name_starts_with=self.file_name_starts_with)





