import os

import pandas as pd


class RawData:
    def __init__(self, year: int, folder: str, file_name_starts_with: str = ""):
        self.year = year
        self.folder = folder
        self.file_name_starts_with = file_name_starts_with
        self.data = None

    def _get_data_paths(self):
        files_in_folder = os.listdir(path=self.folder)
        data_files_paths = [self.folder + f'/{f_name}' for f_name in files_in_folder if f_name.lower().startswith(self.file_name_starts_with)]
        return data_files_paths

    def _read_data(self):
        data_paths = self._get_data_paths()
        self.data = {f_name.split('/')[-1].replace('.xlsx', ''): pd.read_excel(io=f_name, header=0, sheet_name=0) for f_name in data_paths}

    def load(self):
        self._read_data()





