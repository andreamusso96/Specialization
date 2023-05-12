from typing import Dict, Any, List
import os

import pandas as pd

from config import DATA_PATH


class DataIO:
    # FOLDERS
    @staticmethod
    def original_data_folder():
        return f'{DATA_PATH}/OriginalData'

    @staticmethod
    def processed_data_folder():
        return f'{DATA_PATH}/ProcessedData'

    @staticmethod
    def omsa_data_folder():
        return f'{DataIO.original_data_folder()}/OMSAData'

    @staticmethod
    def onet_data_folder():
        return f'{DataIO.original_data_folder()}/ONETData'

    @staticmethod
    def soc_data_folder():
        return f'{DataIO.original_data_folder()}/SOC'

    @staticmethod
    def raw_omsa_data_folder(year: int):
        folder_name = f'oesm{str(year)[2:]}ma'
        return f'{DataIO.omsa_data_folder()}/{folder_name}'

    @staticmethod
    def raw_onet_data_folder(year: int):
        folder_name = f'onet{year}'
        return f'{DataIO.onet_data_folder()}/{folder_name}'

    @staticmethod
    def employment_by_occ_distance_folder():
        return f'{DataIO.processed_data_folder()}/EmploymentByOccDistance'

    @staticmethod
    def raw_bea_data_folder():
        return f'{DataIO.original_data_folder()}/BEAData'

    # FILES
    @staticmethod
    def cbsa_crosswalk_file():
        file_name = 'cbsa_crosswalk.xls'
        return f'{DataIO.omsa_data_folder()}/{file_name}'

    @staticmethod
    def soc_crosswalk_file(file_name: str):
        return f'{DataIO.soc_data_folder()}/{file_name}'

    @staticmethod
    def bea_gdp_file():
        file_name = 'bea_gdp.xlsx'
        return f'{DataIO.raw_bea_data_folder()}/{file_name}'

    @staticmethod
    def bea_population_income_file():
        file_name = 'bea_population_income.xlsx'
        return f'{DataIO.raw_bea_data_folder()}/{file_name}'

    @staticmethod
    def processed_omsa_data_file():
        file_name = 'omsa_data.csv'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def processed_onet_data_file():
        file_name = 'onet_data.xlsx'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def processed_bea_data_file():
        file_name = 'bea_data.xlsx'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def specialization_index_cbsas_file():
        file_name = 'specialization_index_cbsa.xlsx'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def specialization_index_occs_cbsas_file():
        file_name = 'specialization_index_occs_cbsas.csv'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    # IO OPERATIONS

    @staticmethod
    def load(file_path: str, **kwargs):
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path, **kwargs)
        elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            return pd.read_excel(file_path, **kwargs)
        else:
            raise ValueError(f'Unknown file extension: {file_path}')

    @staticmethod
    def load_dict(keys: List[Any], path: str, **kwargs) -> Dict[Any, pd.DataFrame]:
        data_dict = {}
        for key in keys:
            data_dict[key] = DataIO.load(file_path=f'{path}/{DataIO._key_to_str(key=key)}.csv', **kwargs)
        return data_dict

    @staticmethod
    def load_folder(folder: str, file_name_starts_with: str = "", **kwargs) -> Dict[str, pd.DataFrame]:
        files_in_folder = [f_name for f_name in os.listdir(path=folder) if not (f_name.startswith('.') or f_name.startswith('~'))]
        file_paths = [folder + f'/{f_name}' for f_name in files_in_folder if f_name.lower().startswith(file_name_starts_with)]
        data_files = {}
        for fpath in file_paths:
            fname = fpath.split('/')[-1].split('.')[0]
            data_files[fname] = DataIO.load(file_path=fpath, **kwargs)
        return data_files

    @staticmethod
    def save(data: pd.DataFrame, path: str, index: bool = False, **kwargs):
        if path.endswith('.csv'):
            data.to_csv(path, index=index, **kwargs)
        elif path.endswith('.xlsx') or path.endswith('.xls'):
            data.to_excel(path, index=index, **kwargs)
        else:
            raise ValueError(f'Unknown file extension: {path}')

    @staticmethod
    def save_dict(data_dict: Dict[Any, pd.DataFrame], path: str, **kwargs):
        for key, value in data_dict.items():
            DataIO.save(data=value, path=f'{path}/{DataIO._key_to_str(key=key)}.csv', **kwargs)

    @staticmethod
    def _key_to_str(key: Any):
        if isinstance(key, tuple):
            return '_'.join([str(k) for k in key])


