from typing import Dict, Any

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

    # FILES
    @staticmethod
    def cbsa_crosswalk_file():
        file_name = 'cbsa_crosswalk.xls'
        return f'{DataIO.omsa_data_folder()}/{file_name}'

    @staticmethod
    def processed_omsa_data_file():
        file_name = 'omsa_data.csv'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def processed_onet_data_file():
        file_name = 'onet_data.xlsx'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def specialization_index_cbsas_file():
        file_name = 'specialization_index_cbsa.xlsx'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def specialization_index_occs_cbsas_file():
        file_name = 'specialization_index_occs_cbsas.csv'
        return f'{DataIO.processed_data_folder()}/{file_name}'

    @staticmethod
    def save(data: pd.DataFrame, path: str):
        if path.endswith('.csv'):
            data.to_csv(path, index=False)
        elif path.endswith('.xlsx'):
            data.to_excel(path, index=False)
        elif path.endswith('.xls'):
            data.to_excel(path, index=False)
        else:
            raise ValueError(f'Unknown file extension: {path}')

    @staticmethod
    def save_dict(data_dict: Dict[Any, pd.DataFrame], path: str):
        for key, value in data_dict.items():
            DataIO.save(data=value, path=f'{path}/{DataIO._key_to_str(key=key)}.csv')

    @staticmethod
    def _key_to_str(key: Any):
        if isinstance(key, tuple):
            return '_'.join([str(k) for k in key])


