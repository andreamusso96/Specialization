from config import PROJECT_PATH as project_path
from Utils import Data
import pandas as pd


class BaseIO:
    @staticmethod
    def _load_data(path: str, file_name: str, header: int = 0, index_col: int = None) -> pd.DataFrame:
        return pd.read_excel(f'{path}/{file_name}.xlsx', header=header, index_col=index_col)

    @staticmethod
    def _save_data(data: pd.DataFrame, path: str, file_name: str, save_index=False) -> None:
        data.to_excel(f'{path}/{file_name}.xlsx', index=save_index)


class OriginalDataIO(BaseIO):
    data_path = f'{project_path}/Data/OriginalData'

    @staticmethod
    def load_occupation_msa_data() -> pd.DataFrame:
        file_name = 'occupation_msa'
        return OriginalDataIO._load_data(path=OriginalDataIO.data_path, file_name=file_name)

    @staticmethod
    def load_occupation_characteristic_data() -> pd.DataFrame:
        file_names = ['abilities', 'knowledge', 'skills', 'work_activities']
        data = [OriginalDataIO._load_data(path=OriginalDataIO.data_path, file_name=file_name) for file_name in file_names]
        df_occupation_characteristics = pd.concat(data, axis=0)
        return df_occupation_characteristics

    @staticmethod
    def load_scales_data() -> pd.DataFrame:
        file_name = 'scales'
        return OriginalDataIO._load_data(path=OriginalDataIO.data_path, file_name=file_name)

    @staticmethod
    def load_soc_definitions() -> pd.DataFrame:
        file_name = 'soc_2018_definitions'
        return OriginalDataIO._load_data(path=OriginalDataIO.data_path, file_name=file_name, header=7)

    @staticmethod
    def load_education_data() -> pd.DataFrame:
        file_name = 'education_training_experience'
        return OriginalDataIO._load_data(path=OriginalDataIO.data_path, file_name=file_name)


class ProcessedDataIO(BaseIO):
    data_path = f'{project_path}/Data/ProcessedData'
    @staticmethod
    def load_all_data():
        occ_msa_data = ProcessedDataIO.load_occupation_msa_data()
        occ_characteristics_data = ProcessedDataIO.load_occupation_characteristic_data()
        occ_network = ProcessedDataIO.load_occupation_network()
        occ_codes = ProcessedDataIO.load_occupations_codes_present_in_both_datasets()
        return Data(occ_codes=occ_codes, occ_msa_data=occ_msa_data, occ_char_data=occ_characteristics_data, occ_network=occ_network)

    @staticmethod
    def load_occupation_msa_data() -> pd.DataFrame:
        file_name = 'occupation_msa'
        return ProcessedDataIO._load_data(path=ProcessedDataIO.data_path, file_name=file_name)

    @staticmethod
    def load_occupation_characteristic_data() -> pd.DataFrame:
        file_name = 'occupation_characteristics'
        return ProcessedDataIO._load_data(path=ProcessedDataIO.data_path, file_name=file_name)

    @staticmethod
    def load_occupation_network() -> pd.DataFrame:
        file_name = 'occupation_network'
        return ProcessedDataIO._load_data(path=ProcessedDataIO.data_path, file_name=file_name, index_col=0)

    @staticmethod
    def load_total_employment_msa() -> pd.DataFrame:
        file_name = 'total_employment_msa'
        return ProcessedDataIO._load_data(path=ProcessedDataIO.data_path, file_name=file_name)

    @staticmethod
    def load_occupations_codes_present_in_both_datasets():
        file_name = 'common_occupation_codes'
        return ProcessedDataIO._load_data(path=ProcessedDataIO.data_path, file_name=file_name)

    # SAVE DATA
    @staticmethod
    def save_occupation_msa_data(occupation_msa_data: pd.DataFrame):
        file_name = 'occupation_msa'
        ProcessedDataIO._save_data(occupation_msa_data, file_name=file_name, path=ProcessedDataIO.data_path)

    @staticmethod
    def save_occupation_characteristic_data(occupation_characteristic_data: pd.DataFrame):
        file_name = 'occupation_characteristics'
        ProcessedDataIO._save_data(occupation_characteristic_data, file_name=file_name, path=ProcessedDataIO.data_path)

    @staticmethod
    def save_occupation_network(occupation_network: pd.DataFrame):
        file_name = 'occupation_network'
        ProcessedDataIO._save_data(occupation_network, file_name=file_name, save_index=True, path=ProcessedDataIO.data_path)

    @staticmethod
    def save_total_employment_msa(total_employment_msa: pd.DataFrame):
        file_name = 'total_employment_msa'
        ProcessedDataIO._save_data(total_employment_msa, file_name=file_name, path=ProcessedDataIO.data_path)

    @staticmethod
    def save_occupations_codes_present_in_both_datasets(common_occupation_codes: pd.DataFrame):
        file_name = 'common_occupation_codes'
        ProcessedDataIO._save_data(common_occupation_codes, file_name=file_name, path=ProcessedDataIO.data_path)