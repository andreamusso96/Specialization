from DataPreprocessing.DataIO import OriginalDataIO, ProcessedDataIO
import pandas as pd
from typing import Tuple, List
import numpy as np


class OccupationCharactersticDataPipeline:
    @staticmethod
    def extract_occupation_characteristic_data():
        occ_characteristics_data = OriginalDataIO.load_occupation_characteristic_data()
        education_data = OriginalDataIO.load_education_data()
        scales_data = OriginalDataIO.load_scales_data()

        # Clean Data
        occ_characteristics_data, education_data = Cleaner.clean_data(occ_characteristics_data=occ_characteristics_data,
                                                                      education_data=education_data)

        # Calculate average education, training and experience level
        education_level_data = EducationDataSummarizer.get_expected_education_level_data(education_data=education_data)

        # Scale occupation characterstic _data
        occ_characteristics_data = MinMaxScaler.scale(data=occ_characteristics_data, scales_data=scales_data,
                                                      scale_name='Importance')

        # Combine the two datasets
        occ_characteristics_data = pd.concat([occ_characteristics_data, education_level_data], ignore_index=True)
        occ_characteristics_data.sort_values(by=['O*NET-SOC Code', 'Element ID'], inplace=True, ignore_index=True)

        # Add a little bit of noise to the _data
        occ_characteristics_data = NoiseAdder.add_noise(data=occ_characteristics_data)

        # Save
        ProcessedDataIO.save_occupation_characteristic_data(occupation_characteristic_data=occ_characteristics_data)
        return occ_characteristics_data


class NoiseAdder:
    @staticmethod
    def add_noise(data: pd.DataFrame) -> pd.DataFrame:
        data['Data Value'] = data['Data Value'] + np.random.uniform(-0.0001, 0.0001, data.shape[0])
        data['Data Value'] = (data['Data Value'] - data['Data Value'].min()) / (data['Data Value'].max() - data['Data Value'].min())
        return data


class Cleaner:
    @staticmethod
    def clean_data(occ_characteristics_data: pd.DataFrame, education_data: pd.DataFrame) -> Tuple[
        pd.DataFrame, pd.DataFrame]:
        # Clean _data
        columns_to_keep = ['O*NET-SOC Code', 'Title', 'Element ID', 'Element Name', 'Scale ID', 'Scale Name',
                           'Data Value']
        occ_char_data = Cleaner._clean_occupation_characteristics_data(data=occ_characteristics_data,
                                                                       cols_to_keep=columns_to_keep)
        education_data = Cleaner._clean_education_data(data=education_data, cols_to_keep=columns_to_keep + ['Category'])
        # Ensure both datasets have the same occupations
        occ_char_data, education_data = Cleaner._remove_occupations_not_in_both_occ_characteristics_and_education_data(
            occ_char_data=occ_char_data, education_data=education_data)
        return occ_char_data, education_data

    @staticmethod
    def _remove_occupations_not_in_both_occ_characteristics_and_education_data(occ_char_data: pd.DataFrame,
                                                                               education_data: pd.DataFrame) -> Tuple[
        pd.DataFrame, pd.DataFrame]:
        common_occupations = list(
            set(occ_char_data['O*NET-SOC Code'].values).intersection(set(education_data['O*NET-SOC Code'].values)))
        occ_char_data = occ_char_data[occ_char_data['O*NET-SOC Code'].isin(common_occupations)].copy()
        education_data = education_data[education_data['O*NET-SOC Code'].isin(common_occupations)].copy()
        return occ_char_data, education_data

    @staticmethod
    def _clean_occupation_characteristics_data(data: pd.DataFrame, cols_to_keep: List[str]):
        occ_char_data = Cleaner._select_occupation_characteristic_data_for_one_scale(data=data, scale_name='Importance')
        occ_char_data = Cleaner._remove_too_detailed_occupations(data=occ_char_data)
        occ_char_data['O*NET-SOC Code'] = Cleaner._transform_occupation_code_to_reduced_form(data=occ_char_data)
        occ_char_data = Cleaner._remove_occupations_not_present_in_bls_data(data=occ_char_data)
        occ_char_data = Cleaner._remove_unnecessary_columns(data=occ_char_data, cols_to_keep=cols_to_keep)
        return occ_char_data

    @staticmethod
    def _clean_education_data(data: pd.DataFrame, cols_to_keep: List[str]):
        education_data = Cleaner._remove_too_detailed_occupations(data=data)
        education_data['O*NET-SOC Code'] = Cleaner._transform_occupation_code_to_reduced_form(data=education_data)
        education_data = Cleaner._remove_occupations_not_present_in_bls_data(data=education_data)
        education_data = Cleaner._remove_unnecessary_columns(data=education_data, cols_to_keep=cols_to_keep)
        return education_data

    @staticmethod
    def _select_occupation_characteristic_data_for_one_scale(data: pd.DataFrame, scale_name: str) -> pd.DataFrame:
        return data.loc[data['Scale Name'] == scale_name].copy()

    @staticmethod
    def _remove_too_detailed_occupations(data: pd.DataFrame) -> pd.DataFrame:
        occupations_to_keep = [not Cleaner._occupation_code_is_too_detailed(code=code) for code in
                               data['O*NET-SOC Code']]
        return data.loc[occupations_to_keep].copy()

    @staticmethod
    def _transform_occupation_code_to_reduced_form(data: pd.DataFrame) -> pd.DataFrame:
        col = 'O*NET-SOC Code'
        return data[col].apply(lambda x: x.split('.')[0]).copy()

    @staticmethod
    def _remove_occupations_not_present_in_bls_data(data: pd.DataFrame) -> pd.DataFrame:
        occ_codes_in_both_datasets = ProcessedDataIO.load_occupations_codes_present_in_both_datasets()
        return data[data['O*NET-SOC Code'].isin(occ_codes_in_both_datasets['occ_code'].values)].copy()

    @staticmethod
    def _remove_unnecessary_columns(data: pd.DataFrame, cols_to_keep: List[str]) -> pd.DataFrame:
        columns_to_remove = [col for col in data.columns if col not in cols_to_keep]
        return data.drop(columns=columns_to_remove).copy()

    @staticmethod
    def _occupation_code_is_too_detailed(code: str) -> bool:
        detail_level = code.split('.')[1]
        if detail_level == '00':
            return False
        else:
            return True


class MinMaxScaler:
    @staticmethod
    def scale(data: pd.DataFrame, scales_data: pd.DataFrame, scale_name: str) -> pd.DataFrame:
        scale_min, scale_max = MinMaxScaler._get_scale_min_and_max(scales_data=scales_data, scale_name=scale_name)
        data['Data Value'] = data['Data Value'].apply(
            lambda x: MinMaxScaler._min_max_scale(val=x, scale_min=scale_min, scale_max=scale_max)).copy()
        return data

    @staticmethod
    def _get_scale_min_and_max(scales_data: pd.DataFrame, scale_name: str) -> Tuple[float, float]:
        scale_info = scales_data[scales_data['Scale Name'] == scale_name]
        scale_max = scale_info['Maximum'].values[0]
        scale_min = scale_info['Minimum'].values[0]
        return scale_min, scale_max

    @staticmethod
    def _min_max_scale(val: float, scale_min: float, scale_max: float) -> float:
        return (val - scale_min) / (scale_max - scale_min)


class EducationDataSummarizer:
    @staticmethod
    def get_expected_education_level_data(education_data: pd.DataFrame) -> pd.DataFrame:
        scale_ids = ['RL', 'RW', 'PT', 'OJ']
        expected_level_scale_data = []
        for scale_id in scale_ids:
            expected_level_scale_data.append(
                EducationDataSummarizer._get_expected_education_level_data_for_scale(data=education_data,
                                                                                     scale_id=scale_id))

        summarized_data = pd.concat(expected_level_scale_data, ignore_index=True)
        summarized_data.sort_values(by=['O*NET-SOC Code', 'Element ID'], inplace=True, ignore_index=True)
        return summarized_data

    @staticmethod
    def _get_expected_education_level_data_for_scale(data: pd.DataFrame, scale_id: str) -> pd.DataFrame:
        data_for_scale = data.loc[data['Scale ID'] == scale_id]
        category_weights = data_for_scale.pivot(index='O*NET-SOC Code', columns='Category', values='Data Value')
        categories = np.array(category_weights.columns).astype(int) / len(category_weights.columns)
        expected_levels = (category_weights.dot(categories) / 100).to_frame(name='Data Value')
        expected_levels.reset_index(names=['O*NET-SOC Code'], inplace=True)
        data_for_scale = expected_levels.merge(
            data_for_scale.drop_duplicates(subset='O*NET-SOC Code').drop(columns=['Category', 'Data Value']),
            on='O*NET-SOC Code', how='left')
        return data_for_scale


if __name__ == '__main__':
    OccupationCharactersticDataPipeline.extract_occupation_characteristic_data()
