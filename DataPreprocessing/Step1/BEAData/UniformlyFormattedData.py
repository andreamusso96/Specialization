import pandas as pd

from DataPreprocessing.Step1.BEAData.RawData import RawDataBEAGDP, RawDataBEA, RawDataBEAPopulation
from DataPreprocessing.Step1.CBSA.CBSACrosswalk import ConsistentCBSACrosswalk


class UniformlyFormattedDataBase:
    def __init__(self, raw_data: RawDataBEA, cbsa_crosswalk: ConsistentCBSACrosswalk):
        self.raw_data = raw_data
        self.cbsa_crosswalk = cbsa_crosswalk
        self.data = self.raw_data.data

    def load(self):
        self._drop_unnecessary_columns()
        self._melt()
        self._set_consistent_cbsa_codes()
        self._set_consistent_cbsa_names()
        self._reformat_descriptions()

    def _melt(self):
        self.data = self.data.melt(id_vars=['GeoFips', 'GeoName', 'Description'], var_name='Year', value_name='Data Value')
        self.data.dropna(subset=['Data Value'], inplace=True, ignore_index=True)

    def _drop_unnecessary_columns(self):
        self.data.drop(columns=['LineCode'], inplace=True)

    def _set_consistent_cbsa_codes(self):
        self.data['GeoFips'] = self.data['GeoFips'].astype(int).apply(self.cbsa_crosswalk.get_consistent_cbsa_code)
        self.data.dropna(subset=['GeoFips'], inplace=True, ignore_index=True)

    def _set_consistent_cbsa_names(self):
        self.data['GeoName'] = self.data['GeoFips'].astype(int).apply(self.cbsa_crosswalk.get_consistent_cbsa_name)

    def _reformat_descriptions(self):
        self.data['Description'] = self.data['Description'].apply(self._reformat_description)

    def _cbsa_fips_to_string(self):
        self.data['GeoFips'] = self.data['GeoFips'].astype(int).astype(str)

    def _sort_data(self):
        self.data.sort_values(by=['GeoFips', 'Year', 'Description'], inplace=True, ignore_index=True)

    @staticmethod
    def _reformat_description(description: str) -> str:
        if description.startswith('Population'):
            return 'population'
        elif description.startswith('Real GDP'):
            return 'real_gdp_thousands'
        elif description.startswith('Current-dollar GDP'):
            return 'current_dollar_gdp_thousands'
        elif description.startswith('Personal income'):
            return 'personal_income_thousands'
        elif description.startswith('Per capita personal income'):
            return 'per_capita_personal_income'
        elif description.startswith('Chain-type'):
            return 'chained_type_gdp'
        else:
            raise ValueError(f'Unexpected description: {description}')


class UniformlyFormattedData:
    def __init__(self):
        self.raw_data = [RawDataBEAGDP(), RawDataBEAPopulation()]
        self.cbsa_crosswalk = ConsistentCBSACrosswalk()
        self.data = None

    def load(self):
        uniformly_formatted_data = []
        for rd in self.raw_data:
            rd.load()
            formatted_data = UniformlyFormattedDataBase(raw_data=rd, cbsa_crosswalk=self.cbsa_crosswalk)
            formatted_data.load()
            uniformly_formatted_data.append(formatted_data)

        self.data = pd.concat([u.data for u in uniformly_formatted_data], ignore_index=True)
        return self.data


if __name__ == '__main__':
    data = UniformlyFormattedData().load()
    from DataPreprocessing.DataIO import DataIO
    DataIO.save(data, DataIO.processed_bea_data_file())
