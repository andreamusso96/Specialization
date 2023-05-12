import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from sklearn.preprocessing import MinMaxScaler

from DataPreprocessing.DataIO import DataIO


class OccupationNetwork:
    def __init__(self):
        self.network = None

    def get_all_occ_codes(self):
        return list(self.network.index)

    def get_all_occ_codes_as_dataframe(self):
        return pd.DataFrame(self.get_all_occ_codes(), columns=['occ_code'])

    def get_distance_distribution_samples(self):
        samples = self.network.values[np.triu_indices(self.network.shape[0], k=1)]
        return samples

    def get_distance_distribution_quantiles(self, quantiles: np.ndarray):
        quantiles_distance_distribution = np.quantile(self.get_distance_distribution_samples(), q=quantiles)
        return quantiles_distance_distribution

    def get_occ_codes_within_distance_interval(self, code: str, a: float, b: float):
        assert 0 <= a < b <= 1
        distances_from_code = self.network.loc[code]
        indices_codes_within_interval = np.argwhere((distances_from_code > a) & (distances_from_code <= b)).flatten()
        codes_within_interval = list(self.network.index[indices_codes_within_interval])
        return codes_within_interval

    def load(self):
        onet_data = DataIO.load(file_path=DataIO.processed_onet_data_file())
        self.network = self._build_network(onet_data=onet_data)
        return self.network

    def _build_network(self, onet_data: pd.DataFrame):
        embedding_vectors = self._embedding(onet_data=onet_data)
        scaled_distance_matrix = self._distance(embedding=embedding_vectors)
        distance_matrix = pd.DataFrame(scaled_distance_matrix, index=embedding_vectors.index, columns=embedding_vectors.index)
        return distance_matrix

    @staticmethod
    def _embedding(onet_data: pd.DataFrame) -> pd.DataFrame:
        embedding_vectors = onet_data.pivot(index='O*NET-SOC Code', columns='Element ID', values='Data Value')
        embedding_vectors.fillna(value=0, inplace=True)
        return embedding_vectors

    @staticmethod
    def _distance(embedding: pd.DataFrame) -> np.ndarray:
        distance_matrix = squareform(pdist(embedding, metric='euclidean'))
        scaler = MinMaxScaler()
        scaled_distance_matrix = scaler.fit_transform(distance_matrix)
        return scaled_distance_matrix





if __name__ == '__main__':
    n = OccupationNetwork()
    n.load()

