from DataIO import ProcessedDataIO
import pandas as pd
from scipy.spatial.distance import pdist, squareform


class OccupationNetworkDataPipeline:
    @staticmethod
    def build_occupation_network() -> pd.DataFrame:
        # Load _data
        occ_characteristics_data = ProcessedDataIO.load_occupation_characteristic_data()

        # Construct Occupation Network
        occupation_network_matrix = OccupationNetworkConstructor.construct_occupation_network(occ_characteristics_data=occ_characteristics_data)

        # Save Occupation Network
        ProcessedDataIO.save_occupation_network(occupation_network=occupation_network_matrix)
        return occupation_network_matrix


class OccupationNetworkConstructor:
    @staticmethod
    def construct_occupation_network(occ_characteristics_data) -> pd.DataFrame:
        df = occ_characteristics_data.pivot(index='O*NET-SOC Code', columns='Element ID', values='Data Value')
        cosine_similarity = squareform(pdist(df, metric='cosine'))
        df_cosine_similarity_occupations = pd.DataFrame(cosine_similarity, index=df.index, columns=df.index)
        return df_cosine_similarity_occupations


if __name__ == '__main__':
    OccupationNetworkDataPipeline.build_occupation_network()