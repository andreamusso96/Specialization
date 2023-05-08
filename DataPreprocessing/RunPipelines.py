from DataPreprocessing.OccupationNetworkData import OccupationNetworkDataPipeline
from DataPreprocessing.OccupationCharacteristicData import OccupationCharactersticDataPipeline
from DataPreprocessing.OccupationMSAData import OccupationMSADataPipeline


def run_pipelines():
    OccupationMSADataPipeline.process_occupation_msa_data()
    OccupationCharactersticDataPipeline.extract_occupation_characteristic_data()
    OccupationNetworkDataPipeline.build_occupation_network()


if __name__ == '__main__':
    run_pipelines()