import numpy as np

from DataPreprocessing.Step1.OMSAData.UniformlyFormattedData import UniformlyFormattedData as OMSAUniformlyFormattedData
from DataPreprocessing.Step1.ONETData.UniformlyFormattedData import UniformlyFormattedData as ONETUniformlyFormattedData
from DataPreprocessing.Step2.OccupationNetwork import OccupationNetwork
from DataPreprocessing.Step2.OMSAData import OMSAData
from DataPreprocessing.Step2.EmploymentByOccDistanceCalculator import EmploymentByOccDistanceCalculator
from DataPreprocessing.Step2.SpecializationIndexCalculator import SpecializationIndexCalculator
from DataPreprocessing.DataIO import DataIO


def run_pipeline():
    """

    print('STEP 1: Converting raw data to uniformly formatted data')

    print('STEP 1: ONET data')
    
    onet_formatted_data = ONETUniformlyFormattedData().load()
    DataIO.save(data=onet_formatted_data, path=DataIO.processed_onet_data_file())
    
    print('STEP 1: OMSA data')

    omsa_formatted_data = OMSAUniformlyFormattedData().load()
    DataIO.save(data=omsa_formatted_data, path=DataIO.processed_omsa_data_file())

    print('STEP 1: Completed')
    print('STEP 2: Computing employment by occ distance and specialization indices')"""

    print('STEP 2: Loading data')

    onet_network_data = OccupationNetwork()
    onet_network_data.load()
    omsa_data = OMSAData()
    omsa_data.load()

    print('STEP 2: Computing employment by occ distance')

    quantiles = np.array([0, 0.25, 0.5, 0.75, 1])
    employment_by_occ_distance = EmploymentByOccDistanceCalculator(occ_network=onet_network_data, omsa_data=omsa_data, quantiles=quantiles)
    employment_by_occ_distance = employment_by_occ_distance.compute()
    DataIO.save_dict(data_dict=employment_by_occ_distance, path=DataIO.employment_by_occ_distance_folder(), index=True)

    print('STEP 2: Computing specialization indices')
    """
    specialization_index = SpecializationIndexCalculator(occ_network=onet_network_data, omsa_data=omsa_data)
    specialization_index_occs_cbsas, specialization_index_cbsas = specialization_index.compute()
    DataIO.save(data=specialization_index_cbsas, path=DataIO.specialization_index_cbsas_file(), index=True)
    DataIO.save(data=specialization_index_occs_cbsas, path=DataIO.specialization_index_occs_cbsas_file(), index=True)"""

    print('STEP 2: Completed')
    print('Pipeline Completed')


if __name__ == '__main__':
    run_pipeline()