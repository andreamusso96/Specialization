from OccupationClassification.OccupationClassifier import OccupationClassifier
from Plotting.PlotRegressionResults import PlotRegressionResult
from EmploymentRegression.Regression import Regression
from Utils import Data
from typing import List, Tuple


def plot_regression_results_for_various_characteristics(data: Data, characteristic_names_and_ids: List[Tuple[str, str]], n_quantiles_reg: int, n_quantiles_char: int):
    classifier = OccupationClassifier(occ_codes=data.occ_codes, occ_msa_data=data.occ_msa_data, occ_characteristics_data=data.occ_char_data)
    regression = Regression.get_quantile_employment_counts_regression(occ_msa_data=data.occ_msa_data, occ_network=data.occ_network, n_quantiles=n_quantiles_reg)
    for char_name, char_id in characteristic_names_and_ids:
        occ_grouped_by_characteristic = classifier.get_occupations_grouped_by_characteristic(n_quantiles=n_quantiles_char, characteristic_id=char_id)
        plotter = PlotRegressionResult(regression=regression)
        plotter.plot(occupations=occ_grouped_by_characteristic, title=char_name)