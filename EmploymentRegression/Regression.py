from EmploymentRegression.QuantileEmploymentCountsRegression import QuantileEmploymentCountsRegression
from EmploymentRegression.QuantileEmploymentCounts import QuantileEmploymentCounts
import pandas as pd


class Regression:
    @staticmethod
    def get_quantile_employment_counts_regression(occ_network: pd.DataFrame, occ_msa_data: pd.DataFrame, n_quantiles: int) -> QuantileEmploymentCountsRegression:
        counts = QuantileEmploymentCounts(occupation_network=occ_network, occupation_msa_data=occ_msa_data)
        msa_occ_employment_tables_for_quantiles = counts.get_msa_occ_employment_tables(n_quantiles=n_quantiles)
        productivity_data = occ_msa_data.pivot(index='area', columns='occ_code', values='a_median')
        regression = QuantileEmploymentCountsRegression(msa_occ_employment_tables_for_quantiles=msa_occ_employment_tables_for_quantiles, productivity_data=productivity_data)
        return regression