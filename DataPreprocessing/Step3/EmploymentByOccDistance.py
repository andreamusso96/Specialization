import numpy as np
import pandas as pd

from DataPreprocessing.DataIO import DataIO


class EmploymentByOccDistance:
    def __init__(self, quantiles: np.ndarray):
        self.quantiles = quantiles
        self.data = None

    def load(self):
        keys = [(self.quantiles[i], self.quantiles[i+1]) for i in range(len(self.quantiles) - 1)]
        self.data = DataIO.load_dict(keys=keys, path=DataIO.employment_by_occ_distance_folder())
        return self.data


if __name__ == '__main__':
    quantiles = np.array([0,0.25,0.5,0.75, 1])
    e = EmploymentByOccDistance(quantiles=quantiles)
    e.load()
    print(e.data)
