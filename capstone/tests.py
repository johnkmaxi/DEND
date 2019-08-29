"""
Unit tests
"""
import re
from zipfile import ZipFile
import unittest
import pandas as pd
import numpy as np

from helper import file_finder, csv_in_zip, calc_problem_duration

class TestHelpers(unittest.TestCase):

    def test_file_finder(self):
        keys = ['88101', 'PRESS', 'RH_DP', 'TEMP', 'WIND']
        search_list = ['this has 88101 in it', 'this has TEMP', 'should find RH_DP', 'here is WIND', 'do you feel the PRESS?', 'this is not found']
        found = file_finder(keys, search_list)
        self.assertTrue(len(found), 5)

    def test_csv_in_zip(self):
        df = csv_in_zip('data\\test.zip')
        self.assertTrue(df.shape, (2,4))

    def test_calc_problem_duration(self):
        df = pd.DataFrame([[1, np.nan, 2],[2,2,2]])
        df = df.apply(calc_problem_duration, axis=1)
        self.assertTrue(df.isnull().sum(), 1)

if __name__ == '__main__':
    unittest.main()
