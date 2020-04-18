"""
Created on Sun Apr 12 09:17:47 2017
@author: ankurD
"""
import unittest
import config
from eq_prediction import EQPrediction


class TestEQPrediction(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_base_url(self):
        self.assertEqual(config.EARTHQUAKE_BASE_URL, "http://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson")

    def test_threashold_pool_size(self):
        self.assertEqual(config.THREADPOOL_SIZE, 4)

    def test_data_cnt_chk(self):
        start_date    = '2020-01-01'
        end_date      = '2020-01-10'
        eq_prediction = EQPrediction(config)
        eq_prediction.load_data(start_date, end_date)
        df       = eq_prediction.const_bigdata
        rows_cnt = len(df.index)

        self.assertFalse(rows_cnt, 100)


if __name__ == '__main__':
    unittest.main()
