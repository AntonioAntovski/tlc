import unittest
import os
from data_transformer import DataTransformer


class TLCTest(unittest.TestCase):
    def setUp(self):
        self.data_transformer = DataTransformer()
        self.parent_folder = "test_utils"
        self.csv_file_name = "test_utils/yellow_tripdata_2018-01.csv"

    def test_merge_datasets(self):
        if os.path.exists(f"{self.parent_folder}/yellow_green_tripdata.csv"):
            os.remove(f"{self.parent_folder}/yellow_green_tripdata.csv")

        self.data_transformer.merge_yellow_green_datasets(self.parent_folder)
        self.assertTrue(
            os.path.exists(f"{self.parent_folder}/yellow_green_tripdata.csv")
        )

    def test_parquet_format(self):
        self.data_transformer.to_parquet_format(self.csv_file_name)
        self.assertTrue(os.path.exists(self.csv_file_name.replace("csv", "parquet")))

    def test_avro_format(self):
        self.data_transformer.to_avro_format(self.csv_file_name)
        self.assertTrue(os.path.exists(self.csv_file_name.replace("csv", "avro")))
