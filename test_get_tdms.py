import unittest
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from nptdms import TdmsFile
import os


class TestTDMSProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Initializes the SparkSession object and creates sample TDMS files for testing.
        """
        # Initialize SparkSession object
        cls.spark = SparkSession.builder.appName("TDMSProcessingTest").getOrCreate()

        # Create sample TDMS files for testing
        cls.test_data_folder = "./test_data/"
        if not os.path.exists(cls.test_data_folder):
            os.mkdir(cls.test_data_folder)

        test_data = {
            "channel1": [(1, 2.0), (2, 4.0), (3, 6.0)],
            "channel2": [(1, "foo"), (2, "bar"), (3, "baz")]
        }
        for channel, data in test_data.items():
            tdms_file_path = os.path.join(cls.test_data_folder, f"{channel}.tdms")
            with TdmsFile.open(tdms_file_path, "w") as tdms_file:
                group = tdms_file.groups()[0]
                channel_object = group.create_channel(channel, "unit", np.dtype(type(data[0][1])))
                channel_object.write(data)

    @classmethod
    def tearDownClass(cls):
        """
        Deletes the sample TDMS files created during testing.
        """
        for file in os.listdir(cls.test_data_folder):
            os.remove(os.path.join(cls.test_data_folder, file))
        os.rmdir(cls.test_data_folder)

    def test_tdms_processing(self):
        """
        Tests the TDMS processing code by reading the sample TDMS files, cleaning and extracting the data,
        and verifying that the resulting DataFrame contains the expected data.
        """
        # Define the path of the sample TDMS files
        tdms_files_path = os.path.join(self.test_data_folder, "*.tdms")

        # Read the sample TDMS files into a PySpark DataFrame
        tdms_df = self.spark.read.format("nptdms").load(tdms_files_path)

        # Clean and extract the data from the DataFrame
        clean_data_df = tdms_df.select(
            col("channel"),
            explode(col("data")).alias("raw_data"),
        ).select(
            col("channel"),
            col("raw_data").getField("relative_time").alias("timestamp"),
            col("raw_data").getField("data").alias("value")
        ).orderBy("channel", "timestamp")

        # Verify that the resulting DataFrame contains the expected data
        expected_data = [
            ("channel1", 1, 2.0),
            ("channel1", 2, 4.0),
            ("channel1", 3, 6.0),
            ("channel2", 1, "foo"),
            ("channel2", 2, "bar"),
            ("channel2", 3, "baz")
        ]
        actual_data = [(row.channel, row.timestamp, row.value) for row in clean_data_df.collect()]
        self.assertEqual(actual_data, expected_data)
