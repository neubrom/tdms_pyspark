import os
import shutil
from pyspark.sql import SparkSession

# Define the SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

# Define the root folder for test files
test_root_folder_path = "./test_data/"

# Define the output folder for test files
test_output_folder_path = "./test_output/"

def test_tdms_to_parquet():
    # Create the test root folder and files
    os.makedirs(test_root_folder_path, exist_ok=True)
    os.makedirs(test_output_folder_path, exist_ok=True)
    tdms_file_path = os.path.join(test_root_folder_path, "test.tdms")
    with TdmsFile(tdms_file_path, "w") as tdms_file:
        group = tdms_file.create_group("Group1")
        channel1 = group.create_channel("Channel1", "Unit1", float64)
        channel2 = group.create_channel("Channel2", "Unit2", float64)
        data1 = [1.0, 2.0, 3.0]
        data2 = [4.0, 5.0, 6.0]
        channel1.write(data1)
        channel2.write(data2)

    # Create the test labels to keep file
    labels_to_keep_path = os.path.join(test_root_folder_path, "labels_to_keep.txt")
    with open(labels_to_keep_path, "w") as f:
        f.write("Channel1\n")

    # Test the function
    tdms_to_parquet(labels_to_keep_path, test_root_folder_path, test_output_folder_path, spark)

    # Check that the Parquet file was created
    assert os.path.exists(os.path.join(test_output_folder_path, "test.tdms.parquet"))

    # Read the Parquet file and check that only the data from Channel1 is present
    data_df = spark.read.parquet(os.path.join(test_output_folder_path, "test.tdms.parquet"))
    assert data_df.count() == 3  # 3 rows of data for Channel1 only
    assert data_df.where("channel = 'Channel1'").count() == 3
    assert data_df.where("channel = 'Channel2'").count() == 0

    # Delete the test files and folders
    shutil.rmtree(test_root_folder_path)
    shutil.rmtree(test_output_folder_path)
