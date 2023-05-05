# Import necessary libraries
from pyspark.sql.functions import *
from nptdms import TdmsFile  # library for reading tdms files
import os

# Read the list of labels to keep
labels_to_keep_path = "/path/to/labels_to_keep.txt"
with open(labels_to_keep_path, "r") as f:
    labels_to_keep = [line.strip() for line in f.readlines()]

# Define the path of the root folder that contains the TDMS files
root_folder_path = "/mnt/data/"  # replace with your root folder path

# Define the path of the folder where the Parquet files will be saved
output_folder_path = "/mnt/output/"  # replace with your output folder path

# Get a list of all files in the root folder and its subfolders
file_paths = []
for root, dirs, files in os.walk(root_folder_path):
    for file in files:
        if file.endswith(".tdms"):
            file_paths.append(os.path.join(root, file))

# Loop through each file path and check if the corresponding Parquet file exists
for file_path in file_paths:
    parquet_file_path = os.path.join(output_folder_path, os.path.basename(file_path) + ".parquet")
    if not os.path.exists(parquet_file_path):
        # Read the TDMS file into a TdmsFile object
        tdms_file = TdmsFile(file_path)

        # Get all the channels from the TDMS file
        all_channels = tdms_file.groups()[0].channels()

        # Extract the data from each channel and create a dictionary with channel name as key and data as value
        channel_data_dict = {}
        for channel in all_channels:
            channel_data = tdms_file.object().read_data(channel)
            channel_data_dict[channel.name] = channel_data

        # Convert the dictionary to a PySpark DataFrame
        data_df = spark.createDataFrame(list(channel_data_dict.items()), ["channel", "data"])

        # Clean and extract the data from the DataFrame
        clean_data_df = tdms_df.select(
            col("channel"),
            explode(col("data")).alias("raw_data"),
        ).select(
            col("channel"),
            col("raw_data").getField("relative_time").alias("timestamp"),
            col("raw_data").getField("data").alias("value")
        ).orderBy("channel", "timestamp").filter(
            col("channel").isin(labels_to_keep)
        )

       # Save the cleaned data to a Parquet file
        clean_data_df.write.mode("overwrite").parquet(parquet_file_path)

        # Show the first few rows of the cleaned data
        clean_data_df.show(10)
    else:
        print(f"File {file_path} already processed and saved as a Parquet file.")
