from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

root_folder = "/path/to/root/folder"
lookup_table_path = "/path/to/lookup_table.parquet"

# Step 1: Get a list of all the subdirectories within the root folder
folders = [f.path for f in os.scandir(root_folder) if f.is_dir()]

# Step 2: Read the lookup table into a DataFrame
lookup_table = spark.read.parquet(lookup_table_path)

# Step 3: Get the list of folders that have already been converted and listed in the lookup table
converted_folders = lookup_table.select("folder_name").distinct().rdd.flatMap(lambda x: x).collect()

# Step 4: Compare the list of folders with the converted folders and identify the ones that haven't been converted yet
not_converted_folders = [folder for folder in folders if folder not in converted_folders]

# Step 5: Iterate over the not converted folders, read CSV files, and convert to Parquet
for folder in not_converted_folders:
    csv_files = [f.path for f in os.scandir(folder) if f.is_file() and f.name.endswith(".csv")]
    for csv_file in csv_files:
        # Read CSV file into DataFrame
        df = spark.read.csv(csv_file, header=True, inferSchema=True)

        # Convert DataFrame to Parquet format
        parquet_file = csv_file[:-4] + ".parquet"
        df.write.parquet(parquet_file)

        # Update the lookup table
        folder_name = os.path.basename(folder)
        lookup_table = lookup_table.union(
            spark.createDataFrame([(folder_name, parquet_file)], ["folder_name", "parquet_file"]))
        lookup_table.write.mode("overwrite").parquet(lookup_table_path)
