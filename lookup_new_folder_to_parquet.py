from pyspark.sql import SparkSession
import os
import hashlib

spark = SparkSession.builder.getOrCreate()

root_folder = "/path/to/root/folder"
lookup_table_path = "/path/to/lookup_table.parquet"


# Function to check if the lookup table exists
def lookup_table_exists():
    return os.path.exists(lookup_table_path)


# Function to create a new lookup table
def create_lookup_table():
    # Create an empty DataFrame
    schema = StructType([
        StructField("folder_name", StringType(), nullable=False),
        StructField("parquet_file", StringType(), nullable=False),
        StructField("hash_value", StringType(), nullable=False),
        StructField("created_time", TimestampType(), nullable=False),
        StructField("modified_time", TimestampType(), nullable=False)
    ])
    empty_df = spark.createDataFrame([], schema)

    # Write the empty DataFrame as a Parquet file
    empty_df.write.mode("overwrite").parquet(lookup_table_path)


# Step 1: Get a list of all the subdirectories within the root folder
folders = [f.path for f in os.scandir(root_folder) if f.is_dir()]

# Step 2: Check if the lookup table exists and create a new one if it doesn't
if not lookup_table_exists():
    create_lookup_table()

# Step 3: Read the lookup table into a DataFrame
lookup_table = spark.read.parquet(lookup_table_path)

# Step 4: Get the list of folders that have already been converted and listed in the lookup table
converted_folders = lookup_table.select("folder_name").distinct().rdd.flatMap(lambda x: x).collect()

# Step 5: Compare the list of folders with the converted folders and identify the ones that haven't been converted yet
not_converted_folders = [folder for folder in folders if folder not in converted_folders]

# Step 6: Iterate over the not converted folders, check if they have been changed, and convert files to Parquet
for folder in not_converted_folders:
    # Check if the folder has been changed
    folder_changed = False

    # Compute a hash value for the folder
    folder_hash = hashlib.md5()

    for root, dirs, files in os.walk(folder):
        for file in files:
            # Compute a hash value for each file
            file_path = os.path.join(root, file)
            with open(file_path, "rb") as f:
                file_hash = hashlib.md5()
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)
                folder_hash.update(file_hash.hexdigest().encode())

    # Compare the computed hash value with the existing hash value in the lookup table
    folder_name = os.path.basename(folder)
    existing_row = lookup_table.filter(lookup_table.folder_name == folder_name).collect()

    if not existing_row or existing_row[0]["hash_value"] != folder_hash.hexdigest():
        folder_changed = True

    if folder_changed:
        csv_files = [f.path for f in os.scandir(folder) if f.is_file() and f.name.endswith(".csv")]
        for csv_file in csv_files:
            # Read CSV file into DataFrame
            df = spark.read.csv(csv_file, header=True, inferSchema=True)

            # Convert DataFrame to Parquet format
            parquet_file = csv_file[:-4] + ".parquet"
            df.write.parquet(parquet_file)

            # Update the lookup table
            if not existing_row:
                created_time = os.path.getctime(folder)
                modified_time = os

