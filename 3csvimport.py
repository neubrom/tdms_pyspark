from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, regexp_replace

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file
df = spark.read.csv("your_file.csv", header=True, inferSchema=True)

# Find the indices where the blocks start (empty lines)
block_indices = [index for index, row in enumerate(df.collect()) if row[0] is None]

# Process Block 2
block2_start = block_indices[1] + 2  # Start index of Block 2
block2_end = block_indices[2] - 1  # End index of Block 2
df_block2 = df.select("_c0", "_c1", "_c2").na.drop().filter(f"index >= {block2_start} and index <= {block2_end}")
df_block2 = df_block2.withColumn("NewColumn", lit(100))

# Process Block 3
block3_start = block_indices[2] + 2  # Start index of Block 3
block3_end = df.count() - 1  # End index of Block 3
df_block3 = df.select("_c0", "_c1", "_c2").na.drop().filter(f"index >= {block3_start} and index <= {block3_end}")

# Remove prefixes and suffixes from column names in Block 3
df_block3 = df_block3.withColumnRenamed("_c0", regexp_replace("_c0", "^1\\.", "").alias("1.2a"))
df_block3 = df_block3.withColumnRenamed("_c1", regexp_replace("_c1", "^1\\.", "").alias("1.2b"))
df_block3 = df_block3.withColumnRenamed("_c2", regexp_replace("_c2", "^1\\.", "").alias("1.2c"))
df_block3 = df_block3.withColumn("NewColumn", lit(1000))

# Process Block 1
block1_start = 1  # Start index of Block 1
block1_end = block_indices[0] - 1  # End index of Block 1
df_block1 = df.select("_c0", "_c1", "_c2").na.drop().filter(f"index >= {block1_start} and index <= {block1_end}")
df_block1 = df_block1.withColumn("NewColumn", df_block1["_c1"] + df_block1["_c2"])

# Union all blocks
final_df = df_block2.union(df_block3).union(df_block1)

