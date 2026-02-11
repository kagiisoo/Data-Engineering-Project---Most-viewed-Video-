import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit, expr

# Spark session
spark = SparkSession.builder.appName("Youtube Data Engineering Pipeline").getOrCreate()
print("Spark created successfully")

dataframes = []
raw_dataPath = "data/raw data"

# Loop through CSV files only
for file in os.listdir(raw_dataPath):
    if file.endswith(".csv"):
        print("Reading file:", file)
        country = file[:2]  # first 2 letters as country code
        df = spark.read.option("header", True)\
                       .option("multiLine", True)\
                       .option("escape", '"')\
                       .option("quote", '"')\
                       .option("inferSchema", True)\
                       .csv(os.path.join(raw_dataPath, file))
        # add country column
        df = df.withColumn("country", lit(country))
        dataframes.append(df)

print("All CSV files read successfully")

# Combine all countries
df = dataframes[0]
for d in dataframes[1:]:
    df = df.unionByName(d)

print("All countries combined")

# Convert trending_date using try_to_date to avoid errors
df = df.withColumn(
    "trending_date",
    expr("try_to_date(trending_date, 'yy.dd.MM')")
)

print("Trending date converted safely")

# Remove duplicates
df = df.dropDuplicates(["video_id", "trending_date"])
print("Duplicates removed")

# Show first 5 rows
df.show(5, truncate=False)

# Save to S3 in Parquet format
df.write.mode("overwrite").parquet("data/processed_data/parquet")
print("Processed data saved")

spark.stop()
print("Spark session stopped")
