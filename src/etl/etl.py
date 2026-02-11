from pyspark.sql import SparkSession

#initializing spark
spark = SparkSession.builder.appName("Youtube Analysis ETL").getOrCreate() 

df = spark.read.option("header", "true").csv("s3://kagiso-youtube-raw-data/csv")
#read all csv files from s3 folder


df_transformed = df.select("video_id", "title", "channel_title", "category_id", "views", "likes", "dislikes", "country")
#selecting only the required columns

df.filtered.write.mode("overwrite").parquet("s3://kagiso-youtube-processed-data/parquet")
#writing the transformed data back to s3 in parquet format