# Databricks notebook source
from pyspark.sql.functions import col, date_format

# COMMAND ----------

schema = "userId int, movieId int,tag string, timestamp int"

# COMMAND ----------

df_tags_raw = spark.read.format("csv").schema(schema).option("header", "true").load("dbfs:/mnt/movies/raw_tags/dbo.tags1.csv")

# COMMAND ----------

df_formated = df_tags_raw.withColumn("timestamp", date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd"))\
    .withColumnRenamed("userId", "user_id")\
    .withColumnRenamed("movieId", "movie_id")

# COMMAND ----------

display(df_formated)

# COMMAND ----------

df_formated.write.mode("overwrite").save("/mnt/movies/clean_tags/tags_sl")
