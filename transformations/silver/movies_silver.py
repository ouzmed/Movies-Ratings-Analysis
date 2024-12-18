# Databricks notebook source
# DBTITLE 1,define the schema
schema = "movieID int, title string, genres string"

# COMMAND ----------

df_raw_movies = spark.read.format("csv").schema(schema).option("header","true").load("dbfs:/mnt/movies/raw_movies/movies.csv")

# COMMAND ----------

display(df_raw_movies)

# COMMAND ----------

from pyspark.sql.functions import current_date
df_movies_injest = df_raw_movies.withColumn("date_injestion", current_date())\
    .withColumnRenamed("movieId","movie_id")
display(df_movies_injest)

# COMMAND ----------

df_movies_injest.write.mode("overwrite").save("/mnt/movies/clean_movies/movies_sl")
