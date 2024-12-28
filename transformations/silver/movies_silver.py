# Databricks notebook source
# MAGIC %run "/Workspace/Users/sof_ou@outlook.fr/Movies-Ratings-Analysis/transformations/setup/commun variables"

# COMMAND ----------

# define the schema for the raw_movies
def getSchema():
    schema = "movieID int, title string, genres string"
    return schema

# read the raw_movies
def readRawMovies(schema, location_path):
    from pyspark.sql.functions import current_date
    return (
        spark.read
             .format("csv")
             .schema(schema)
             .option("header", "true")
             .load(location_path)
             .withColumn("date_injestion", current_date())
             .withColumnRenamed("movieId","movie_id")
    )

# write the raw_movies
def writeRawMovies(table_name):
    df = readRawMovies(getSchema(),raw_movies_location)
    df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# execute the loading into the raw_movies delta table
writeRawMovies("movies_sl")
