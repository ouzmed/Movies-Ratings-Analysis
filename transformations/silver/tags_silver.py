# Databricks notebook source
# MAGIC %run "/Workspace/Users/sof_ou@outlook.fr/Movies-Ratings-Analysis/transformations/setup/commun variables"

# COMMAND ----------

def getSchema():
    schema = "userId int, movieId int,tag string, timestamp int"
    return schema

def readTags(schema, location):
    from pyspark.sql.functions import col, date_format
    return (
        spark.read
             .format("csv")
             .schema(schema)
             .option("header", "true")
             .load(location)
             .withColumn("timestamp", date_format(col("timestamp").cast("timestamp"), "yyyy-MM-dd"))
             .withColumnRenamed("userId", "user_id")
             .withColumnRenamed("movieId", "movie_id")
    )

def writeTags(table_name):

    df = readTags(getSchema(), raw_tags_location)
    df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

writeTags("tags_sl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tags_sl
