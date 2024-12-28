# Databricks notebook source
# MAGIC %run "/Workspace/Users/sof_ou@outlook.fr/Movies-Ratings-Analysis/transformations/setup/commun variables"

# COMMAND ----------

def getSchema():
    schema = "userId int, movieId int, rating double, timestamp int"
    return schema

def readRatings(schema):
    from pyspark.sql.functions import to_timestamp, date_format, year, month, dayofmonth, col
    return (
        spark.read
             .format("csv")
             .schema(schema)
             .option("header", "true")
             .load(raw_ratings_location)
             .withColumn("date", date_format(to_timestamp("timestamp"), "yyyy-MM-dd"))
             .withColumnRenamed("userId", "user_id")
             .withColumnRenamed("movieId", "movie_id")
             .withColumn("year", year(col("date")))
             .withColumn("month", month(col("date")))
             .withColumn("day", dayofmonth(col("date")))
             .drop("timestamp","date")

    )

def writeRatings(table_name):
    df = readRatings(getSchema())
    df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

writeRatings("ratings_sl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ratings_sl

# COMMAND ----------

# DBTITLE 1,rename the columns
# dic_rename = {
#     "userId": "user_id",
#     "movieId": "movie_id"
# }
# for old, new in dic_rename.items():
#     df_cleansed = df_cleansed.withColumnRenamed(old, new)
