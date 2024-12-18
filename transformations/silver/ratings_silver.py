# Databricks notebook source
from pyspark.sql.functions import to_timestamp, date_format, year, month, dayofmonth, col

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

schema = "userId int, movieId int, rating double, timestamp int"

# COMMAND ----------

df_rating = spark.read.format("csv").schema(schema).option("header", "true").load("dbfs:/mnt/movies/raw_ratings/ratings.csv")

# COMMAND ----------

display(df_rating)

# COMMAND ----------

# DBTITLE 1,formating the timestamp column
df_cleansed = df_rating.withColumn("date", date_format(to_timestamp("timestamp"), "yyyy-MM-dd"))\
    .drop("timestamp")

# COMMAND ----------

# DBTITLE 1,rename the columns
dic_rename = {
    "userId": "user_id",
    "movieId": "movie_id"
}
for old, new in dic_rename.items():
    df_cleansed = df_cleansed.withColumnRenamed(old, new)

# COMMAND ----------

# DBTITLE 1,adding the year column
df_final = df_cleansed.withColumn("year", year(col("date")))\
    .withColumn("month", month(col("date")))\
    .withColumn("day", dayofmonth(col("date")))\
        .drop("date")

# COMMAND ----------

display(df_final)

# COMMAND ----------

df_final.write.mode("overwrite").save("/mnt/movies/clean_ratings/ratings_sl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ratings_sl
