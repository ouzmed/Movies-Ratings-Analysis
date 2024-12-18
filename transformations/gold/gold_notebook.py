# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_clean_movies = spark.read.table("movies_sl")
df_clean_ratings = spark.read.table("ratings_sl")
df_clean_tags = spark.read.table("tags_sl")

# COMMAND ----------

display(df_clean_tags)

# COMMAND ----------

# DBTITLE 1,Question 1
df_clean_ratings.groupBy("year").agg(count("rating").alias("NbRatings")).orderBy("year").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Question 2
df_clean_ratings.groupBy("rating").count().orderBy("rating").show(truncate=False)

# COMMAND ----------

df_join_movies_ratings = df_clean_movies.join(df_clean_ratings, on="movie_id", how="left")\
    .select(df_clean_movies.movie_id, "title","genres", "rating", "year")

# COMMAND ----------

df_join_movies_tags = df_join_movies_ratings.join(df_clean_tags, on="movie_id", how="left")\
    .select(df_clean_movies.movie_id, "title","genres", "rating", "year", "tag", "timestamp")

# COMMAND ----------

display(df_join_movies_tags)

# COMMAND ----------

df_join_movies_tags.count()

# COMMAND ----------

# DBTITLE 1,Question 3
movies_18 = df_join_movies_tags.filter((df_join_movies_tags.tag.isNull()==False) & (df_join_movies_tags.rating.isNull()==True)).select("title")
movies_18 = movies_18.dropDuplicates()
display(movies_18)

# COMMAND ----------

# DBTITLE 1,Question 4
df_rated_untaged = df_join_movies_tags.filter((df_join_movies_tags.tag.isNull()==True) & (df_join_movies_tags.rating.isNull()==False))
#display(df_rated_untaged)
df_rated_untaged.groupBy("movie_id","title").agg(avg("rating"),count("rating").alias("NbRating")).filter(col("NbRating")>30).limit(10).show()

# COMMAND ----------

display(df_rated_untaged)

# COMMAND ----------

display(df_clean_tags.groupBy("movie_id").agg(count("tag").alias("NbTag")).orderBy("NbTag", ascending=False))
display(df_clean_tags.groupBy("user_id").agg(count("tag").alias("NbTag")).orderBy("NbTag", ascending=False))

# COMMAND ----------

# DBTITLE 1,Question 5
df_avg_tag = df_join_movies_tags.groupBy("movie_id").agg(count("tag").alias("NbTag")).orderBy("NbTag", ascending=False)
display(df_avg_tag)


