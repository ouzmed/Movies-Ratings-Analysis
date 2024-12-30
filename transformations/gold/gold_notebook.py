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
from pyspark.sql.functions import count
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
from pyspark.sql.functions import col, avg, count, round
df_rated_untaged = df_join_movies_tags.filter((df_join_movies_tags.tag.isNull()==True) & (df_join_movies_tags.rating.isNull()==False))
#display(df_rated_untaged)
df_rated_untaged.groupBy("movie_id","title").agg(round(avg("rating").alias("avg_rating"),2),count("rating").alias("nb_rating")).filter(col("nb_rating")>30).limit(10).show()

# COMMAND ----------

display(df_rated_untaged)
display(df_rated_untaged.count())

# COMMAND ----------

# DBTITLE 1,Question 5
df_avg_tag = df_join_movies_tags.groupBy("movie_id").agg(count("tag").alias("NbTag")).orderBy("NbTag", ascending=False)
display(df_avg_tag)
display(df_clean_tags.groupBy("user_id").agg(count("tag").alias("NbTag")).orderBy("NbTag", ascending=False))


# COMMAND ----------

display(df_clean_movies.head(1))
#display(df_clean_ratings.head(1))
display(df_clean_tags.head(1))

# COMMAND ----------

# DBTITLE 1,Question 6
#users who taged movies without rateing them
display(df_clean_tags.join(df_clean_ratings, on="movie_id", how="left").filter(col("rating").isNull()).select("tags_sl.user_id").distinct())

# COMMAND ----------

# DBTITLE 1,Question 7
display(df_join_movies_ratings.groupBy("genres").agg(count("rating").alias("count_ratings")).orderBy(col("count_ratings").desc()).limit(5))
print("the most genre rated is Comedy")

# COMMAND ----------

# DBTITLE 1,Question 8
display(df_clean_movies.join(df_clean_tags, on="movie_id", how="inner").groupBy("genres").agg(count("tag").alias("count_tag")).orderBy(col("count_tag").desc()).limit(5))
print("the most genre tagged is Drama")

# COMMAND ----------

# DBTITLE 1,Question 9
display(df_clean_movies.join(df_clean_tags, on="movie_id", how="inner").groupBy("title").agg(count("tag").alias("nb_tag")).orderBy(col("nb_tag").desc()).limit(1))
#display(df_clean_movies.filter(df_clean_movies.movie_id==296).select("title"))

# COMMAND ----------

# DBTITLE 1,Question 10
# top 10 movies in terms of average rating
df_q10 = df_clean_movies.join(df_clean_ratings, on="movie_id", how="inner").select("movie_id", "title", "user_id", "rating")
display(df_q10.groupBy("title").agg(count("user_id").alias("NbUser"), round(avg("rating"), 2).alias("avg_rating")).filter(col("NbUser") > 30).orderBy(col("avg_rating").desc()).select("title").limit(10))
