from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

import sys
sys.path.insert(1, '/home/george/Big-Data-Movie-Project/settings.py')
import settings

spark = SparkSession \
    .builder \
    .appName("Spark_movie_genres_task") \
    .getOrCreate()

df_movies = spark.read.option("header", "true") \
    .csv("/home/george/Big-Data-Movie-Project/Movie-Data-Ingestion/movies_metadata.csv")
df_movie_genres = df_movies.select(f.col("id").alias("movie_tmdb_id"), f.col("genres"))
df_movie_genres = df_movie_genres.withColumn("genres", f.from_json(f.col("genres"), "array<string>"))
df_movie_genres = df_movie_genres.withColumn("genres", f.explode("genres"))

schema = t.StructType(
    [
        t.StructField('id', t.StringType(), True),
        t.StructField('name', t.StringType(), True)
    ]
)
df_movie_genres = df_movie_genres.withColumn('genres', f.from_json('genres', schema)) \
    .select(f.col('movie_tmdb_id').cast('int'), f.col('genres.id').cast('int').alias('genre_id'),
            f.col('genres.name').alias('gener_name'))
df_movie_genres = df_movie_genres.na.drop('any')

df_movie_genres.select(*df_movie_genres.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movie_genres") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()

df_genres = df_movie_genres.select(['genre_id', 'genre_name']).distinct()
df_genres.sort(f.col('genre_id')).show(df_genres.count())

df_genres.select(*df_genres.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "genres") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()
