from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import sys
sys.path.insert(1, '/home/george/Big-Data-Movie-Project/settings.py')
import settings

spark = SparkSession \
    .builder \
    .appName("Spark_movies_task") \
    .getOrCreate()

df_movies = spark.read.option("header", "true") \
    .csv("/home/george/Big-Data-Movie-Project/Movie-Data-Ingestion/movies_metadata.csv")
df_movies = df_movies.withColumnRenamed("id", "tmdb_id") \
    .withColumn('imdb_id', f.col('imdb_id').substr(f.lit(4), f.length('imdb_id'))) \
    .drop('belongs_to_collection', 'genres', 'production_companies', 'production_countries', 'spoken_languages') \
    .na.drop(subset=['tmdb_id'])

df_movies.select(*df_movies.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movies") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()
