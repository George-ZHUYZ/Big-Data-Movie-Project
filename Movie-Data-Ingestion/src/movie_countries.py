from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

import sys
sys.path.insert(1, '/home/george/Big-Data-Movie-Project/settings.py')
import settings

spark = SparkSession \
    .builder \
    .appName("Spark_movie_countries_task") \
    .getOrCreate()

df_movies = spark.read.option("header", "true").csv("../movies_metadata.csv")
df_movie_prod_countries = df_movies.select(f.col("id").alias("movie_tmdb_id"), f.col("production_countries"))
df_movie_prod_countries = df_movie_prod_countries.withColumn("production_countries",
                                                             f.from_json(f.col("production_countries"),
                                                                         "array<string>"))
df_movie_prod_countries = df_movie_prod_countries.na.drop(subset=['production_countries'])
df_movie_prod_countries = df_movie_prod_countries.withColumn("production_countries",
                                                             f.explode("production_countries"))

schema = t.StructType(
    [
        t.StructField('iso_3166_1', t.StringType(), True),
        t.StructField('name', t.StringType(), True)
    ]
)

df_movie_prod_countries = df_movie_prod_countries.withColumn('production_countries',
                                                             f.from_json('production_countries', schema)) \
    .select(f.col('movie_tmdb_id'), f.col('production_countries.iso_3166_1').alias('country_code'),
            f.col('production_countries.name').alias('country_name'))
df_movie_prod_countries = df_movie_prod_countries.na.drop('any')

df_movie_prod_countries.select(*df_movie_prod_countries.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movie_prod_countries") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()

df_prod_countries = df_movie_prod_countries.select(['country_code', 'country_name']).distinct()
df_prod_countries.sort(f.col('country_code'))

df_prod_countries.select(*df_prod_countries.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "prod_countries") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()
