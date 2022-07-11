from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

import sys
sys.path.insert(1, '/home/george/Big-Data-Movie-Project/settings.py')
import settings

spark = SparkSession \
    .builder \
    .appName("Spark_movie_companies_task") \
    .getOrCreate()

df_movies = spark.read.option("header", "true") \
    .csv("/home/george/Big-Data-Movie-Project/Movie-Data-Ingestion/movies_metadata.csv")
df_movie_prod_companies = df_movies.select(f.col("id").alias("movie_tmdb_id"), f.col("production_companies"))
df_movie_prod_companies = df_movie_prod_companies.withColumn("production_companies",
                                                             f.from_json(f.col("production_companies"),
                                                                         "array<string>"))
df_movie_prod_companies = df_movie_prod_companies.na.drop(subset=['production_companies'])
df_movie_prod_companies = df_movie_prod_companies.withColumn("production_companies",
                                                             f.explode("production_companies"))

schema = t.StructType(
    [
        t.StructField('id', t.StringType(), True),
        t.StructField('name', t.StringType(), True)
    ]
)

df_movie_prod_companies = df_movie_prod_companies.withColumn('production_companies',
                                                             f.from_json('production_companies', schema)) \
    .select(f.col('movie_tmdb_id'), f.col('production_companies.id').alias('company_id'),
            f.col('production_companies.name').alias('company_name'))
df_movie_prod_companies = df_movie_prod_companies.na.drop('any')

df_movie_prod_companies.select(*df_movie_prod_companies.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movie_prod_companies") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()

df_prod_companies = df_movie_prod_companies.select(['company_id', 'company_name']).distinct()
df_prod_companies.sort(f.col('company_id'))

df_prod_companies.select(*df_prod_companies.columns).write.format("jdbc") \
    .option("url", "jdbc:mysql://" + settings.DB_Host + "/spark_movies") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "prod_companies") \
    .option("user", settings.DB_Username).option("password", settings.DB_Password).save()
