import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t


def create_spark():
    findspark.init()

    return SparkSession \
        .builder \
        .appName("Spark_movies_genres_task") \
        .config("spark.master", "spark://ZHUYANZHAO-HWPC.localdomain:7077") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.22") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.num", "1") \
        .getOrCreate()


def movies_func():
    spark = create_spark()

    df_movies = spark.read.option("header", "true").csv("./includes/movies_metadata.csv")
    df_movies = df_movies.withColumnRenamed("id", "tmdb_id") \
        .withColumn('imdb_id', f.col('imdb_id').substr(f.lit(4), f.length('imdb_id'))) \
        .drop('belongs_to_collection', 'genres', 'production_companies', 'production_countries', 'spoken_languages') \
        .na.drop(subset=['tmdb_id'])

    df_movies.select(*df_movies.columns).write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movies") \
        .option("user", "root").option("password", "zhuyz928").save()


def genres_func():
    spark = create_spark()

    df_movies = spark.read.option("header", "true").csv("./includes/movies_metadata.csv")
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
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movie_genres") \
        .option("user", "root").option("password", "zhuyz928").save()

    df_genres = df_movie_genres.select(['genre_id', 'genre_name']).distinct()
    df_genres.sort(f.col('genre_id')).show(df_genres.count())

    df_genres.select(*df_genres.columns).write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "genres") \
        .option("user", "root").option("password", "zhuyz928").save()


def prod_companies_func():
    spark = create_spark()

    df_movies = spark.read.option("header", "true").csv("./includes/movies_metadata.csv")
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
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movie_prod_companies") \
        .option("user", "root").option("password", "zhuyz928").save()

    df_prod_companies = df_movie_prod_companies.select(['company_id', 'company_name']).distinct()
    df_prod_companies.sort(f.col('company_id'))

    df_prod_companies.select(*df_prod_companies.columns).write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "prod_companies") \
        .option("user", "root").option("password", "zhuyz928").save()


def prod_countries_func():
    spark = create_spark()

    df_movies = spark.read.option("header", "true").csv("./includes/movies_metadata.csv")
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
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "movie_prod_countries") \
        .option("user", "root").option("password", "zhuyz928").save()

    df_prod_countries = df_movie_prod_countries.select(['country_code', 'country_name']).distinct()
    df_prod_countries.sort(f.col('country_code'))

    df_prod_countries.select(*df_prod_countries.columns).write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/spark_movies") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "prod_countries") \
        .option("user", "root").option("password", "zhuyz928").save()
