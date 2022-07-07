$SPARK_HOME/bin/spark-submit \
          --master spark://ZHUYANZHAO-HWPC.localdomain:7077 \
          --deploy-mode client \
          --conf "spark.executor.cores=1" \
          --conf "spark.executor.num=1" \
          --conf "spark.executor.memory=1g" \
          --conf "spark.cores.max=1" \
          --packages "mysql:mysql-connector-java:8.0.22" \
          /home/george/Big-Data-Movie-Project/src/movie_genres.py