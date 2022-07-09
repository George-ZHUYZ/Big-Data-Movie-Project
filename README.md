# Big-Data-Movie-Project

It's a practice project to create a movie data pipeline and provide a tool to query any movie information as wishes.

## Description

Having the raw dataset of Movies, create a database and do a meticulous design with a snowflake schema. Also by the Apache Spark and Airflow tools, build a data pipeline from the data source to the data warehouse. Meanwhile, offer a simple CLI command tool with an integrated API to allow users to query all kinds of movie information as they wish.

## Getting Started

### Download the dataset of Movies

```
curl -O https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset
```

### Create a new database as the schema

![alt text](https://github.com/George-ZHUYZ/Big-Data-Movie-Project/blob/main/Moive_Project_ERD.png)

### Dependencies

* Any OS (Widnows, Unbuntu, MacOS and etc.) with Python 3.6+ installed.
* PowserShell for Windows, Bash Shell for Unbuntu and MacOS.

### Installing

* Install MySQL with APT

```
sudo apt-get update
sudo apt-get install mysql-server
```

* Install Apache Spark

```
wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
tar xvf spark-3.2.1-bin-hadoop3.2.tgz
sudo mv spark-3.2.1-bin-hadoop3.2/ /opt/spark 
```

* Install PySpark

```
pip install pyspark
```


* Install Apache Airflow

```
pip install apache-airflow
```

### Executing program

* Movies data ingestion

```
1. Put the dag file into the correct directory, e.g. /home/user_name/airflow/dags
2. Start airflow services: airflow webserver -p 8088 && airflow schedule
3. Access localhost:8088 to run the dag file
```

* Query the movie data

```
python ./Movie-Data-Query-API.py
```
