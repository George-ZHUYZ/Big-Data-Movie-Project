import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='airflow_movies',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:
    start_op = BashOperator(
        task_id='start_op',
        bash_command='echo "Movie Data Ingestion Starts!"',
    )

    movies_table_op = BashOperator(
        task_id='movies_table_op',
        bash_command='bash ~/Big-Data-Movie-Ingestion/script/movies.sh',
    )

    genres_table_op = BashOperator(
        task_id='genres_table_op',
        bash_command='bash ~/Big-Data-Movie-Ingestion/script/movie_genres.sh',
    )

    companies_table_op = BashOperator(
        task_id='companies_table_op',
        bash_command='bash ~/Big-Data-Movie-Ingestion/script/movie_companies.sh',
    )

    countries_table_op = BashOperator(
        task_id='countries_table_op',
        bash_command='bash ~/Big-Data-Movie-Ingestion/script/movie_countries.sh',
    )

    end_op = BashOperator(
        task_id='end_op',
        bash_command='echo "Movie Data Ingestion Finishes!"',
    )

    start_op >> movies_table_op >> [genres_table_op, companies_table_op, countries_table_op] >> end_op

if __name__ == "__main__":
    dag.cli()
