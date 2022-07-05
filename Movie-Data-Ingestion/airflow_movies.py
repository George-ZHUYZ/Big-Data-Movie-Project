from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta
from includes.movie_data_functions import movies_func
from includes.movie_data_functions import genres_func
from includes.movie_data_functions import prod_companies_func
from includes.movie_data_functions import prod_countries_func


args = {
    'owner': 'Airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='airflow_movies',
    default_args=args,
    schedule_interval='* * * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['airflow_movies']
)

start_op = BashOperator(
    task_id='start_op',
    bash_command='echo Movie Data Ingestion Starts!',
    dag=dag,
)

movies_table_op = PythonOperator(
    task_id='movies_table_op',
    python_callable=movies_func,
    dag=dag,
)

genres_table_op = PythonOperator(
    task_id='genres_table_op',
    python_callable=genres_func,
    dag=dag,
)

prod_companies_table_op = PythonOperator(
    task_id='prod_companies_table_op',
    python_callable=prod_companies_func,
    dag=dag,
)

prod_countries_table_op = PythonOperator(
    task_id='prod_countries_table_op',
    python_callable=prod_countries_func,
    dag=dag,
)

end_op = BashOperator(
    task_id='end_op',
    bash_command='echo Movie Data Ingestion Finishes!',
    dag=dag,
)

start_op >> movies_table_op >> [genres_table_op, prod_companies_table_op, prod_countries_table_op] >> end_op

if __name__ == "__main__":
    dag.cli()
