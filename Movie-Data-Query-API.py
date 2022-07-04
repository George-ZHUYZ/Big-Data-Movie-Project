from sqlalchemy import create_engine
import mysql
import pymysql
import pandas as pd
import json


def db_Connection():
    db_username = 'root'
    db_password = 'zhuyz928'
    db_hostaddress = 'localhost:3306'

    try:
        db_connection_str = 'mysql+pymysql://' + db_username + ':' + db_password + '@' + db_hostaddress + '/spark_movies'
        db_connection = create_engine(db_connection_str)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
        return None

    return db_connection


def db_query(query, my_params):
    # Create the connection to DATABASE
    my_connection = db_Connection()

    if my_connection:
        my_data = pd.read_sql(query, my_connection, params=my_params)
        print(my_data)
    else:
        print('Failed to connect the DATABASE!!!')


def parse_input(my_input):
    columns = list(my_input.keys())
    values = []

    query = """SELECT * FROM movies m
        LEFT JOIN movie_genres mg ON m.tmdb_id = mg.movie_tmdb_id
        LEFT JOIN movie_prod_companies mpc ON m.tmdb_id  = mpc.movie_tmdb_id
        LEFT JOIN movie_prod_countries mpc2 ON m.tmdb_id = mpc2.movie_tmdb_id
    """

    if len(columns) > 0:
        columns = [column + ' = %s' for column in columns]
        query += "WHERE " + " AND ".join(columns)
        values = list(my_input.values())

    db_query(query, values)


if __name__ == "__main__":
    init_input = input(
        """
        Please enter the query params, currently only support the exactly query mode.

        * The input should meet the json format, e.g. {"tmdb_id": "862"} or {"company_id": "420", "country_code": "US"}.
        """
    )

    if init_input != '':
        my_input = json.loads(init_input)

        if type(my_input) is dict:
            parse_input(my_input)
        else:
            print('Invalid input!!!')
    else:
        print('Invalid input!!!')
