# python3

import mysql.connector

from common.constants import CREATE_TABLE, SELECT_DATABASE, CREATE_DATABASE


def setup_mysql():
    my_db = _get_mysql_connection()
    cursor = my_db.cursor()
    cursor.execute(CREATE_DATABASE)
    cursor.execute(SELECT_DATABASE)
    cursor.execute(CREATE_TABLE)
    return my_db


def _get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
