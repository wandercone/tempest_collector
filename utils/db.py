import os

import pymysql


def get_db_connection():
    """Open and return a new pymysql connection using environment-configured credentials."""
    return pymysql.connect(
        host=os.environ["SQL_HOST"],
        user=os.environ["SQL_USER"],
        password=os.environ["SQL_PASSWD"],
        database=os.environ["SQL_DB"],
    )
