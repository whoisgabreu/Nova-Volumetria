import psycopg2
from dotenv import load_dotenv
import os


class Db_Connection:

    def __init__(self):

        load_dotenv()
        DB_NAME = os.getenv("DB_NAME")
        USER = os.getenv("USER")
        PD = os.getenv("PD")
        HOST = os.getenv("HOST")
        PORT = os.getenv("PORT")

        self.conn = psycopg2.connect(
            dbname = DB_NAME, 
            user = USER, password = PD, 
            host = HOST, port = PORT
        )
        self.cursor = self.conn.cursor()

    def get_cursor(self):
        return self.cursor

    def get_conn(self):
        return self.conn

    def db_commit(self):
        self.conn.commit()

    def close_connections(self):
        self.conn.close()
        self.cursor.close()

    def disconnect(self):
        return self.conn.close()