import pandas as pd
import sqlite3

from src.db_handler.queries import ALL_TABLES_QUERY, ALL_TABLE_QUERY


class ExcelSQL:
    def __init__(self, file_path, default_database_connection=":memory:"):
        self.file_path = file_path
        self.df = [pd.read_excel(fp, sheet_name=None) for fp in file_path]
        self.conn = sqlite3.connect(default_database_connection)
        self.cur = self.conn.cursor()

    def create_table(self):
        for df in self.df:
            for sheet_name, d in df.items():
                d.to_sql(sheet_name, self.conn, if_exists='replace', index=False)

    def get_tables(self):
        self.cur.execute(ALL_TABLES_QUERY)
        return self.cur.fetchall()

    def get_table(self, table_name):
        self.cur.execute(ALL_TABLE_QUERY.format(table_name=table_name))
        return self.cur.fetchall()

    def close(self):
        self.conn.close()
