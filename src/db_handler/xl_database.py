import pandas as pd
import sqlite3
import warnings

from src.db_handler.queries import ALL_TABLES_QUERY, ALL_TABLE_QUERY
from datetime import datetime, timedelta


class ExcelSQL:
    def __init__(self, file_path, default_database_connection=":memory:"):
        self.file_path = file_path
        self.df = [pd.read_excel(fp, sheet_name=None) for fp in file_path]
        self.conn = sqlite3.connect(default_database_connection)
        self.cur = self.conn.cursor()

    def filter_sql(self, cutoff_date):
        """Filter SQL database by date cutoff. Loads all tables, filters/adjusts them, then writes back."""
        # Parse cutoff_date - handle DD.MM.YYYY format
        if isinstance(cutoff_date, str):
            # Try DD.MM.YYYY format first
            try:
                cutoff_date = datetime.strptime(cutoff_date, "%d.%m.%Y")
            except ValueError:
                try:
                    cutoff_date = datetime.strptime(cutoff_date, "%d-%m-%Y")
                except ValueError:
                    cutoff_date = pd.to_datetime(cutoff_date)
        cutoff_date = pd.to_datetime(cutoff_date)
        
        # Get all table names
        tables = self.get_tables()
        table_names = [table[0] for table in tables]
        
        for table_name in table_names:
            # Read table from SQL
            d = pd.read_sql_query(f'SELECT * FROM "{table_name}"', self.conn)
            
            # Handle PositionLevel table specially - adjust instead of filtering
            if table_name == "PositionLevel" and 'startLevel' in d.columns and 'levelId' in d.columns:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    d['startLevel'] = pd.to_datetime(d['startLevel'], errors='coerce')
                    mask_to_adjust = d['startLevel'].notna() & (d['startLevel'] >= cutoff_date)
                    d.loc[mask_to_adjust, 'levelId'] = d.loc[mask_to_adjust, 'levelId'] - 1
                    d.loc[mask_to_adjust, 'startLevel'] = cutoff_date - timedelta(days=1)
            else:
                # Filter other tables (exclude T1-T5 from date filtering)
                mask = pd.Series([True] * len(d), index=d.index)
                exclude_cols = {'T1', 'T2', 'T3', 'T4', 'T5'}
                for col in d.columns:
                    if col in exclude_cols:
                        continue
                    if pd.api.types.is_datetime64_any_dtype(d[col]):
                        mask = mask & (d[col].isna() | (d[col] < cutoff_date))
                    elif d[col].dtype == 'object':
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            test = pd.to_datetime(d[col], errors='coerce')
                            if test.notna().any():
                                # Exclude time-only: check if dates vary (not all same date)
                                if len(test[test.notna()].dt.date.unique()) > 1:
                                    mask = mask & (test.isna() | (test < cutoff_date))
                d = d[mask]
            
            # Write filtered/adjusted data back to SQL
            d.to_sql(table_name, self.conn, if_exists='replace', index=False)

    def create_table(self, date_filter = None):
        for df in self.df:
            for sheet_name, d in df.items():
                if sheet_name == "History_bots":
                    d['date'] = d['date'].apply(lambda date_str: datetime.strptime(date_str, "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"))
                d.to_sql(sheet_name, self.conn, if_exists='replace', index=False)
        if date_filter is not None:
            self.filter_sql(date_filter)

    def get_tables(self):
        self.cur.execute(ALL_TABLES_QUERY)
        return self.cur.fetchall()

    def get_table(self, table_name):
        self.cur.execute(ALL_TABLE_QUERY.format(table_name=table_name))
        return self.cur.fetchall()

    def close(self):
        self.conn.close()