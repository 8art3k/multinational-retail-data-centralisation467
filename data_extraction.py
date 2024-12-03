import pandas as pd 
from database_utility import DatabaseConnector

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector
      
    def read_rds_table(self, table_name, engine):
        tables = self.db_connector.list_db_tables(engine)
        if table_name in tables:
            query = f'SELECT * FROM {table_name};'
            data = pd.read_sql(query, engine)
            df = pd.DataFrame(data)
            return df
        else:
            raise ValueError(f'Table {table_name} not found.')



