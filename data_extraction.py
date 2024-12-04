import pandas as pd 
from database_utility import DatabaseConnector
import tabula

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

    def retrieve_pdf_data(self,URL):
        dfs = tabula.read_pdf(URL, pages='all', multiple_tables=True)
        concatenated_df = pd.concat(dfs, ignore_index=True)
        return concatenated_df



