import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd 

class DatabaseConnector:
    def __init__(self, yaml_file):
            self.yaml_file = yaml_file

    def read_db_creds(self):
        with open(self.yaml_file,'r') as file:
            creds = yaml.safe_load(file)
        return creds
            
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        user = db_creds.get('RDS_USER')
        password = db_creds.get('RDS_PASSWORD')
        host = db_creds.get('RDS_HOST')
        port = db_creds.get('RDS_PORT')
        database = db_creds.get('RDS_DATABASE')
        
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        return engine
    
    def list_db_tables(self,engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    def upload_to_db(self, df, table_name, engine, if_exists='replace'):
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f'Data uploaded to {table_name}')

    def read_from_db(self, query, engine):
        return pd.read_sql(query, engine)       

