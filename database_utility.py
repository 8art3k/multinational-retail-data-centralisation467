import pandas as pd 
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:    # class which handles database connections
    def __init__(self, yaml_file):      # initializes the DatabaseConnector with YAML file containing credentials as a parameter
            self.yaml_file = yaml_file

    def read_db_creds(self):    # loads database credentials from the provided YAML file
        with open(self.yaml_file,'r') as file:
            creds = yaml.safe_load(file)
        return creds
            
    def init_db_engine(self):   # initializes and returns SQLAlchemy engine connected to the database using the credentials from the YAML file
        db_creds = self.read_db_creds()
        user = db_creds.get('RDS_USER')
        password = db_creds.get('RDS_PASSWORD')
        host = db_creds.get('RDS_HOST')
        port = db_creds.get('RDS_PORT')
        database = db_creds.get('RDS_DATABASE')
        
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        return engine
    
    def list_db_tables(self,engine):    # returns a list of all tables from the database
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    def upload_to_db(self, df, table_name, engine, if_exists='replace'):    # uploads a DataFrame to a specified table in the database
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f'Data uploaded to {table_name}')

    def read_from_db(self, query, engine):  # executes an SQL query on the database
        return pd.read_sql(query, engine)       

