import pandas as pd 
import numpy as np
from database_utility import DatabaseConnector
import tabula
import requests
import boto3

class DataExtractor:
    def __init__(self, db_connector=None, api_key=None): 
        self.db_connector = db_connector
        self.api_key = api_key
        self.headers = {'x-api-key': self.api_key} if self.api_key else None
                  
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

    def list_number_of_stores(self, number_of_stores_endpoint):
        response = requests.get(number_of_stores_endpoint, headers=self.headers)   
        if response.status_code == 200:
            data = response.json()  
            print('Response data:', data)
            return data.get('number_stores', 0)  
        else:
            print(f'Failed with a status code: {response.status_code}')
            return None 

    def retrieve_stores_data(self, store_endpoint, num_of_stores):
        stores_data = []  
        for store_number in range(num_of_stores):
            store_url = store_endpoint.format(store_number=store_number) 
            response = requests.get(store_url, headers=self.headers)
            
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data) 
            else:
                print(f'Failed to retrieve store {store_number} with a status code: {response.status_code}')
 
        stores_df = pd.DataFrame(stores_data)
        return stores_df

    def extract_from_s3(self, s3_url):
        s3 = boto3.client('s3')
        bucket_name = s3_url.split('/')[2]
        file_key = '/'.join(s3_url.split('/')[3:])
        local_file_path = 'C:/Users/Wupees/AiCore/products.csv'
        s3.download_file(bucket_name, file_key, local_file_path)
        df = pd.read_csv(local_file_path)
        return df
     
    def  extract_from_s3_to_json(self, s3_url):
        s3 = boto3.client('s3')
        bucket_name = s3_url.split('/')[2].split('.')[0]
        file_key = '/'.join(s3_url.split('/')[3:])
        file = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_json(file['Body'])  
        return df
