import numpy as np
import pandas as pd 
import re
import requests
import tabula
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utility import DatabaseConnector

def user_data():    # extracts user data from the 'legacy_users' table, cleans it, and uploads it to the 'dim_users' table
    yaml_file_path = 'db_creds.yaml'
    db_connector = DatabaseConnector(yaml_file_path)
    engine = db_connector.init_db_engine()  
    data_extractor = DataExtractor(db_connector)
    table_name = 'legacy_users' 

    legacy_user_data = data_extractor.read_rds_table(table_name, engine)
    data_cleaning = DataCleaning(legacy_user_data)
    cleaned_data = data_cleaning.clean_user_data()   

    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    new_table_name = 'dim_users'
    db_connector_target.upload_to_db(cleaned_data, new_table_name, engine_target, if_exists='replace')
    print(f'Data from "legacy_users" has been cleaned and uploaded to "{new_table_name}"')

#  user_data()

def card_details_pdf(): # extracts card details from a PDF file, cleans the data, and uploads it to the 'dim_card_details' table
    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    pdf_URL = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
  
    data_extractor = DataExtractor(db_connector_target)
    card_data = data_extractor.retrieve_pdf_data(pdf_URL)
       
    data_cleaning = DataCleaning(card_data)
    cleaned_card_data = data_cleaning.clean_card_data()
   
    new_table_name = 'dim_card_details'
    db_connector_target.upload_to_db(cleaned_card_data, new_table_name, engine_target, if_exists='replace')
    print(f'Data from the PDF has been cleaned and uploaded to "{new_table_name}"')

#  card_details_pdf()

def store_details():    # extracts store details from an API, cleans the data, and uploads it to the 'dim_store_details' table
    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    data_extractor = DataExtractor(db_connector=None, api_key='yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX')
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    num_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint)
    stores_df = data_extractor.retrieve_stores_data(store_endpoint, num_of_stores)

    data_cleaning = DataCleaning(stores_df)
    cleaned_stores_df = data_cleaning.clean_store_data()

    new_table_name = 'dim_store_details'
    db_connector_target.upload_to_db(cleaned_stores_df, new_table_name, engine_target, if_exists='replace')
    print(f'Data from the store API has been cleaned and uploaded to "{new_table_name}"')

# store_details()
   
def product_data(): # extracts product data from an S3 bucket, cleans the data, and uploads it to the 'dim_products' table
    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    s3_url = 's3://data-handling-public/products.csv'
    data_extractor = DataExtractor(db_connector=None, api_key=None)  
    product_data_df = data_extractor.extract_from_s3(s3_url)

    data_cleaning = DataCleaning(product_data_df)  
    cleaned_product_data = data_cleaning.clean_products_data()

    new_table_name = 'dim_products'
    db_connector_target.upload_to_db(cleaned_product_data, new_table_name, engine_target, if_exists='replace')
    print(f'Data from S3 has been cleaned and uploaded to "{new_table_name}"')

# product_data()

def product_orders_data():  # extracts product orders data from the 'orders_table', cleans it, and uploads it to the 'orders_table' table
    yaml_file_path = 'db_creds.yaml'  
    db_connector = DatabaseConnector(yaml_file_path)
    engine = db_connector.init_db_engine()       
    data_extractor = DataExtractor(db_connector, api_key=None)
    table_name = 'orders_table'
    orders_table_df = data_extractor.read_rds_table(table_name, engine)
  
    data_cleaning = DataCleaning(orders_table_df)
    cleaned_orders_data = data_cleaning.clean_orders_data()

    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    new_table_name = 'orders_table'
    db_connector.upload_to_db(cleaned_orders_data, new_table_name, engine_target, if_exists='replace')
    print(f'Orders data cleaned and uploaded to "{new_table_name}".')

#  product_orders_data()

def sales_data():   # extracts sales data from an S3 URL (JSON format), cleans it, and uploads it to the 'dim_date_times' table
    s3_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    db_connector = None 
    api_key = None 
    data_extractor = DataExtractor(db_connector, api_key)
    sales_df = data_extractor.extract_from_s3_to_json(s3_url) 

    data_cleaning = DataCleaning(sales_df)
    cleaned_df = data_cleaning.clean_sales_data()

    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    new_table_name = 'dim_date_times'
    db_connector_target.upload_to_db(cleaned_df, new_table_name, engine_target, if_exists='replace')
    print(f'Sales data uploaded to "{new_table_name}"')

#  sales_data()
    
