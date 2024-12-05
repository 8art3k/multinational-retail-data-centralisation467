from database_utility import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import tabula
import requests
import pandas as pd 

def user_data():
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

# user_data()

def card_details_pdf():
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

# card_details_pdf()

def store_details():
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
    print(cleaned_stores_df.info())

    new_table_name = 'dim_store_details'
    db_connector_target.upload_to_db(cleaned_stores_df, new_table_name, engine_target, if_exists='replace')
    print(f'Data from the store API has been cleaned and uploaded to "{new_table_name}"')

# store_details()
   
  