import pandas as pd
from sqlalchemy import text
from database_utility import DatabaseConnector
from uuid import UUID
from data_extraction import DataExtractor

def download_df(table_name):    # this function connects to the database and downloads data from the specified table
    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine_target)
    return df

def upload_df(df, new_table_name):    
    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()
    db_connector_target.upload_to_db(df, new_table_name, engine_target, if_exists='replace')
    print(f'Data has been uploaded to the "{new_table_name}" table.')

def update_orders_table():  # downloads the 'orders_table' from the target database, updates the data types, and re-uploads it
    table_name = 'orders_table'
    orders_table_df = download_df(table_name)

    orders_table_df['date_uuid'] = orders_table_df['date_uuid'].apply(UUID)
    orders_table_df['user_uuid'] = orders_table_df['user_uuid'].apply(UUID)

    orders_table_df['card_number'] = orders_table_df['card_number'].astype(str).str[:19]  # max length of values for each column used
    orders_table_df['store_code'] = orders_table_df['store_code'].astype(str).str[:12]    # max length of values for each column used
    orders_table_df['product_code'] = orders_table_df['product_code'].astype(str).str[:11]  # max length of values for each column used
    
    orders_table_df['product_quantity'] = orders_table_df['product_quantity'].astype('int16')

    '''
    print(f"product_quantity type: {type(orders_table_df['product_quantity'].iloc[0])}")
    '''
    new_table_name = 'orders_table'
    upload_df(orders_table_df, new_table_name)

    return orders_table_df

#  update_orders_table()

def update_dim_users_table():
    table_name = 'dim_users'
    dim_users_df = download_df(table_name)

    dim_users_df = dim_users_df.dropna(subset=['join_date']) # removes 'null' values

    dim_users_df['first_name'] = dim_users_df['first_name'].astype(str).str[:255]
    dim_users_df['last_name'] = dim_users_df['last_name'].astype(str).str[:255]
    dim_users_df['date_of_birth'] = pd.to_datetime(dim_users_df['date_of_birth'], errors='coerce').dt.date
    dim_users_df['country_code'] = dim_users_df['country_code'].astype(str).str[:10]       
    dim_users_df['user_uuid'] = dim_users_df['user_uuid'].apply(UUID)
    dim_users_df['join_date'] = pd.to_datetime(dim_users_df['join_date'], errors='coerce').dt.date # strips date of time
    
    '''
    print(f"join_date type: {type(dim_users_df['join_date'].iloc[0])}")
    '''

    new_table_name  = 'dim_users'
    upload_df(dim_users_df, new_table_name)

#  update_dim_users_table()

def update_dim_store_details_table():
    table_name = 'dim_store_details'
    dim_store_details_df = download_df(table_name)

    dim_store_details_df['longitude'] = pd.to_numeric(dim_store_details_df['longitude'], errors='coerce')
    dim_store_details_df['locality'] = dim_store_details_df['locality'].astype(str).str[:255]
    dim_store_details_df['store_code'] = dim_store_details_df['store_code'].astype(str).str[:11]
    dim_store_details_df['staff_numbers'] = pd.to_numeric(dim_store_details_df['staff_numbers'], errors='coerce', downcast='integer')
    dim_store_details_df['opening_date'] = pd.to_datetime(dim_store_details_df['opening_date'], errors='coerce').dt.date
    dim_store_details_df['store_type'] = dim_store_details_df['store_type'].astype('object').str[:255]
    dim_store_details_df['latitude'] = pd.to_numeric(dim_store_details_df['latitude'], errors='coerce')
    dim_store_details_df['country_code'] = dim_store_details_df['country_code'].astype(str).str[:2]
    dim_store_details_df['continent'] = dim_store_details_df['continent'].astype(str).str[:255]
    
    '''
    print(f"continent type: {type(dim_store_details_df['continent'].iloc[0])}")
    '''
    new_table_name  = 'dim_store_details'
    upload_df(dim_store_details_df, new_table_name)

#  update_dim_store_details_table()

'''Remaining tasks were completed directly in pgAdmin4 due to column types not updating when uploaded to the databse.'''


