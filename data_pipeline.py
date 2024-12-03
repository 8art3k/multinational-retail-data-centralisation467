from database_utility import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    yaml_file_path = 'db_creds.yaml'
    db_connector = DatabaseConnector(yaml_file_path)
    engine = db_connector.init_db_engine()  
    data_extractor = DataExtractor(db_connector)
    table_name = 'legacy_users' 

    legacy_user_data = data_extractor.read_rds_table(table_name, engine)
    data_cleaning = DataCleaning(legacy_user_data)
    cleaned_data = data_cleaning.clean_user_data()   

    #data now cleaned 
    #save to my server sales_data as dim_users
    yaml_file_path_target = 'db_creds_target.yaml'
    db_connector_target = DatabaseConnector(yaml_file_path_target)
    engine_target = db_connector_target.init_db_engine()

    new_table_name = 'dim_users'
    db_connector_target.upload_to_db(cleaned_data, new_table_name, engine_target, if_exists='replace')
    print(f"Data from 'legacy_users' has been cleaned and uploaded to '{new_table_name}'")

main()