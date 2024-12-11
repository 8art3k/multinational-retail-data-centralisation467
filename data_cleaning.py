import numpy as np
import pandas as pd
import re

class DataCleaning:     # class for data cleaning
    def __init__(self,df):   # initializes the DataCleaning clas with a DataFrame
        self.df = df
        
    def clean_user_data(self):  # cleans the user data by replacing 'NULL' with NaN, droppping rows with NaN values, convers the 'join_date' column to a datetime format
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce')
        return self.df
    
    def clean_card_data(self):  # cleans the card data by replacing 'NULL' with NaN, dropping rows with NaN values, removes duplicate rows based on 'card_number', filters for valid numeric 'card_number' values, and converts 'date_payment_confirmed' column to a datetime format
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df.drop_duplicates(subset=['card_number'], keep='first',inplace=True)
        self.df = self.df[self.df['card_number'].apply(lambda character: str(character).isdigit())]
        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], errors='coerce')
        return self.df
    
    def clean_store_data(self): # cleans the store data by removing the 'lat' column, replacing 'NULL' with NaN, converting the 'opening_date' column to a datetime format, filtering out rows with invalid 'opening_date', cleaning 'staff_numbers' by removing non-digit characters, and normalizing the 'continent' column
        self.df.drop(columns=['lat'], inplace=True) # drop 'lat' column as mostly empty
        self.df.replace('NULL', np.nan, inplace=True)
        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce')     
        self.df = self.df[self.df['opening_date'].notna()] 
        self.df.dropna(inplace=True)
        self.df.replace('NULL', np.nan, inplace=True)
        self.df['staff_numbers'] = self.df['staff_numbers'].astype(str).str.replace(r'\D', '', regex=True) # remove non-digit characters
        self.df['continent'] = self.df['continent'].str.strip() # strip white spaces
        self.df['continent'] = self.df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        return self.df
       
    def convert_product_weights(self):  # converts product weight values to a consistent format in kilograms (kg)
        for row, weight in self.df['weight'].items():
            if pd.isna(weight):
                continue 

            if 'kg' in weight:
                weight_value = re.sub(r'[^\d\.]', '', weight)  # Remove non-numeric characters
                if weight_value.replace('.', '', 1).isdigit(): # Check if value is a float
                    self.df.loc[row, 'weight'] = float(weight_value)  # Convert to float and assign
                else:
                    self.df.loc[row, 'weight'] = None  # Set to None if not a valid float
                continue
        
            match = re.match(r'(\d+)\s*x\s*(\d+)(g|ml)', str(weight)) # check if weight represented as multiplication
            if match:
                multiplier = float(match.group(1))  # Extract multiplier 
                unit_weight = float(match.group(2))  # Extract unit weight 
                unit = match.group(3)  # Extract the unit measurement
                total_weight = multiplier * unit_weight  
                if unit == 'g':
                    self.df.loc[row, 'weight'] = total_weight / 1000  
                elif unit == 'ml':
                    self.df.loc[row, 'weight'] = total_weight / 1000  
            else:
                weight = re.sub(r'[^\d\.]', '', str(weight))  # Remove non-numeric characters
                if weight.replace('.', '', 1).isdigit(): # Check if value is a float
                    weight = float(weight)
                    self.df.loc[row, 'weight'] = weight / 1000  # Convert to kg (for g or ml)
                else:
                    self.df.loc[row, 'weight'] = None  
        return self.df
    
    def clean_products_data(self):  # cleans the products data by replacing 'NULL' with NaN, dropping rows with NaN values, and converting the 'weight' column to a consistent format in kilograms
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df = self.convert_product_weights()
        return self.df
    
    def clean_orders_data(self):    # cleans the orders data by dropping 'first_name', 'last_name', '1' columns
        self.df.drop(columns=['first_name'], inplace=True)
        self.df.drop(columns=['last_name'], inplace=True) 
        self.df.drop(columns=['1'], inplace=True)   
        return self.df
    
    def clean_sales_data(self): # cleans the sales data by filtering rows based on valid time periods
        valid_time_periods = ['Morning', 'Late_Hours', 'Midday', 'Evening']
        self.df = self.df[self.df['time_period'].isin(valid_time_periods)]
        return self.df