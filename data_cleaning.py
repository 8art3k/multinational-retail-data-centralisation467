import pandas as pd
import numpy as np
import re

class DataCleaning:
    def __init__(self,df):   
        self.df = df
        
    def clean_user_data(self):
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce')
        return self.df
    
    def clean_card_data(self):
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df.drop_duplicates(subset=['card_number'], keep='first',inplace=True)
        self.df[self.df['card_number'].apply(lambda character: str(character).isdigit())]
        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], errors='coerce')
        return self.df
    
    def clean_store_data(self):
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
       
    def convert_product_weights(self):
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
    
    def clean_products_data(self):
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df = self.convert_product_weights(self.df)
        return self.df
    
    def clean_orders_data(self):
        self.df.drop(columns=['first_name'], inplace=True)
        self.df.drop(columns=['last_name'], inplace=True) 
        self.df.drop(columns=['1'], inplace=True)   
        return self.df