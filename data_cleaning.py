import pandas as pd
import numpy as np

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
        print('Cleaning complete')
        return self.df
    
    def clean_store_data(self):
        self.df.drop(columns=['lat'], inplace=True)
        self.df.replace('NULL', np.nan, inplace=True)
        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce')     
        self.df = self.df[self.df['opening_date'].notna()]
        self.df.dropna(inplace=True)
        self.df.replace('NULL', np.nan, inplace=True)
        self.df['staff_numbers'] = self.df['staff_numbers'].astype(str).str.replace(r'\D', '', regex=True)
        self.df['continent'] = self.df['continent'].str.strip()
        self.df['continent'] = self.df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        print(self.df['continent'].unique())
        return self.df