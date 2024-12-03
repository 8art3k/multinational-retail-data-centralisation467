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