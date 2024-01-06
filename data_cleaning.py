import pandas as pd
import numpy as np
from datetime import datetime
from pprint import pprint
from sqlalchemy import create_engine, inspect, table
import psycopg2
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect, text
import requests
import tabula
from database_utils import DatabaseConnector
from IPython.display import display
from tabula import read_pdf
import PyPDF2 as pdf
import json
import boto3
import os
import re
import pandasgui
from pandasgui import show

class DataCleaning:
      def __init__(self) -> None:
         # self.date_df = date_df  
          pass
           #cleaning the user_data table. To look out for NULL values, errors with dates, wrong typed values and rows filled with the wrong information.
      def clean_user_data(self, df_users):        
         # df_users().replace('NULL', np.NaN, inplace=True)
         # df_users.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)
          df_users['date_of_birth'] = pd.to_datetime(df_users['date_of_birth'], errors = 'ignore')
          df_users['join_date'] = pd.to_datetime(df_users['join_date'], errors ='coerce')
          df_users = df_users.dropna(subset=['join_date'])
          #df_users['phone_number'] = df_users['phone_number'].str.replace('\W', '')
          #df_users = df_users.drop_duplicates(subset=['email_address'])
          df_users.drop(df_users.columns[0], axis=1, inplace=True)       
          print(df_users)         
          return df_users
        
         # Task4: Step 3: Cleaning the card_data table in removing any erroneous values, NULL values or any formatting errors.                 
      def clean_card_data(self, dfc):                    
         dfc = dfc.drop(dfc[dfc['expiry_date'].apply(lambda x: (len(x) > 5) or len(x) == 4)].index.tolist())
         dfc['card_number'] = dfc['card_number'].apply(lambda x: str(x) )
         dfc['card_number'] = dfc['card_number'].apply(lambda x: x.strip('?') if '?' in x else x)
        #converts the date strings to ISO date format
         dfc['date_payment_confirmed'] =dfc['date_payment_confirmed'].apply(lambda x: pd.to_datetime(x, errors='coerce'))
         return dfc       

#Task5 - Step 4: Cleaning the data retrieve from the API and pandas DataFrame is returned. 
      def clean_store_data(self, dfs):
        #setting 'index' column as index
          dfs =dfs.set_index('index')
        #drops the rows with wrong data and NULL values and deleting column 'lat' which has no useful data
          dfs.dropna(subset=['store_code'], how='any', axis=0, inplace=True)
          dfs = dfs[~dfs['staff_numbers'].str.contains('\W', na=False)]
          dfs = dfs.drop(dfs[dfs['country_code'].str.len() > 3].index.tolist())
          del dfs['lat']
          dfs['continent'] = dfs['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')
        #replacing mis-spelled words in the columns with correct values
          mapping = {'eeEurope': 'Europe', 'eeAmerica': 'America', '30e': '30', '80R': "80", 'A97': "97", '3n9': "39", 'J78':'78', 'N/A': np.nan, None: np.nan}
          for column in ['staff_numbers', 'continent', 'latitude', 'longitude']:
              dfs[column] = dfs[column].replace(mapping)
       #converts the date strings to ISO date format
              dfs['opening_date'] = dfs['opening_date'].apply(lambda x: pd.to_datetime(x, errors='coerce'))
              print(dfs)
              return dfs
           
# Step 2: converting (ml, oz, lb, g) of the product_weights to kg, and taking the products DataFrame as an argument. 
      def convert_product_weights(self, products_data): 
          #products_data = products_data.drop(products_data[products_data['weight'].apply(lambda x: (type(x) != str or len(x) == 10))].index.tolist())          
          products_data['weight'] = products_data['weight'].fillna('NO VALUE')
          conversion_dict = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.0283495, 'lb': 0.45359}        
          pattern = r'(\d+\.\d+|\d+)\s*x\s*(\d+\.\d+|\d+)\s*(kg|g|ml|oz|lb)?' 

          products_data['weight'] = products_data['weight'].apply(lambda x: float(re.search(pattern, x).group(1)) * float(re.search(pattern, x).group(2)) * conversion_dict.get(re.search(pattern, x).group(3), 0.001) if ' x ' in str(x) and pd.notna(x)         
            else float(x[:-2]) if x.endswith('kg') 
            else float(x[:-2])*0.001 if x.endswith('ml') 
            else float(x[:-1])*0.001 if x.endswith('g') 
            else float(x[:-2])*0.0283495 if x.endswith('oz')
            else float(x[:-2])*0.45359 if x.endswith('lb')
            else float(x[:-3]) * 0.001 if x.endswith('.')
            else np.nan if pd.notna(x)
            else np.NAN)
          products_data['date_added'] = pd.to_datetime(products_data['date_added'], errors ='coerce')
          products_data.dropna(subset=['date_added'], how='any', axis=0, inplace=True)                          
          products_data.drop(columns='Unnamed: 0', axis=1, inplace=True)      
          products_data['weight'] = products_data['weight'].astype('float64')
          products_data['weight'] = products_data['weight'].apply(lambda x: round(x, 3)) #if pd.isna(x) == False else np.NAN) # rounding
          
          return products_data
       
        #Step 3: Now create another method called clean_products_data this method will clean the DataFrame of any additional erroneous values.
      def clean_products_data(self, products_data):
          products_data = self.convert_product_weights(products_data)
         # products_data['date_added'] = pd.to_datetime(products_data['date_added'], errors ='coerce')
          products_data.dropna(subset=['date_added'], how='any', axis=0, inplace=True)         
          #products_data['weight'] = products_data['weight'].astype(str).str.replace(r'\W+', '', regex=True)
          #products_data.drop(columns='Unnamed: 0', axis=1, inplace=True)
          #products_data['weight'] = products_data['weight'].astype('float64')                      
          return products_data
       
      def clean_orders_data(self, data_frame):
          self.data_frame = data_frame         
          data_frame.drop(["level_0", "first_name", "last_name", "1", "index"], axis=1, inplace=True )
          data_frame.dropna(how='any', axis=1)
          return data_frame            
        
      def clean_date_details(self, date_df):
          self.date_df = date_df
          self.date_df['month'] = pd.to_numeric( self.date_df['month'], errors='coerce', downcast="integer")
          self.date_df['year'] = pd.to_numeric(self.date_df['year'], errors='coerce', downcast="integer")
          self.date_df['day'] = pd.to_numeric(self.date_df['day'], errors='coerce', downcast="integer")
          self.date_df['timestamp'] = pd.to_datetime(self.date_df['timestamp'], format='%H:%M:%S', errors='coerce')
          self.date_df.dropna(how='any', inplace= True)
         # new_date_df = self.date_df.reset_index(inplace=True)
          return date_df                                   
            
 
if __name__ == '__main__': 
   #dict=upload_to_db('dim_date_times')  
   dc = DatabaseConnector('yaml_file')
   dext = DataExtractor() 
   dcl = DataCleaning()
   creds = dext.read_creds('db_creds.yaml')
   con = dext.init_db_engine(creds)
   df_users = dext.read_rds_table('legacy_users', con)
   new_users_df=dcl.clean_user_data(df_users)
   print(new_users_df)
   print(new_users_df.info())

# Calling clean_orders_data dataframe   
   orders_data=dext.read_rds_table('orders_table', con) 
   new_df = dcl.clean_orders_data(orders_data)
   print(new_df)
   display(new_df)
   pd.set_option('display.max_columns', None)
   
#calling the clean_date_details
   date_data = dext.extract_from_s3_link('date_details')
   new_date_df = dcl.clean_date_details(date_data)
   print(new_date_df)
   
# Calling stores_table
   dfs = dext.retrieve_stores_data('store_details', con)
   new_store_data = dcl.clean_store_data(dfs)
   pd.set_option('display.max_columns', None)
   pd.set_option('display.max_rows', None)
   print(new_store_data.info())
   #show()
   
# Calling card_details_table
   #dfc = dext.retrieve_pdf_data('card_details')   
   #cleaned_card = dcl.clean_card_data(dfc)
   #print(cleaned_card.info())
   #show() 
   
# Calling the clean_products_data dataframe table
   products_data = dext.extract_from_s3('products_table')
   products_df=dcl.convert_product_weights(products_data)
   print(products_df)
   print(products_df.info())
   #new_products_df = dcl.clean_products_data(products_data)
   #print(new_products_df)
   #show()
   pd.set_option('display.max_columns', None)
  # pd.set_option('display.max_rows', None)
   
   
   
   
   
   
   
   

   
   
   
   

   
          
          
   
