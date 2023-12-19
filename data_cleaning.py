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
import pandasgui
from pandasgui import show

# Step 6: Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.
# You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.

class DataCleaning:
      def __init__(self) -> None:
         # self.data_frame = data_frame  
          pass
      def clean_user_data(self):        
          df_users().replace('NULL', np.NaN)
          df_users.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)
          df_users['date_of_birth'] = pd.to_datetime(df_users['date_of_birth'], errors = 'ignore')
          df_users['join_date'] = pd.to_datetime(df_users['join_date'], errors ='coerce')
          df_users = df_users.dropna(subset=['join_date'])
          df_users['phone_number'] = df_users['phone_number'].str.replace('/W', '')
          df_users = df_users.drop_duplicates(subset=['email_address'])
          df_users.drop(df_users.columns[0], axis=1, inplace=True)
          dim_users = df_users.to_csv("legacy_users.csv")
          print(dim_users)
          return dim_users
      #def clean_user_data(self):           
        #  self.df_users = self.df_users.dropna(how='any').dropna(how='any', axis=1)
         # self.df_users.update(self.df_users)
         # self.df_users = self.df_users.reset_index(drop=True)
          #self.df_users.update(self.df_users)
          #return self.df_users    

         # Task4: Step 3: Create a method called clean_card_data in your DataCleaning class to clean the data to remove any erroneous values, NULL values or errors with formatting.
      def clean_card_data(self, card_data_table):
        card_data_table.replace('NULL', np.NaN, inplace=True)
        card_data_table.dropna(subset=['card_number'], how='any', axis=0, inplace=True)
        dim_card_details = card_data_table[~card_data_table['card_number'].str.contains('[a-zA-Z?]', na=False)]
        return dim_card_details
        # def clean_card_data(self, table):
            # self.valid_date(table, 'date_payment_confirmed')
             #self.remove_null(table)
            #   try:
             #     assert table['card_number'].str.isdigit()
              # except:
              #    AssertionError
            # print (table)
              # return table

#Task5 - Step 4: Create a method in the DataCleaning class called clean_store_data which cleans the data retrieve from the API and returns a pandas DataFrame.
# Step 5: Upload your DataFrame to the database using the upload_to_db method storing it in the table dim_store_details. 
      def clean_store_data(self, dfs):
        #setting 'index' column as index
          dfs =table.set_index('index')
        #drops the rows with wrong data and NULL values and deleting column 'lat' which has no useful data
          dfs = table.drop(table[table['country_code'].str.len() > 3].index.tolist())
          del table['lat']
        #replacing mis-spelled words in the columns with correct values
          mapping = {'eeEurope': 'Europe', 'eeAmerica': 'America', '30e': '30', '80R': "80", 'A97': "97", '3n9': "39", 'J78':'78', 'N/A': np.nan, None: np.nan}
          for column in ['staff_numbers', 'continent', 'latitude', 'longitude']:
              dfs[column] = table[column].replace(mapping)
       #converts the date strings to ISO date format
              dfs['opening_date'] = table['opening_date'].apply(lambda x: pd.to_datetime(x, errors='coerce', infer_datetime_format= True))
              return dfs

    #  You will need to be logged into the AWS CLI before you retrieve the data from the bucket.
#Step 2: Create a method in the DataCleaning class called convert_product_weights this will take the products DataFrame as an argument and return the products DataFrame.
#If you check the weight column in the DataFrame the weights all have different units.
#Convert them all to a decimal value representing their weight in kg. Use a 1:1 ratio of ml to g as a rough estimate for the rows containing ml.
#Develop the method to clean up the weight column and remove all excess characters then represent the weights as a float.
      def convert_product_weights(self, table): 
         table = table.drop(table[table['weight'].apply(lambda x: (type(x) != str or len(x) == 10))].index.tolist())
         table['weight'] = table['weight'].apply(lambda x:x.strip('kg') if x[-2:] == 'kg' else x)
         table['weight'] = table['weight'].apply(lambda x: str(int(x[:-2])/1000) if x.endswith('ml') else x)
         table['weight'] = table['weight'].apply(lambda x :str(int(x[:x.index('x')-1]) * int(x[x.index('x')+2:-1])/1000) if 'x' in x else x)
         table['weight'] = table['weight'].apply(lambda x: str(float(x[:-1])/1000) if x.endswith('g') else x)
         table['weight'] = table['weight'].apply(lambda x: str(int(x[:-3])/1000) if x.endswith('g .') else x)
         table['weight'] = table['weight'].apply(lambda x: str(int(x[:-2])*0.028) if x.endswith('oz') else x)
         table['weight'] = table['weight'].astype('float64')
         return table
       #  def convert_product_data(self, x): if 'kg' in x: x = x.replace('kg', '') x = float(x); elif 'ml' in x: x = x.replace('ml', '') x = float(x)/1000;  elif 'g' in x: x = x.replace('g', '')  x = float(x)/1000; elif 'lb' in x: x = x.replace('lb', '') x = float(x)*0.453591; elif 'oz' in x: x = x.replace('oz', '') x = float(x)*0.0283495; return x
        
        #Step 3: Now create another method called clean_products_data this method will clean the DataFrame of any additional erroneous values.
      def clean_products_data(self, data):
          data.replace('NULL', np.NaN, inplace=True)
          data['date_added'] = pd.to_datetime(data['date_added'], errors ='coerce')
          data.dropna(subset=['date_added'], how='any', axis=0, inplace=True)
          data['weight'] = data['weight'].apply(lambda x: x.replace(' .', ''))
          temp_cols = data.loc[data.weight.str.contains('x'), 'weight'].str.split('x', expand=True) # splits the weight column in top 2 temp columns split by the 'x'
          numeric_cols = temp_cols.apply(lambda x: pd.to_numeric(x.str.extract('(\d+\.?\d*)', expand=False)), axis=1) # Extracts the numeric values from the temp columns just created
          final_weight = numeric_cols.prod(axis=1) # Gets the product of the 2 numeric values
          data.loc[data.weight.str.contains('x'), 'weight'] = final_weight
          data['weight'] = data['weight'].apply(lambda x: self.convert_product_data(x))
          data.drop(data.columns[0], axis=1, inplace=True)
          return data #  def convert_product_weights(self):
          #self.df_bucket['weights_in_kg'] = self.df_bucket['weight'].str.extract(r'(\d+.\d+)').astype('float')
          #for page in self.df_bucket['weights_in_kg']:
          #  if 'x' in str(page):
            #    page = page.replace('x', '*')
              #  page = page.replace(' ', '')
              #  page = eval(page)
          #cells_to_divide = self.df_bucket['weight'].str.contains('kg',na=False)
          #self.df_bucket['weights_in_kg'].iloc[~cells_to_divide.values] = self.df_bucket['weights_in_kg'].iloc[~cells_to_divide.values].multiply(0.001)
          #return self.df_bucket
          
      def clean_products_data(self):
          self.df_bucket = self.df_bucket.dropna(how='all')
          self.df_bucket['removed'] = self.df_bucket['removed'].astype('category')
          self.df_bucket['category'] = self.df_bucket['category'].astype('category')
          return self.df_bucket
        
 

     # def clean_orders_data(self, df_S3):                                
        #  df_S3.drop(columns='1',inplace=True)  
        #  df_S3.drop(columns='first_name',inplace=True)  
         # df_S3.drop(columns='last_name',inplace=True)  
         # df_S3['card_number'] = df_S3['card_number'].apply(self.isDigits)  
         # df_S3.dropna(how='any', inplace= True)   
          #return df_S3
          #cleaned_orders_table = dcl.clean_order_data(df_S3)
          #cleaned_orders_table.to_csv('orders.csv')
          #dc.upload_to_db(cleaned_orders_table, "df_S3", db_creds) 
  

      def clean_date_time(self, table_name):
          self.table_name = self.df_S3
          self.df_S3['month'] = pd.to_numeric( self.df_S3['month'],errors='coerce', downcast="integer")
          self.df_S3['year'] = pd.to_numeric(self.df_S3['year'], errors='coerce', downcast="integer")
          self.df_S3['day'] = pd.to_numeric(self.df_S3['day'], errors='coerce', downcast="integer")
          self.df_S3['timestamp'] = pd.to_datetime(self.df_S3['timestamp'], format='%H:%M:%S', errors='coerce')
          self.df_S3.dropna(how='any',inplace= True)
          new_df_S3 = self.df_S3.reset_index(inplace=True)
          return new_df_S3           # 6 - upload_to_db('dim_date_times')                             
       
       
         # This method removes some columns and dropped NaN values
      def clean_orders_data(self, data_frame):
          self.data_frame = data_frame         
          data_frame.drop(["level_0", "first_name", "last_name", "1", "index"], axis=1, inplace=True )
          data_frame.dropna(how='any', axis=1)
          return data_frame                                                             # 5 - upload_to_db('orders_table')
          
          
          #self.cleaning_orders_obj = self.dcl.cleaning_orders_data('dfo')
          #if hasattr(self.cleaning_orders_data, 'dfo'):
           #  print(f"{self.cleaning_orders_data.dfo}")
          #else:
           #   print("dfo attribute does not exist.")
        
       
       
      #@staticmethod
      #def clean_orders_data(dfo):
          #new_dfo = dfo
          #new_dfo = new_dfo.drop(columns=['level_0', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        # Returning the longest length
         # print(new_dfo[[144]])
         # return new_dfo
      
      
      
      
      
        
          #This method cleanse sales_date_df file 
     # def clean_sales_date(self):
        #  self.sales_date_df['time_period'] = self.sales_date_df['time_period'].astype('category')
          #self.sales_date_df = self.sales_date_df.dropna(how='any').dropna(how='any',axis=1)
         # self.sales_date_df = self.sales_date_df.drop_duplicates()
          #self.sales_date_df.update(self.sales_date_df)
         #return self.sales_date_df
          # engine = DatabaseConnector.init_db_engine(self, creds)
         # data_frame = DataExtractor(engine).read_rds_tables(table_name) 
 
if __name__ == '__main__': 
   #dict=upload_to_db('dim_date_times')  
   dc = DatabaseConnector('yaml_file')
   dext = DataExtractor() 
   dcl = DataCleaning()
  
   creds = dext.read_creds('db_creds.yaml')
   con = dext.init_db_engine(creds)
  # dim_users = dext.read_rds_table('legacy_users', con)
   #dcl.clean_user_data(["dim_users"])
   orders_data=dext.read_rds_table('orders_table', con) 
   new_df = dcl.clean_orders_data(orders_data)
   print(new_df)
   display(new_df)
   pd.set_option('display.max_columns', None)
  
  
  
  
   
   #print(table(cleaned_orders, headers = 'keys', tablefmt = 'psql'))
   # print(dim_users)
   
   
   
   
   
    #Connects to the database, extracts the data from the relational database on AWS, cleans the data and uploads the data to the db
   # db_creds = connector.read_db_creds()
   # engine = connector.init_db_engine(db_creds)
   # table_names = connector.list_db_tables(engine)
   # legacy_users_table = extractor.read_rds_table(table_names, 'legacy_users', engine)
   # clean_legacy_users_table = cleaner.clean_user_data(legacy_users_table)
    #clean_legacy_users_table.to_csv('users.csv')
    #connector.upload_to_db(clean_legacy_users_table, "dim_users", db_creds)

    #Extracts data from pdf, cleans the data and uploads the df to the db
   # card_data_table = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
   # card_data_table.to_csv('card_details.csv')
   # clean_card_data_table = cleaner.clean_card_data(card_data_table)
    #clean_card_data_table.to_csv('card_data.csv')
   #connector.upload_to_db(clean_card_data_table, "dim_card_details", db_creds)


    # Extracts, cleans and uploads store data to db
    #api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    #number_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
   #retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
   # number_stores = extractor.list_number_of_stores(number_stores_endpoint, api_key)
   # store_data = extractor.retrieve_stores_data(number_stores, retrieve_store_endpoint, api_key)
   # store_data.to_csv('store_outputs.csv')
   # clean_store_data_table = cleaner.clean_store_data(store_data)
   # clean_store_data_table.to_csv('store_outputs.csv')
    #connector.upload_to_db(clean_store_data_table, "dim_store_details", db_creds)


    # Extracts the product data
   # product_data = extractor.extract_from_s3('s3://data-handling-public/products.csv')
   #cleaned_product_data = cleaner.clean_product_data(product_data)
    #cleaned_product_data.to_csv('product.csv')
    #connector.upload_to_db(cleaned_product_data, 'dim_products', db_creds)

    # Order table data
   # clean_orders_table = cleaner.clean_order_data(orders_table)
    #clean_orders_table.to_csv('orders.csv')
    #connector.upload_to_db(clean_orders_table, "orders_table", db_creds)

    #Date data
    #date_data = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
   #clean_date_data = cleaner.clean_date_data(date_data)
    #clean_date_data.to_csv('date.csv')
    #connector.upload_to_db(clean_date_data, "dim_date_times", db_creds)
  

   
          
          
   
