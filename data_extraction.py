import yaml
import sqlalchemy as db
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
import psycopg2
import io
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


class DataExtractor:
    def __init__(self):
       # self.yaml_file = yaml_file 
        pass
    def read_creds(self, yaml_file):
        with open(yaml_file, 'r') as file:
            self.credentials = yaml.safe_load(file)
            return self.credentials
    def establish_conn(self, conn):
        self.conn = psycopg2.connect(user =self.creds['RDS_USER'],password=self.creds['RDS_PASSWORD'],host =self.creds['RDS_HOST'],port =self.creds['RDS_PORT'],dbname= self.creds['RDS_DATABASE'])
        self.creds = self.read_creds()
        self.cur = self.conn.cursor()
        return self.cur    
    def init_db_engine(self, creds):       
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()
        return conn
    def read_data(self, conn):
        self.conn = self.engine.connect()
        self.inspector = db.inspect(self.conn)
        tables = self.inspector. get_table_names()
        print(tables)
        return tables
    def read_rds_table(self, tables_name, conn):
       # self.tables_name = 'legacy_users'
        df_users = pd.read_sql_table(tables_name, conn)
       
        print(df_users)
        return df_users

    def extract_user_data(self, engine):
        self.engine ='postgres'
        self.engine = create_engine(f"{self.RDS_DATABASE_TYPE}+{self.RDS_DBAPI}://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DATABASE}")
        self.engine = engine.connect()
        self. data = pd.read_rds_table(self.tables_name, engine)
        self.postgres = self.connect(self.RDS_HOST, self.RDS_PASSWORD, self.RDS_USER, self.RDS_DATABASE, self.RDS_PORT)      
        self.df_users='postgres'.extract('engine')
        pd.set_option('display.max_columns', None)
        
        print(self.df_users)
        show(self.df_users)
        return self.df_users
 
         # Step 2: Retrieving  pdf_data using the pdf_link as an argument.
    def retrieve_pdf_data(self, link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        df = tabula.read_pdf(link, pages='all')
        df = pd.concat(df)
        dfc = df.reset_index(drop=True)
        print(dfc)
        return dfc 
       
        # Task5: Step 1: A method to list the number_of_stores to extract using the endpoint and header dictionary as an argument.     
    def list_number_of_stores(self, retu_endpoint, header_dict):
        self.header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} 
        self.retu_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(self.retu_endpoint, headers=self.header_dict) 
        data = response.json()
        return data
# Step 3: Method to retrieve stores_data from the API.
    def retrieve_stores_data(self, retr_endpoint, header_dict):
        self.header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.retr_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        list_of_df = []
        self.data = self.list_number_of_stores(self.retr_endpoint, header_dict)
        for store_number in range (0, self.data['number_stores']):
            self.retr_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            response = requests.get(self.retr_endpoint, headers=self.header_dict)
            list_of_df.append(pd.json_normalize(response.json()))
        return pd.concat(list_of_df)     
            
  # Task6- This method is called to extract products file from s3_address . 
    def extract_from_s3(self, s3_address):
        s3_address = 's3://data-handling-public/products.csv'
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='data-handling-public', Key='products.csv')
        content = response['Body'].read() 
        df_s3 = pd.read_csv(io.BytesIO(content))
        return df_s3
         
   #Task 7 This step aim to Extract the orders data using the read_rds_table method created earlier that return a pandas DataFrame.   
    def extract_orders_data(self, engine):
        self.engine ='postgres'
        self.engine = create_engine(f"{self.RDS_DATABASE_TYPE}+{self.RDS_DBAPI}://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DATABASE}")
        self.engine = engine.connect()
        self. data = pd.read_rds_table(self.tables_name, engine)
        self.postgres = self.connect(self.RDS_HOST, self.RDS_PASSWORD, self.RDS_USER, self.RDS_DATABASE, self.RDS_PORT)      
        self.dfo='postgres'.extract('engine')
        pd.set_option('display.max_columns', None)
        print(self.dfo)
        self.dfo.isna().any().sum()
        return self.dfo                          
    
    #Task8 This step is about to retrieve the JSON file, date_details from the S3_link     
    def extract_from_s3_link(self, url):
        url = 'http://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        date_details = requests.get(url).json()
        date_details_df = pd.DataFrame(date_details)
        return date_details_df     

if __name__ == '__main__': 
   dext = DataExtractor() 
   creds = dext.read_creds('db_creds.yaml')
   con = dext.init_db_engine(creds)
 # Calling to extract the users table   
   df_users = dext.read_rds_table('legacy_users', con)
   print(df_users.info())
   pd.set_option('display.max_columns', 12)
   pd.set_option('display.max_rows', None)
   show(df_users)
# Calling method to extract orders table   
   dfo = dext.read_rds_table('orders_table', con)   
   print(dfo.info())
   show(dfo)
# Calling method to retrieve card_details table    
   dfc = dext.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
   print(dfc.info())    
   show(dfc)
   pd.set_option('display.max_columns', None)
   pd.set_option('display.max_rows', None)
   
# Calling to retrieve the store_details table  
   dfd = dext.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
   print(dfd)
   dfs = dext.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}',{"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
   print(dfs.info())
   pd.set_option('display.max_columns', None)
   pd.set_option('display.max_rows', None)
   show() 
   
# Calling the products extraction table
   products_data = dext.extract_from_s3('s3://data-handling-public/products.csv')
   print(products_data)
   print(products_data.info())
   show(products_data)  

   date_details_df = dext.extract_from_s3_link('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
   print(date_details_df.info())
   show(date_details_df)
   pd.set_option('display.max_columns', None)
   
   def main():
       dict.list_db_tables()
       main()
 
  
       
 

