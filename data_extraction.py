import yaml
import sqlalchemy as db
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
import psycopg2
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
        # print(df.columns)
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
        
        #print(tabula(self.df_users, headers = 'keys', tablefmt = 'psql'))
        print(self.df_users)
        show(self.df_users)
        return self.df_users  # 1- upload_to_db('dim_users')
         # Step 2: Create a method in your DataExtractor class called retrieve_pdf_data, which takes in a link as an argument and returns a pandas DataFrame.
         # Use the tabula-py Python package, imported with tabula to extract all pages from the pdf document at following link. Then return a DataFrame of the extracted data.
    def retrieve_pdf_data(self, link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        df = tabula.read_pdf(link, pages="all")
        df = pd.concat(df)
        dfc = df.reset_index(drop=True)
        print(dfc)
        return dfc  # 2- upload_to_db('dim_card_details')
      #  dfs = pd.concat(tabula.read_pdf(url, pages='all'), ignore_index=True)
       # return dfs
        # Task5: Step 1: Create a method in your DataExtractor class called list_number_of_stores which returns the number of stores to extract. It should take in the number of stores endpoint and header dictionary as an argument.
      
    def list_number_of_stores(self, retu_endpoint, header_dict):
        self.header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} 
        self.retu_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(self.retu_endpoint, headers=self.header_dict) 
        data = response.json()
        return data
# Step 3: create another method retrieve_stores_data which will take the retrieve a store endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.
    def retrieve_stores_data(self, retr_endpoint, header_dict):
        self.header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.retr_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        list_of_df = []
        self.data = self.list_number_of_stores(self.retr_endpoint, header_dict)
        for store_number in range (0, self.data['number_stores']):
            self.retr_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            response = requests.get(self.retr_endpoint, headers=self.header_dict)
            list_of_df.append(pd.json_normalize(response.json()))
        return pd.concat(list_of_df)      # 3 - upload_to_db('dim_stores-_details')  
            
  # Task6- Step 1:Create a method in DataExtractor called extract_from_s3 which uses the boto3 package to download and extract the information returning a pandas DataFrame.
 # The S3 address for the products data is the following s3://data-handling-public/products.csv the method will take this address in as an argument and return the pandas DataFrame. 
    def extract_from_s3(self, s3_link):
        self.s3_link = 's3://data-handling-public/products.csv'
        s3 = boto3.client('s3')
        #s3.download_file('data-handling-public', '/products.csv', '/Users/vicky/multinational-retail-data-centralisation335/products.csv')       
        response = s3.get_object(Bucket='data-handling-public', Key='products.csv')
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}") # 4 - upload_to_db('dim_products')
            return pd.read_csv(response.get("Content"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
         
   #Task 7  # Order table data  orders_table = extractor.read_rds_table(table_names, 'orders_table', engine)  # # Step 1:Using the database table listing methods you created earlier list_db_tables, list all the tables in the database to get the name of the table containing all information about the product orders.
   # Step 2: Extract the orders data using the read_rds_table method you create earlier returning a pandas DataFrame.  
   
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
        return self.dfo                           # 5 - upload_to_db('orders_table')
 
    #Task8 The final source of data is a JSON file, orders_table S3_link     
    def extract_from_s3_link(self, url):
        self.url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        response = requests.get(url)
        dict = response.json()
        df_S3 = pd.DataFrame([])
        for column_name in dict.keys():
            value_list = []
            for _ in dict[column_name].keys():
                value_list.append(dict[column_name][_])
            df_S3[column_name] = value_list
        return df_S3  # 6 - upload_to_db('dim_date_times')
 

if __name__ == '__main__': 
   dext = DataExtractor() 
   creds = dext.read_creds('db_creds.yaml')
   con = dext.init_db_engine(creds)
  # df_users = dext.read_rds_table('legacy_users', con)
   #pd.set_option('display.max_columns', None)
   #print(df_users)
   #show(df_users)
   
   dfo = dext.read_rds_table('orders_table', con)
   
   #print(tabula(dfo, headers = 'keys', tablefmt = 'psql')) 
   #print(dfo)
   #show(dfo)
   dfc = dext.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
   #print(dfc.head())
   #print(dfc.info())
   #display(df)
  # pd.set_option('display.max_columns', 11)
  # pd.set_option('display.max_rows', 1000)
   #print(dfo.head(1000))
   #dfd = dext.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
   #print(dfd)
   #dfs = dext.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
   #print(dfs) 
   #data = dext.extract_from_s3('s3://data-handling-public/products.csv')
   #print(data)   
   #db.to_csv('dim_store_details.csv')
  # df_S3 = dext.extract_from_s3_link('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
   #print(df_S3)
   def main():
       dict.list_db_tables()
       main()
   #extract_from_s3('s3://data-handling-public/products.csv')
   #df = dext.extract_from_s3_link()
       
 

