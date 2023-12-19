import pandas as pd
import numpy as np
import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
import psycopg2
import tabula as tb
import requests
import json
import boto3
import os   
from botocore import UNSIGNED
from botocore.client import Config
from database_utils  import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning   import DataCleaning

'''  DatabaseConnector Class '''
class DatabaseConnector:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file

    ''' This method loads the yaml file from db_creds.yaml '''
    def read_db_credentials(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    ''' This method creates engine using AWS RDS '''
    def init_db_engine(self, creds):
        self.engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return self.engine
    def establish_conn(self, creds):
        self.creds = self.read_db_credentials()
        conn = psycopg2.connect(user =self.creds['RDS_USER'],password=self.creds['RDS_PASSWORD'],host =self.creds['RDS_HOST'],port =self.creds['RDS_PORT'],dbname= self.creds['RDS_DATABASE'])
        cur = conn.cursor()
        return cur
    def list_db_tables(self, creds):
        cur = self.establish_conn(creds)
        query = ("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)      
     #This method sends db_creds.yaml file to SQL
    #def upload_to_db_as_dim_users(self, df_users, table_name , engine_yaml):
                  # Step 7: Now create a method in your DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.
          # Step 8: Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users. #This method sends df_users file to SQL
    def upload_to_db(self, df_users, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          #db.upload_to_db(df,'dim_users', engine)
          df_users.to_sql(df_users, 'dim_users', local_engine, if_exists='replace')
          print('dim_users')
         # df_users.to_sql(table_name, engine_yaml, if_exists='replace')
          return 'dim_users'

        #This method sends df_pdf file to SQL
    #def upload_to_db(self, df_pdf, table_name, engine_pdf):
      #  df_pdf.to_sql(table_name, engine_pdf, if_exists='replace')
              # Step 4: Once cleaned, upload the table with your upload_to_db method to the database in a table called dim_card_details. 
    def upload_to_db(self, data_frame, dim_card_details, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          data_frame.to_sql(dim_card_details, local_engine, if_exists='replace')
          print(dim_card_details)
          return dim_card_details    

      #This method sends df_api file to SQL 
    #def upload_to_db_as_dim_store_details(self, df_api, table_name, engine_api):
       # df_api.to_sql(table_name, engine_api, if_exists='replace')
    def upload_to_db(self, dfs, dim_store_details, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          dim_store_details = dfs.to_sql(dim_store_details, local_engine, if_exists='replace')
          print(dim_store_details)
          return dim_store_details       

    # This method sends df_bucket file to SQL
    #def upload_to_db_as_dim_products(self, df_bucket, table_name, engine_bucket):
       # df_bucket.to_sql(table_name, engine_bucket, if_exists='replace')
              # Step 4: Once complete insert the data into the sales_data database using your upload_to_db method storing it in a table named dim_products.  
    def upload_to_db(self, df_bucket, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          dim_products = df_bucket.to_sql(dim_products, local_engine, if_exists='replace')
          print(dim_products)
          return dim_products

    #This method sends orders_table file to SQL '''
    #def upload_to_db_as_orders_table(self, df_new_orders, table_name, engine_orders):
        #df_new_orders.to_sql(table_name, engine_orders, if_exists='replace')
        # Step 4: Once cleaned upload using the upload_to_db method and store in a table called orders_table.
    def upload_to_db(self, df_S3, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          orders_table = df_S3.to_sql(orders_table, local_engine='sales_data', if_exists='replace')
          print(orders_table)

    #This method sends sales_date file to SQL '''
    #def upload_to_db_as_dim_date_times(self, sales_date_df, table_name, engine_sales):
       # sales_date_df.to_sql(table_name, engine_sales, if_exists='replace')
    def upload_to_db(self, dft, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          dim_date_times = dft.to_sql(dim_date_times, local_engine, if_exists='replace')
          print(dim_date_times) 
          
    def run_methods(self):
        creds = DatabaseConnector('db_creds.yaml').read_db_credentials()
        DatabaseConnector('db_creds.yaml').init_db_engine

        print(creds)
    def upload_orders_to_db(self, orders_df, db_creds):
        local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds           ['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        local_engine.connect()
        orders_table =orders_df.to_sql(orders_df, 'orders_table', local_engine, if_exists='replace')    
        
if __name__ == '__main__':  
   dc = DatabaseConnector('db_creds.yaml')
   dext = DataExtractor()
   dcl = DataCleaning()
   creds = dext.read_creds('db_creds.yaml')
   con = dext.init_db_engine(creds)
  # dim_users = dext.read_rds_table('legacy_users', con)
   #dcl.clean_user_data(["dim_users"])
   orders_data=dext.read_rds_table('orders_table', con) 
   new_df = dcl.clean_orders_data(orders_data)
   dc.upload_orders_to_db(new_df, creds)
   #DatabaseConnector('db_creds.yaml').run_methods()
   #dc.upload_to_db_as_dim_users('dim_users', 'engine_yaml')  
  
  # def main():
      #  dc.list_db_tables('creds')
  # main() 
 
  