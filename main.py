import pandas as pd
import numpy as np
import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
import psycopg2
import tabula as tb
#from tabula import read_pdf
#import PyPDF2 as pdf
import requests
import json
import boto3
import os   
#from botocore import UNSIGNED
#from botocore.client import Config
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

    #This method sends orders_table file to SQL known as orders_table '''
        # Step 4: Once cleaned upload using the upload_to_db method and store in a table called orders_table.        
    def upload_orders_to_db(self, orders_df, db_creds):
        local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        local_engine.connect()
        orders_df.to_sql('orders_table', local_engine, if_exists='replace')   

        #This method sends dfc called dim_card_details table file to SQL database.
              # Step 4: Once cleaned, upload the table with your upload_to_db method to the database in a table called dim_card_details. 
    def upload_card_details_to_db(self, dfc, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          dfc.to_sql('dim_card_details', local_engine, if_exists='replace')
         
      #This method sends df_api file to SQL 
    def upload_store_details_to_db(self, dfs, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          dfs.to_sql('dim_store_details', local_engine, if_exists='replace')
          
    #This method sends date_details file to SQL '''
    def upload_date_details_to_db(self, date_details_df, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          date_details_df.to_sql('dim_date_times', local_engine, if_exists='replace')
                              
      
    # This method sends df_bucket file to SQL as dim_products
    def upload_products_data_to_db(self, products_df, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          products_df.to_sql('dim_products', local_engine, if_exists='replace')

     #This method sends db_creds.yaml file to SQL as dim_users
    def upload_users_data_to_db(self, new_users_df, db_creds):
          local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
          local_engine.connect()
          new_users_df.to_sql('dim_users', local_engine, if_exists='replace')
 
    def run_methods(self):
        creds = DatabaseConnector('db_creds.yaml').read_db_credentials()
        DatabaseConnector('db_creds.yaml').init_db_engine
        print(creds)
       
if __name__ == '__main__':  
   dc = DatabaseConnector('db_creds.yaml')
   dext = DataExtractor()
   dcl = DataCleaning()
   creds = dext.read_creds('db_creds.yaml')
   con = dext.init_db_engine(creds)

   df_users = dext.read_rds_table('legacy_users', con)
   new_users_df=dcl.clean_user_data(df_users) 
   dc.upload_users_data_to_db(new_users_df, creds)
   
   orders_data=dext.read_rds_table('orders_table', con) 
   new_df = dcl.clean_orders_data(orders_data)
   dc.upload_orders_to_db(new_df, creds)
   
   DatabaseConnector('db_creds.yaml').run_methods()
   
   date_data = dext.extract_from_s3_link('date_details')
   new_df = dcl.clean_date_details(date_data)
   dc.upload_date_details_to_db(new_df, creds)
   
   dfc = dext.retrieve_pdf_data('card_details')   
   cleaned_card = dcl.clean_card_data(dfc)
   dc.upload_card_details_to_db(cleaned_card, creds)
   
   dfs = dext.retrieve_stores_data('store_details', con)
   new_store_data = dcl.clean_store_data(dfs)
   dc.upload_store_details_to_db(new_store_data, creds)
  
   products_data = dext.extract_from_s3('products_table')
   products_df=dcl.convert_product_weights(products_data)
   new_products_df = dcl.clean_products_data(products_data)
   dc.upload_products_data_to_db(products_df, creds)
 
  