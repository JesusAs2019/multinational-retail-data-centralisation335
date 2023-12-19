import pandas as pd
import numpy as np
import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect
import psycopg2
#import tabula as tb
#import requests
#import json
#import boto3
#import o   

# Class to connect to database using psycopg2
class DatabaseConnector:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file

    def read_creds(self):
        with open(self.yaml_file, 'r') as file:
            creds = yaml.safe_load(file)
            return creds
    def init_db_engine(self, creds):
        engine=create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
   # def init_db_engine(self):
       # engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'aicore_admin'}:{'AiCore2022'}@{'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'}:{'5432'}/{'postgres'}")
       # return engine   
      
    def establish_conn(self):
        self.creds = self.read_creds()
        conn = psycopg2.connect(user =self.creds['RDS_USER'],password=self.creds['RDS_PASSWORD'],host =self.creds['RDS_HOST'],port =self.creds['RDS_PORT'],dbname= self.creds['RDS_DATABASE'])
        cur = conn.cursor()
        return cur

    def list_db_tables(self):
        cur = self.establish_conn()
        query = ("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)

    # def upload_to_db(self, df, table):
    #     # df = pd.DataFrame()
    #     conn = self.establish_conn()
    #     try:
    #         conn.execute(df.to_sql(name=table, con=conn, if_exists='replace', index=False))
    #         conn.close()
    #         # df.to_sql(name=table, con=conn, if_exists='replace', index=False)
    #         print(f"Data uploaded to '{table}' table successfully.")
    #     except Exception as e:
    #         print(f"Error uploading data to '{table}' \n {e}")
    #     # conn.execute(df.to_sql(name=table, con=conn, if_exists='replace', index=False))
    #     conn.close()
         # Step 8: Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.
    def upload_to_db(self, df, db_creds):
        local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds ['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        local_engine.connect()
        dim_users=df.to_sql(df, dim_users, local_engine='sales_data', if_exists='replace')
        print(dim_users)
        return dim_users

if __name__ == '__main__':

    dc = DatabaseConnector('db_creds.yaml')
    #dc.upload_to_db('df', 'db_creds')
    def main():
        dc.list_db_tables()
    main()

