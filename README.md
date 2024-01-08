# multinational-retail-data-centralisation33

---
---

![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/c7572e7a-9ac8-4708-8ef1-1f87bd963a30)


## Table of Contents

1. [Project Scenario Overview](#project-scenario-overview)
2. [Project Discription](#project-discription)
3. [Installation Instructions](#installation-instructions)
4. [Usage Instructions](#usage-instructions)
5. [File Structure](#file-structure)
6. [Data Proccessing](#data-processing)
7. [Lesson Learned](#lesson-learned)
8. [Liscense Information](#liscense-information)


> [Project Scenario Overview](#project-scenario-overview)

Working for a worldwide retails multinational company with a vision to foster a more data-driven strategy approach, we are asked to develop a data pipeline so that the organisation huge sales data scatter over diverse data source locations can be simply acceded from one central data repository.

> [Project Discription](#project-discription)

This project organised into four milestones aims at the data centralization for a sale products multinational organisation. It implements data engineering processes in creating a data pipeline method: Moving company’s bulky datasets from different data sources to a central database (destination). Before data flows into this central data repository, it underwent some data processing using ETL (extract-transform-load) method — so, by using advanced data engineering tools, data has been gathered, imported, cleaned/wrangling, and uploaded to its destination the central database (known as Sales Data), and finally, from there for an optimized data storage repo and ease access a star-based database schema has been produced for querying and analysis. In developing this complex SQL-star-based data queries from advanced Pandas, Python, PostgreSQL and SQL, this project provides to the production environment (stakeholders…) a content for business holistic valuable awarenesses and decisions making on sales and revenues of the types of the stores and their locations.
This project is an opportunity to present to the users a real-life data solution practice, from data acquisition to query and analysis.
The data processing scenarios that the project addresses have been implement in following order of actions:
1.	From five different sources methods in Python have been settled to extract or retrieve retail sales data from their raw formats include JSON, PDF, CSV, AWS RDS database, and RESTful API files.
2.	Then cleaning class code created to clean the 6 tables extracted before being loaded into a central Postgres database as sales data.
3.	A star-based schema database has been created, and then joining 5-dimension tables to the central orders using the primary and the foreign keys making the data queries and analysis easier.
4.	The Queries data questions from the boss to give up-to-date information on the sales data have been answered.

## MILESTONE 1 : Environment Set up
![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/c0481770-92d7-4a77-8f3c-c0133deb05d8)

This  milestone consist of one task, creating the development environment in following the instructions how to install the GitHub Bot: After authorisation and the setting up, installation is completed, a new repo named "Multinational-Retail-Data-Centralisation335" was created in GitHub remotely.  This repo Https link will be used to clone it by the mean of "git clone <repo-Https-link>" method to the local machine where the data tansformatiom will take place.  GitHub will be used to track changes of the different codes locally and pushed to remotely save them in GitHub repo.

> [Installation Instructions](#installation-instructions)

This repository can be access from my GitHub page then copy and paste to any  browser to read:
https://github.com/JesusAs2019/multinational-retail-data-centralisation335.git
Then if anyone to test or make a contribution to this work he can clone it in his using terminal by typing:
"git clone https://github.com/example_repository /folder-name"

> [Usage Instructions](#usage-instructions)
The code credentials file for this project has been protected in gitignore file to secure the sources still, the codes can be run as follow:

1. Run all the codes extraction, cleaning and loading on local engine VS Code terminal.  in "main.ipynb"
2. The required .sql query can be run from sales_data base file in postgres pgAdmin4.

> [Repository Files Structure](#repository-files-structure)
- .gitignore
- database_utils.py
- data_extraction.py
- data_cleaning.py
- main.py
- db_creds.yaml (hidden to gitignore)
- products.csv
- dim_stores_details.csv


## MILESTUNE 2: Data Extraction and Cleaning from Data Sources
![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/4a60825f-b124-44d0-b896-8ea1bf33935e)


---

Our first mission here is to implement the ETL (Extract-Transform-Load) process; that means extracting all the data needed from the different dat sources, cleaning and loading them to the cental database, the queries and analysis will take place.

### Task 1

A new database sales_data was created on pgAdmin4, this was used to store the dataframes locally on a database that can be queried using PostgreSQL.

### Task 2

This task is about the initialisation of the main three project classes in step 1, 2, and 3. Respectively, the scripts and Classes use to extract and clean the data from multiple data sources have been defined.
Step 1:
In this step both new Python script named data_extraction.py within which a class named DataExtractor have been created. This class is used to create methods to extract data from  the 5 different data sources include jason and CSV files, an API and an S3 bucket...

Step 2:
Here another script named database_utils.py and inside it a class DatabaseConnector used to connect with the different data sources and the local engine to upload data frames (issue from cleaning class methods) to the database were created.

Step 3:
Finally, to clean each data from all the sources a script named data_cleaning.py containing a class DataCleaning with its cleaning methods were created.


 [Data Proccessing](data-processing)

1. Data extraction. In "data_extraction.py" we store methods responsible for the upload of data into pandas data frame from different sources.


   
2. Data cleaning. In "data_cleaning.py" we develop the class DataCleaning that clean different tables, which we uploaded in "data_extraction.py".



3. Uploading data into the database. We write DatabaseConnector class "database_utils.py", which initiates the database engine based on credentials provided in ".yml" file.



4. "main.py" contains methods, which allow uploading data directly into the local database.

## Data Processing steps:

There are in total six (6) sources of data.

Step 1:
 Remote Postgres database in AWS Cloud. The table "order_table" is the data of the most interest for the client as it contains actual sales information. In the table, we need to use the following fields "date_uuid", "user_uuid", "card_number", "store_code", "product_code" and "product_quantity". The first 5 fields will become foreign keys in our database, therefore we need to clean these columns from all Nans and missing values. The "product_quantity" field has to be an integer.

Step 2:
 Remote Postgres database in AWS Cloud. The user's data  "dim_users" table. This table is also stored in the remote database, so we use the same upload technics as in the previous case. The primary key here is the "user_uuid" field.
Step 3:
 Public link in AWS cloud. The "dim_card_details" is accessible by a link from the s3 server and stored as a ".pdf" file. We handle reading ".pdf" using the "tabula" package. The primary key is the card number. The card number has to be converted into a string to avoid possible problems and cleaned from "?" artefacts.

Step 4:
 The AWS-s3 bucket. The "dim_product" table. We utilise the boto3 package to download this data. The primary key is the "product code" field. The field "product_price" has to be converted into float number and the field "weight" has to convert into grams concerning cases like ("kg", "oz", "l", "ml").

Step 5:
 The restful-API.  The "dim_store_details" data is available by the GET method. The ".json" response has to be converted into the pandas dataframe. The primary key field is "store_code".
Step 6:
 The "dim_date_times" data is available by link. The ".json" response has to be converted into the pandas datagrame. The primary key is "date_uuid".

 ## MILESTONE 3: SQL Star_Baseed Schema of the Database
 



## MILESTONE 4: QUerying The Data

![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/aecc3dbb-350f-4cf8-bf67-9cbb3c800910)




