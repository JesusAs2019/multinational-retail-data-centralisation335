# multinational-retail-data-centralisation33

---
---

![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/c7572e7a-9ac8-4708-8ef1-1f87bd963a30)


## Table of Contents

1. [Project Scenario Overview](#project-scenario-overview)
2. [Project Discription](#project-discription)
## A- [MILESTONE 1](milestone-1)
4. [Installation Instructions](#installation-instructions)
5. [Usage Instructions](#usage-instructions)
6. [File Structure](#file-structure)
## B- [MILESTONE 2](milestone-2)
8. [Data Proccessing](#data-processing)
## C- [MILESTONE 3](mileston-3)
## D- [MILESTONE 4](mileston-4)
11. [Lesson Learned](#lesson-learned)
12. [Liscense Information](#liscense-information)
13. [Acknowledgement](#acknowledgement)



---
---


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


## MILESTONE 2: Data Extraction and Cleaning from Data Sources
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
Dealing here with the remote Postgres database in AWS Cloud. The "order_table" table is the core data, and is of the most interest for the Company as it contains the genuine and, updated sales information. In this table the main columns are "user_uuid", "date_uuid", "card_number", "product_code", "store_code" and "product_quantity". The first 5 columns are used as foreign keys in the database schema, and these columns must be cleaned from all the Nans, duplicate, and missing values. The column product_quantity" field has to be an integer.

Step 2:
 In the remote Postgres database in AWS Cloud there is the user's data table and called "dim_users" table and stored in sales_data database in pgAdmin4 postgres. The column "user_uuid" is used as primary key.
 
Step 3:
 Public link in AWS cloud to extract and clean the table card_details accessible from the pdf link in the s3 server using the tabula package. Uploaded and stored in pgAdmin4 prostgres as dim_card_details under sales_data database. The column card_number has been used as primary key. The card_number column has be cleaned from '?' artefacts, and to be converted into a string to avoid possible conflicts.

Step 4:
 The AWS-s3 bucket contains the table products, and the boto3 package has been used to download its data. After extraction, and cleaning this table was loaded to the sales_data database as "dim_products" table. The column "product_code" is used to set the primary key. Finally, the column 'product_price' has been converted into float number and the column "weight" with the units ml, oz, lb, and g were converted into kg.

Step 5:
 The table store_details stored in restful-API was extracted using the GET method, and the '.json' file response was returned as pandas dataframe. THis table was uploaded to pgAdimin 4 as "dim_store_details" in sales_data database, and the column "store_code" is used to set the primary key.

Step 6:
 The date times table was extracted from the s3 link, and cleaned using advanced python methods, also the .jason response was returned to a dataframe pandas. It was finally uploaded to the sales_data database using the upload engine method as all the tables in that database ubnder the name 'dim_date_times' in pgAdmin4. The column 'date_uuid is used to set the primary key.

 ## MILESTONE 3: SQL Star_Based Schema of the Database
 
 ![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/20eb3342-55df-4df4-a7e3-ce64665c5dc6)

```sql

--- Task 1: Casting the column of the orders_table to the correct data types

SELECT length(max(cast(card_number as Text)))
FROM orders_table
GROUP BY card_number
ORDER BY length(max(cast(card_number as Text))) desc
LIMIT 1

SELECT length(max(cast(store_code as Text)))
FROM orders_table
GROUP BY card_number
ORDER BY length(max(cast(card_number as Text))) desc
LIMIT 1

SELECT length(max(cast(product_code as Text)))
FROM orders_table
GROUP BY product_code     
ORDER BY length(max(cast(product_code as Text))) desc
LIMIT 1

 TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
	ALTER COLUMN product_quantity TYPE SMALLINT;
   execute(sql) output:
 The data types of the orders_table have been changed corresponding to those seen in the table below:
+------------------+--------------------+--------------------+
|   orders_table   | current data type | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR (19)       |
| store_code       | TEXT               | VARCHAR (12)       |
| product_code     | TEXT               | VARCHAR (11)       |
| product quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+  


--- Displaying all the table in the sales_data DATABASE

SELECT * FROM public.orders_table

SELECT * FROM public.dim_users

SELECT * FROM public.dim_products

SELECT * FROM public.dim_store_details

SELECT * FROM public.dim_date_details

SELECT * FROM public.dim_card_details

--- To check what is missing in dim_products to correct and apply easily foreign key  orders_table_product_code_fkey 
SELECT * FROM orders_table
WHERE product_code NOT IN (SELECT product_code from dim_products);
--- To check what is missing in dim_users to correct and apply easily foreign key  orders_table_user_uuid_fkey 
SELECT * FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid from dim_users);
--- To check what is missing in dim_card_details to correct and apply easily foreign key  orders_table_card_number_fkey 
SELECT * FROM orders_table
WHERE user_uuid NOT IN (SELECT dim_users.user_uuid FROM dim_users);

---Task 2: Casting the column of the dim_users_table to the correct data types

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE
        USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN user_uuid TYPE UUID
        USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE;
    execute(sql) output:
 The data types of the dim_users table have been changed corresponding to those seen in the table below:   
    +----------------+--------------------+--------------------+
| dim_user_table | current data type | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR (255       |
| last_name      | TEXT               | VARCHAR (255)      |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(3)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |
+----------------+--------------------+--------------------+

Task 3: Updating dim_stores_details table with the latitude columns merged in one, the row representing the business's website has been changed from NULL to N/A in the location column as follow:

SELECT length(max(cast(store_code as Text)))
FROM dim_store_details
GROUP BY store_code     
ORDER BY length(max(cast(store_code as Text))) desc
LIMIT 1 

SELECT length(max(cast(country_code as Text)))
FROM dim_store_details
GROUP BY country_code     
ORDER BY length(max(cast(country_code as Text))) desc
LIMIT 1 

UPDATE dim_store_details
SET latitude = COALESCE(latitude || lat, latitude);

ALTER TABLE dim_store_details
DROP COLUMN lat;

-- Find the longest store code length
SELECT MAX(LENGTH(store_code::TEXT)) FROM dim_store_details
SET LIMIT 1; --12

-- Find the longest country code length
SELECT MAX(LENGTH(country_code::TEXT)) FROM dim_store_details
SET LIMIT 1; --2

-- Used to find columns with N/A VALUES
SELECT * FROM public.dim_store_details
WHERE address = 'N/A';

-- Updates N/A values into NULL
UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';

UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_details
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';

ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT
        USING staff_numbers::smallint,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255),
    ALTER COLUMN longitude TYPE FLOAT
        USING longitude::FLOAT,
    ALTER COLUMN latitude TYPE FLOAT
        USING latitude::FLOAT; 

Then the data types of the dim_stores_details table have been set corresponding to those seen in the table below:
+---------------------+-------------------+------------------------+
| store_details_table | current data type |   required data type   |
+---------------------+-------------------+------------------------+
| longitude           | TEXT              | FLOAT                  |
| locality            | TEXT              | VARCHAR (255)           |
| store_code          | TEXT              | VARCHAR(12)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR (255) NULLABLE |
| latitude            | TEXT              | FLOAT                  |
| country_code        | TEXT              | VARCHAR(2)             |
| continent           | TEXT              | VARCHAR(255)           |
+---------------------+-------------------+------------------------+

---Task 4: Changes and updates in the dim_products table to ease the delivery team work have been made as follow:

 -- Removing £ sign in product_price
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Adding a weight_class column
ALTER TABLE dim_products
    ADD weight_class VARCHAR(14); 

And then we apply the syntax codes to meet the delivery Team requirements:
+--------------------------+-------------------+
| weight_class VARCHAR(?) | weight range(kg) |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+

UPDATE dim_products
SET weight_class = CASE
WHEN weight_kg < 2 then 'Light'
WHEN weight_kg >= 2 AND weight_kg < 40 then 'Mid_Sized'
WHEN weight_kg >= 40 AND weight_kg < 140 then 'Heavy'
WHEN weight_kg >= 140 then 'Truck_Required'
ELSE NULL
END;

 TASK 5: Update the Products_table has been updated with all the columns cleaned and new column created. From the changes columns have been casted to the required data types as follow:

-- Rename removed column into still_available and weight(kg)
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;
-- Rename
ALTER TABLE dim_products
    RENAME COLUMN weight TO weight_kg;

-- Find the longest product code length
SELECT MAX(LENGTH(product_code)) FROM dim_products
SET LIMIT 1; --11

-- Find the longest EAN length
SELECT MAX(LENGTH("EAN")) FROM dim_products
SET LIMIT 1; --17
--- Find the longest weight_class
SELECT MAX(LENGTH(weight_class)) FROM dim_products
SET LIMIT 1;
-- Alter column data types
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT
    USING product_price::FLOAT,
ALTER COLUMN weight_kg TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID
    USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOLEAN
    USING CASE still_available
    WHEN 'Still_available' THEN TRUE 
    WHEN 'Removed' THEN FALSE
    ELSE NULL
    END;  

execute(sql) output:
 The data types of the dim_products table have been changed corresponding to those seen in the table below:
+-----------------+--------------------+--------------------+
| dim_products   | current data type   | required data type |
+-----------------+--------------------+--------------------+
| product_price   | TEXT               | FLOAT              |
| weight          | TEXT               | FLOAT              |
| EAN             | TEXT               | VARCHAR(17)         |
| product_code    | TEXT               | VARCHAR(11)         |
| date_added      | TEXT               | DATE               |
| uuid            | TEXT               | UUID               |
| still_available | TEXT               | BOOL               |
| weight_class    | TEXT               | VARCHAR(14)         |
+-----------------+--------------------+--------------------+
       

 Task 6: Updating the dim_date_details table. And changes into dim_date_times table columns have been made to the correct data types as follow:

-- Find the longest month length
SELECT MAX(LENGTH(month::TEXT)) FROM dim_date_times
SET LIMIT 1; --2

-- Find the longest year length
SELECT MAX(LENGTH(year::TEXT)) FROM dim_date_times
SET LIMIT 1; --4

-- Find the longest day length
SELECT MAX(LENGTH(day::TEXT)) FROM dim_date_times
SET LIMIT 1; --2

-- Find the longest time_period length
SELECT MAX(LENGTH(time_period::TEXT)) FROM dim_date_times
SET LIMIT 1; --10

-- Alter column data types
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID
    USING date_uuid::uuid;
 The data types of the dim_date_times table have been changed corresponding to those seen in the table below:
+-----------------+-------------------+--------------------+
| dim_date_times  | current data type | required data type |
+-----------------+-------------------+--------------------+
| month           | TEXT              | VARCHAR(2)         |
| year            | TEXT              | VARCHAR(4)         |
| day             | TEXT              | VARCHAR(2)         |
| time_period     | TEXT              | VARCHAR(10         |
| date_uuid       | TEXT              | UUID               |
+-----------------+-------------------+--------------------+


--TASK 7: Updating the dim_card_details table. Changes of dim-card-details table column into the correct data types have been made as follow:

-- Find the longest card_number length
SELECT MAX(LENGTH(card_number)) FROM dim_card_details
SET LIMIT 1; --- >  19

-- Find the longest expiry_date length
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details
SET LIMIT 1; --- > 10

-- Alter column data types
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE;

execute(sql) output:
The data types of the dim_card_details table have been changed corresponding to the right as in the table below:
+------------------------+-------------------+--------------------+
|    dim_card_details    | current data type | required data type |
+------------------------+-------------------+--------------------+
| card_number            | TEXT              | VARCHAR(19)        |
| expiry_date            | TEXT              | VARCHAR(10)        |
| date_payment_confirmed | TEXT              | DATE               |
+------------------------+-------------------+--------------------+

Task 8: The Primary Keys in each dimension tables have been created with its columns matching the same columns inside the orders table using sql as follow:

1/- Applying dim_users primary key:

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

2/- Applying primary key to the dim_card_details' column card_number: 
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

Note:This operation has been donne to overcome the issue to apply the key, after these codes execution the key was apply successfully to the dim_card_details.*
SELECT date_payment_confirmed, count (*)
FROM dim_card_details
GROUP BY date_payment_confirmed
HAVING count (*) > 1;

DELETE FROM dim_card_details
WHERE date_payment_confirmed IN (
  SELECT date_payment_confirmed
  FROM dim_card_details
  GROUP BY date_payment_confirmed
  HAVING COUNT(*) > 1
);

DELETE FROM dim_card_details
WHERE date_payment_confirmed IS NULL;

3/- Applying primary key to the dim_store_details' column store_code:
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

4/- Applying primary key to the dim_date_times column date_uuid: 
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

5/- Applying primary key to the dim_products' column product_code
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);
Note: These syntax codes have been run to overcome the issue applying the key. After these changes the primary key has been apply to dim_product table successfully.
INSERT INTO dim_products (product_code) 
    VALUES ('l1-2836416D');
INSERT INTO dim_products (product_code) 
    VALUES ('M6-7203684r');

Task 9: With the primary keys created in all the dim prefixed tables. There is to create a foreign key to connect the tables using the SQL syntax codes. The foreign keys constraints are applied in the orders_table to reference the primary keys in the columns of the dim tables.This has made the star-based database schema complete.

1/-
ALTER TABLE orders_table
ADD FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

2/-
ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid); 

3/-
ALTER TABLE orders_table
ADD FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

4/-
ALTER TABLE orders_table
ADD FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

5/-
ALTER TABLE orders_table
ADD FOREIGN KEY (card_number)
REFERENCES dim_date_times (date_uuid);
Note: AS there is an issue to the code above, the below syntax codes were run to overcome the problem: 
SELECT orders_table.store_code
	FROM orders_table
	LEFT JOIN dim_store_details
	ON orders_table.store_code = dim_store_details.store_code
	WHERE dim_store_details.store_code IS NULL;

INSERT INTO dim_store_details(store_code)
	SELECT DISTINCT orders_table.store_code
	FROM orders_table
	WHERE orders_table.store_code NOT IN 
		(SELECT dim_store_details.store_code
		FROM dim_store_details);
Finally, the Foreign Keys have been added to the orders_table successfully to get the relations between the orders_table to the other 5 dimension tables (respectively: orders_table_product_code_fkey, orders_table_card_number_fkey, orders_table_user_uuid_fkey, orders_table_date_uuid_fkey, orders_table_store_code_fkey) in form of SQL star_based SCHEMA (reference database_star_based_schema.pgerd).

```
![image](C:\Users\vicky\OneDrive\Documents\AiCore_Data-Engineering\Foreign_Keys_Orders_table.png)

![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/9109456c-af70-4467-a211-0c81f87dd5cf)


## MILESTONE 4: Querying The Data

![image](https://github.com/JesusAs2019/multinational-retail-data-centralisation335/assets/56179535/aecc3dbb-350f-4cf8-bf67-9cbb3c800910)

```
# MILESTONE 4: CREATING THE STAR_BASED SCHEMA DATABASE

---
	
#Task 1: How many stores does the business have and in which  countries?
---
SELECT country_code, count(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY country_code;

---sql execute output:
+-------------+-------------------+
|"country_code"|"total_no_stores" |
+--------------+----------------- +
   "DE",             "141"
   "GB",             "266"
   "US",             "34"


# Task 2: Which locations have the most stores?

---
	
SELECT locality, count(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 10;

------sql execute output:

"locality","total_no_stores"
"Chapletown",    "14"
"Belper",         "13"
"Bushey",         "12"
"Exeter",         "11"
"Rutherglen",     "10"
"Arbroath",       "10"
"High Wycombe",   "10"
"Surbiton",        "9"
"Lancing",         "9"
"Aberdeen",        "9"

#Task 3: Which months produced the lagest amount of sales?

---
	
SELECT
	ROUND(CAST(SUM(spend) AS numeric), 2) AS total_sales,
	month
FROM (
	SELECT
		ord.product_quantity * prod.product_price AS spend,
		dt.month
	FROM orders_table ord
	INNER JOIN dim_date_times dt
		ON ord.date_uuid = dt.date_uuid
	INNER JOIN dim_products prod
		ON ord.product_code = prod.product_code
) x
GROUP BY month
ORDER BY total_sales DESC;

---


# Task 4: How many sales are coming from Online?

---
SELECT
	COUNT(*) AS numbers_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	location
FROM (
	SELECT
		ord.product_quantity,
		CASE
			WHEN st.store_type = 'Web Portal' THEN 'Web'
			ELSE 'Offline'
		END AS location
	FROM orders_table ord
	INNER JOIN dim_store_details st
		ON ord.store_code = st.store_code
) x
GROUP BY location
ORDER BY location DESC;
---

# Task 5: Find out the total and percentage of sales coming from each of the different store types is given by the following sql syntax code:

	-----	
SELECT
	store_type,
	ROUND(CAST(SUM(sale) AS NUMERIC), 2) AS total_sales,
	ROUND(CAST(100 * SUM(sale) / total AS NUMERIC), 2) AS "percentage_total(%)"
FROM (
	SELECT
		st.store_type,
		ord.product_quantity * prod.product_price AS sale
	FROM orders_table ord
	INNER JOIN dim_store_details st
		ON ord.store_code = st.store_code
	INNER JOIN dim_products prod
		ON ord.product_code = prod.product_code
) x
CROSS JOIN (
	SELECT SUM(ord.product_quantity * prod.product_price) AS total
	FROM orders_table ord
	INNER JOIN dim_products prod
	ON ord.product_code = prod.product_code
) y
GROUP BY x.store_type, y.total
ORDER BY total_sales DESC;
---

# Task 6: Is to find in which months and in which years have had the Company has produced the highest cost of sales historically:
	
SELECT
	ROUND(CAST(SUM(sale) AS NUMERIC), 2) AS total_sales,
	year,
	month
FROM (
	SELECT
		dt.year,
		dt.month,
		ord.product_quantity * prod.product_price AS sale
	FROM orders_table ord
	INNER JOIN dim_products prod
		ON ord.product_code = prod.product_code
	INNER JOIN dim_date_times dt
		ON ord.date_uuid = dt.date_uuid
) x
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;

---
# Task 7: THe Staff Headcount Query is about to determine the overall staff numbers in each location around the world:
	
SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

# Task 8: Which German stores type is saling the most?
	
SELECT
	ROUND(CAST(SUM(sale) AS NUMERIC), 2) AS total_sales,
	store_type,
	country_code
FROM (
SELECT
	st.store_type,
	st.country_code,
	ord.product_quantity * prod.product_price AS sale
FROM orders_table ord
INNER JOIN (
	SELECT store_code, store_type, country_code
	FROM dim_store_details
	WHERE country_code = 'DE'
	) st
	ON ord.store_code = st.store_code
INNER JOIN dim_products prod
	ON ord.product_code = prod.product_code
) x
GROUP BY
	store_type,
	country_code
ORDER BY
	total_sales;
----
	
#Task 9: The accurate metric for how quickly the company is making sales is determineed by the average time taken between each sale grouped by year.

--------
ALTER TABLE dim_date_times
ADD COLUMN time_diff interval;

UPDATE dim_date_times
SET time_diff = x.time_diff
FROM (
  SELECT timestamp, timestamp, LAG(timestamp) OVER (ORDER BY timestamp) AS time_diff
  FROM dim_date_times
) AS x
WHERE dim_date_times.timestamp = x.timestamp;

# After creating the column time difference, the task query on how quickly the Company is making sales is now much more straightforward with the query syntax below:
----------
WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD H:M:S:SSS') as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte
) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;
-----sql execute is as follow:
 +------+---------------------------------------------------------------+
 | year |                           actual_time_taken                   |
 +------+---------------------------------------------------------------+
 | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "milliseconds": 793 |
 | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "milliseconds": 557 |
 | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "milliseconds": 222 | 
 | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "milliseconds": 348 |
 | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "milliseconds": 438 |
 +------+---------------------------------------------------------------+

```
10. [Lesson Learned](#lesson-learned)

This project helped me understand really the duty of Data Engineer and his daily challenges as developping Data Engineering skills through real-world practical applications is time consuming with constant methods reviews and iterations for to make it works complex code syntax are path with many challenges; therefore, with dedication, commibttement, and constency from the the different processing tools and methods we have >overcomed them, and know how to:

>Create data pipelines from a variety of sources.
>Apply ETL(extraction-Cleaning-loading) to data through Python functions.
>Develop a star-based database schema with and correcting data types.
>Up-to-date data information using SQL queries.
    
12. [Liscense Information](#liscense-information)
This project is open-source and available under the MIT License.

In this project, we create a local PostgreSQL database. We upload data from various sources, process it, create a database schema and run SQL queries.
Key technologies used: Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas).
Abr. Mil = Milestone

13. [Acknowledgement](#acknowledgement)    
> My gratitude to the AiCore engineers and support team for their invaluable dedicated support and guidance. They have continuously provided assistance and were always there willing and passionate to help in any way as they could all over the project.
