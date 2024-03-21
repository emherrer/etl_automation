import pandas as pd
import numpy as np
import glob
import psycopg2 as ps


### Steps
# - Import csv files into pandas dataframe
# - Clean the table name and remove extra symbols, spaces, capital letters
# - Clean columns headers and remove all extra symbols, spaces, capital letters
# - Write the create table SQL statement
# - Import the data into the db

### 1.Import data to dataframe

df = pd.read_csv("data//Customer Contracts$.csv")
df.head()

### 2. Clean table names
# lower case letter
# remove all white spaces and $
# replace -, /, \\, $, with _

file = "Customer Contracts$"
clean_table_name = file.lower().replace(" ", "_").replace("?", "").replace("$", "")\
    .replace("-", "_").replace(r"/", "_").replace("\\", "_").replace("%", "_")\
    .replace(")", "_").replace("(", "_")
clean_table_name


### 3. Clean the headers
# lower case letter
# remove all white spaces and $
# replace -, /, \\, $, with _

df.columns = [x.lower().replace(" ", "_").replace("?", "").replace("$", "")\
    .replace("-", "_").replace(r"/", "_").replace("\\", "_").replace("%", "_")\
    .replace(")", "_").replace("(", "_") for x in df.columns]
df.columns

### 4. Write the create table SQL statement
# we must map the python data types to SQL

# create table customer_contracts
# (
#     customer_name      varchar, 
#     start_date         varchar,
#     end_date           varchar,
#     contract_amount_m  float64,
#     invoice_sent       varchar,
#     paid               varchar
# );
df.dtypes

replacements = {
    "object": "varchar",
    "float64": "float",
    "int64" : "int",
    "datetime64": "timestamp",
    "timedelta64[ns]": "varchar"
}
# Need to replace the python dtypes with the SQL dtypes
# For each column and its dtype.

result = ', '.join([f"{i} {j}" for i, j in zip(df.columns, df.dtypes.replace(replacements))])


### 5. Create conection with postgres DB in AWS
def connect_to_db(host_name, dbname, port, username, password):
    try:
        conn = ps.connect(host=host_name, 
                          database=dbname, 
                          port=port, 
                          user=username, 
                          password=password)
    except ps.OperationalError as e:
        raise e
    else:
        print("Connected!")
    return conn

host_name = "db-pg-emherrer1.cx4as4o8woit.us-east-1.rds.amazonaws.com"
dbname = "dbpg1_name"
port = "5432"
username = "dbpg1"
password = "Aloevera3254"
conn = None

conn = connect_to_db(host_name, dbname, port, username, password)
cursor = conn.cursor()

### 6. Working with PG DB in AWS
# Drop tables with the same name
cursor.execute("drop table if exists customer_contracts;")

# Create the tables
cursor.execute("create table customer_contracts \
    (customer_name varchar, start_date varchar, end_date varchar, \
    contract_amount_m float, invoice_sent varchar, paid varchar)")

# Insert into table 
# first: save df to csv
df.to_csv("customer_contracts.csv", header=df.columns, index=False, encoding="utf-8")

# second: open csv and save it as an object
my_file = open("customer_contracts.csv")
print("file opened in memory")

#Upload csv to DB
SQL_STATEMENT = """
COPY customer_contracts FROM STDIN WITH
    CSV
    HEADER
    DELIMITER AS ','
"""
cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
print("File copied to db in AWS")

# Modify permission to public
cursor.execute("grant select on table customer_contracts to public")
conn.commit()

cursor.close()
print("table customer_contracts imported to db completed")
conn.close()