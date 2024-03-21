from utils import *
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# DB credentials
host_name = os.getenv("HOST_NAME")
dbname = "dbpg1_name"
port = "5432"
username = "dbpg1"
password = os.getenv("PASSWORD")
conn = None   

# Create df from csv files
files = get_csv_files()
df = create_df(files)

# Process the df dict
for key, value in df.items():
    # clean files names
    cleaned_key = clean_table_name(key)
    
    # assign cleaned_key to df dictionary key
    df[cleaned_key] = df.pop(key)
    
    # clean column names
    cleaned_columns, col_str = clean_column_names(value)
    
    # assign cleaned_columns to DataFrame
    value.columns = cleaned_columns
 
    # connect to DB
    conn = connect_to_db(host_name, dbname, port, username, password)
    
    # upload to DB
    upload_to_db(conn, cleaned_key, col_str, value, cleaned_columns)