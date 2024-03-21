import pandas as pd
from pathlib import Path
import psycopg2 as ps


### 1. FINDS CSV IN DIRECTORY
path = Path.cwd() / "data"
files = [file for file in path.glob("*.csv")]

### 2. CREATE DF FROM THE CSV FILES
df = {}
for file in files:
    try:
        df[file.stem] = pd.read_csv(path / file)
    except UnicodeDecodeError:
        df[file.stem] = pd.read_csv(path / file, encoding="ISO-8859-1")

### 3. CLEAN TABLE AND COLUMN NAMES - CREATE TABLE SQL STATEMENT - COPY CSV TO DB
for key, value in df.items():

    # Clean files names
    cleaned_key = (
        key.lower()
        .replace(" ", "_")
        .replace("?", "")
        .replace("$", "")
        .replace("-", "_")
        .replace(r"/", "_")
        .replace("\\", "_")
        .replace("%", "pct")
        .replace(")", "_")
        .replace("(", "_")
    )

    # Assign the cleaned file name back to the dictionary key
    df[cleaned_key] = df.pop(key)

    # Clean column names
    cleaned_columns = [
        col.lower()
        .replace(" ", "_")
        .replace("?", "")
        .replace("$", "")
        .replace("-", "_")
        .replace(r"/", "_")
        .replace("\\", "_")
        .replace("%", "pct")
        .replace(")", "_")
        .replace("(", "_")
        for col in value.columns
    ]

    # Assign the cleaned column names back to the DataFrame
    value.columns = cleaned_columns

    # Replacement dict that maps pandas to sql dtypes
    replacements = {
        "object": "varchar",
        "float64": "float",
        "int64": "int",
        "datetime64": "timestamp",
        "timedelta64[ns]": "varchar",
    }

    # Table schema
    result = ", ".join(
        [f"{i} {j}" for i, j in zip(value.columns, value.dtypes.replace(replacements))]
    )

    # Create conn with PG DB in AWS
    host_name = "db-pg-emherrer1.cx4as4o8woit.us-east-1.rds.amazonaws.com"
    dbname = "dbpg1_name"
    port = "5432"
    username = "dbpg1"
    password = "Aloevera3254"
    conn = None

    conn = ps.connect(
        host=host_name, database=dbname, port=port, user=username, password=password
    )
    cursor = conn.cursor()

    # Drop tables with the same name
    cursor.execute(f"drop table if exists {cleaned_key};")

    # Create tables, columns names and type
    cursor.execute(f"create table {cleaned_key} ({result})")

    # Save each df into csv
    clean_path = Path.cwd() / "data_clean" / f"{cleaned_key}.csv"
    value.to_csv(clean_path, header=value.columns, index=False, encoding="utf-8"
    )

    # Open csv and save it as an object
    my_file = open(clean_path)
    print("File opened in memory")

    # Upload csv file into PG AWS DB
    SQL_STATEMENT = f"COPY {cleaned_key} FROM STDIN WITH CSV HEADER DELIMITER AS ','"

    cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
    print("File copied to db in AWS")

    # Modify permission to public
    cursor.execute(f"grant select on table {cleaned_key} to public")
    
    # Commit and close cursor/connection
    conn.commit()
    cursor.close()
    print(f"Table {cleaned_key} imported to db completed")
    conn.close()

print("All tables have been successfully copied to PG DB at AWS")