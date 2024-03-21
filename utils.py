from pathlib import Path
import psycopg2 as ps
import pandas as pd


# Finds csv in directory
def get_csv_files():
    path = Path.cwd() / "data"
    files = [file for file in path.glob("*.csv")]
    return files


# Create df dict from csv list
def create_df(files):
    path = Path.cwd() / "data"
    df = {}
    for file in files:
        try:
            df[file.stem] = pd.read_csv(path / file)
        except UnicodeDecodeError:
            df[file.stem] = pd.read_csv(path / file, encoding="ISO-8859-1")
    return df


# Clean files names
def clean_table_name(filename):
    cleaned_key = (
        filename.lower()
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
    return cleaned_key


# Clean column names
def clean_column_names(filename):
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
        for col in filename.columns
    ]

    # Replacement dict that maps pandas to sql dtypes
    replacements = {
        "object": "varchar",
        "float64": "float",
        "int64": "int",
        "datetime64": "timestamp",
        "timedelta64[ns]": "varchar",
    }

    # Table schema
    col_str = ", ".join(
        [
            f"{i} {j}"
            for i, j in zip(cleaned_columns, filename.dtypes.replace(replacements))
        ]
    )

    return cleaned_columns, col_str


# Create conection with postgres DB in AWS
def connect_to_db(host_name, dbname, port, username, password):
    try:
        conn = ps.connect(
            host=host_name, database=dbname, port=port, user=username, password=password
        )
    except ps.OperationalError as e:
        raise e
    else:
        print("Connected!")
    return conn

# Upload files to db
def upload_to_db(conn, table_name, col_str, df, df_columns):
    cursor = conn.cursor()
    cursor.execute(f"drop table if exists {table_name};")
    cursor.execute(f"create table {table_name} ({col_str})")

    clean_path = Path.cwd() / "data_clean" / f"{table_name}.csv"
    df.to_csv(clean_path, header=df_columns, index=False, encoding="utf-8")

    my_file = open(clean_path)
    print("File opened in memory")

    SQL_STATEMENT = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
    print("File copied to db in AWS")

    cursor.execute(f"grant select on table {table_name} to public")

    conn.commit()
    cursor.close()
    print(f"Table {table_name} imported to db completed")
    conn.close()
