import psycopg2
import os
import pandas as pd
import dotenv

# Load environment variables
dotenv.load_dotenv(override=True)

# Function to establish a database connection
def loading_layer(**kwargs):
     
    ti = kwargs['ti']
    response =ti.xcom_pull(key="transformed_data", task_ids="transformed_results")
    

    return psycopg2.connect(
        host='localhost',
        dbname= 'velocityrailway_db_cron',
        user= 'joanna',
        password= 'Avokerie3',
        port=5432
    )

# Function to close connections
def close_connection(conn):
    try:
        if conn:
            conn.close()
            print(f"Closed connection to database")
    except Exception as e:
        print(f"Error closing connection: {e}")

    

# SQL Queries
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS train_schedule (
    id SERIAL PRIMARY KEY,
    request_date_time VARCHAR(100),
    station_name VARCHAR(100),
    mode VARCHAR(100),
    train_uid VARCHAR(100),
    origin_name VARCHAR(100),
    operator_name VARCHAR(100),
    platform VARCHAR(100),
    destination_name VARCHAR(100),
    aimed_departure_time VARCHAR(100),
    expected_departure_time VARCHAR(100),
    best_departure_estimate_mins VARCHAR(100),
    aimed_arrival_time VARCHAR(100)
);
"""

INSERT_SQL = """
INSERT INTO train_schedule (
    request_date_time, station_name, mode, train_uid, origin_name, operator_name, 
    platform, destination_name, aimed_departure_time, expected_departure_time, 
    best_departure_estimate_mins, aimed_arrival_time
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Function to load and prepare data from CSV
def load_data_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    # Ensure columns match the table definition
    expected_columns = [
        "request_date_time", "station_name", "mode", "train_uid", "origin_name", 
        "operator_name", "platform", "destination_name", "aimed_departure_time", 
        "expected_departure_time", "best_departure_estimate_mins", "aimed_arrival_time"
    ]
    df = df[expected_columns]
    return [tuple(row) for row in df.values]

# Function to create table and insert data
def store_data(data):
    connections = [
        loading_layer(os.getenv("DB_HOST"), os.getenv("DB_NAME"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_PORT")),
        loading_layer(os.getenv("AZURE_DB_HOST"), os.getenv("AZURE_DB_NAME"), os.getenv("AZURE_DB_USER"), os.getenv("AZURE_DB_PASSWORD"), os.getenv("AZURE_DB_PORT"))
    ]

    for conn in connections:
        try:
            with conn.cursor() as cur:
                # Create table
                print(f"Table created in database")
                cur.execute(CREATE_TABLE_SQL)
                conn.commit()

                # Insert data
                print(f"Data inserted into database")
                cur.executemany(INSERT_SQL, data)
                conn.commit()

                
        except Exception as e:
            print(f"Error in {conn.dsn}: {e}")
            conn.rollback()
        finally:
            close_connection(conn)

# Load data and store in databases
csv_path = "train_schedule.csv"
validated_data = load_data_from_csv(csv_path)
store_data(validated_data)

if __name__ == "__main__":
    loading_layer()