import psycopg2
import os
import pandas as pd
import dotenv

# Load environment variables
dotenv.load_dotenv(override=True)

# Function to establish a database connection
def loading_layer(**kwargs):

    # Reload .env file to ensure updated environment variables
    dotenv.load_dotenv(override=True)

    # Load transformed data from XCom
    ti = kwargs['ti']
    response =ti.xcom_pull(key="transformed_data", task_ids="transformed_results")
    

    # Connection details
    local_conn_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    azure_conn_params = {
        'dbname': os.getenv('AZURE_DB_NAME'),
        'user': os.getenv('AZURE_DB_USER'),
        'password': os.getenv('AZURE_DB_PASSWORD'),
        'host': os.getenv('AZURE_DB_HOST'),
        'port': os.getenv('AZURE_DB_PORT')
    }

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

    def close_previous_connection(conn):
        if conn:
            try:
                conn.close()
                print("Previous connection closed successfully.")
            except Exception as e:
                print(f"Error closing connection: {e}")

    # Insert data into both databases
    for conn_params in [local_conn_params, azure_conn_params]:
        db_name = conn_params['dbname']
        conn = None

        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # Create table if it doesn't exist
            print(f"Creating table in database: {db_name}")
            cursor.execute(CREATE_TABLE_SQL)
            conn.commit()

            # Insert data from CSV (excluding the id column)
            print(f"Inserting data into database: {db_name}")
            with open(csv_path, 'r') as f:
                cursor.copy_expert(
                    """
                    COPY train_schedule (
                        request_date_time, station_name, mode, train_uid, origin_name, operator_name, 
                        platform, destination_name, aimed_departure_time, expected_departure_time, 
                        best_departure_estimate_mins, aimed_arrival_time
                    )
                    FROM STDIN WITH CSV HEADER
                    """,
                    f
                )

            conn.commit()
            print(f"Data inserted successfully {db_name}")

        except psycopg2.OperationalError as e:
            print(f"Database not connected error for {db_name}: {e}")

        except Exception as e:
            print(f"Data not inserted into {db_name}: {e}")
            conn.rollback()

        finally:
            if cursor:
                cursor.close()
            close_previous_connection(conn)
            print(f"{db_name} connection closed.")


if __name__ == "__main__":
    loading_layer()