import os
import pyodbc
import pandas as pd
import pickle
import json
from dotenv import load_dotenv
import sys
import time

# Load environment variables
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Define the SQL query
sql_query = """
SELECT *
FROM eMAR.eMAR_order_data
WHERE rou_prn_other_code = 'R'
AND tx_setting_code = 'I'
AND order_start_date <= order_stop_eff_date
AND v_client_curr_unit_value IS NOT NULL
"""

def fetch_emar_data(conn_string, max_retries=8, timeout=60):
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Open a connection
            conn = pyodbc.connect(conn_string)

            # Execute the query and store the result in a pandas DataFrame
            df = pd.read_sql_query(sql_query, conn)

            # Close the connection
            conn.close()

            return df

        except pyodbc.OperationalError as e:
            if "timeout" in str(e).lower():
                retry_count += 1
                print(f"Timeout occurred. Retrying {retry_count}/{max_retries}...")
                time.sleep(5)  # Wait for 5 seconds before retrying
            else:
                raise e

    raise Exception("Failed to connect to the database after multiple retries due to timeout.")

def main(data_file, param_file):
    try:
        # Load data from data file if it exists
        if os.path.exists(data_file):
            with open(data_file, 'rb') as f:
                data = pickle.load(f)
        else:
            data = None

        # Read parameters from param file
        with open(param_file, 'r') as f:
            parameters = json.load(f)

        if not isinstance(parameters, dict):
            raise ValueError("Parameters should be a dictionary.")

        # Get database credentials from environment variables
        server = os.getenv('database_server')
        port = os.getenv('database_port')
        database = os.getenv('databaseCWS')
        username = os.getenv('database_username')
        password = os.getenv('database_password')
        driver_name = os.getenv('database_driver_name')

        # Verify environment variables
        if not all([server, port, database, username, password, driver_name]):
            raise ValueError("One or more environment variables are missing.")

        # Construct the connection string
        conn_string = (f'DRIVER={{{driver_name}}};SERVER={server};PORT={port};'
                       f'DATABASE={database};UID={username};PWD={password};Timeout=60')

        # Fetch eMAR data
        df = fetch_emar_data(conn_string)

        # Save the result to the data file
        with open(data_file, 'wb') as f:
            pickle.dump(df, f)

        print(f"Data saved to {data_file}")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    # Ensure proper usage
    if len(sys.argv) != 3:
        print("Usage: python 00.py <data_file> <param_file>")
        sys.exit(1)

    data_file = sys.argv[1]
    param_file = sys.argv[2]

    main(data_file, param_file)
