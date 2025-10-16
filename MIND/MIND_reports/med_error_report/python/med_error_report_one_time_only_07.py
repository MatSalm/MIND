import sys
import os
import pickle
import pandas as pd
import json
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Determine if running in a Jupyter notebook
if 'ipykernel' in sys.modules:
    data_file = os.path.join(os.getcwd(), 'temp_data.pkl')
else:
    data_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), 'temp_data.pkl')

# Load the dataframe from the .pkl file
with open(data_file, 'rb') as f:
    calendar_df = pickle.load(f)

# Load parameters from JSON file
json_file = os.path.join(os.getcwd(), 'temp_params.json')
with open(json_file, 'r') as file:
    params = json.load(file)

calendar_start_date = params.get('calendar_start_date')
calendar_stop_date = params.get('calendar_stop_date')

# Validate the date format (optional but recommended)
date_format = "%Y-%m-%d"

try:
    datetime.strptime(calendar_start_date, date_format)
    datetime.strptime(calendar_stop_date, date_format)
except ValueError:
    raise ValueError("Incorrect date format, should be YYYY-MM-DD")

# Database connection details from environment variables
server = os.getenv('database_server')
port = os.getenv('database_port')
databaseCWS = os.getenv('databaseCWS')
username = os.getenv('database_username')
password = os.getenv('database_password')
driver = os.getenv('database_driver_name')

# Ensure the driver is specified correctly
if not driver:
    raise ValueError("ODBC driver not specified in the environment variables.")

# Construct the connection string with timeout
conn_stringCWS = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"PORT={port};"
    f"DATABASE={databaseCWS};"
    f"UID={username};"
    f"PWD={password};"
    f"Timeout=60"  # Set query timeout to 60 seconds
)

# Retry parameters
max_retries = 4
retry_delay = 10  # seconds

# Attempt to connect to the database with retries
conn = None
for attempt in range(1, max_retries + 1):
    try:
        print(f"Attempt {attempt} to connect to the database...")
        conn = pyodbc.connect(conn_stringCWS)
        print("Database connection successful.")
        break  # Exit the loop if connection is successful
    except pyodbc.Error as e:
        print(f"Database connection failed on attempt {attempt}: {e}")
        if attempt < max_retries:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            print("Max retries reached. Exiting.")
            exit(1)

# Query the database if connection is successful
if conn:
    try:
        query = """
        SELECT PATID, admin_date_scheduled, scheduled_admin_time, order_number, order_unique_id
        FROM eMAR.eMAR_administration_data
        WHERE admin_date_scheduled BETWEEN ? AND ?
        AND scheduled_admin_time != 'N/A'
        """

        administration_df = pd.read_sql(query, conn, params=(calendar_start_date, calendar_stop_date))
        print("Data loaded successfully from database.")

    except pyodbc.Error as e:
        print(f"Error executing query: {e}")
        exit(1)
    finally:
        conn.close()

# Convert admin_date_scheduled to string format
administration_df['admin_date_scheduled'] = administration_df['admin_date_scheduled'].astype(str)

# Convert admin_date_scheduled and scheduled_admin_time to a single timestamp column
administration_df['scheduled_admin_timestamp'] = pd.to_datetime(
    administration_df['admin_date_scheduled'] + ' ' + administration_df['scheduled_admin_time'], 
    format='%Y-%m-%d %I:%M %p'
)

# Find all one-time-only medications in calendar_df
one_time_only_meds = calendar_df[calendar_df['one_time_only_code'] == 'Y']

# Initialize counters
removed_records_count = 0
retained_records_count = 0

# Remove all records for PATID and their medication from calendar_df if found in administration_df
for index, row in one_time_only_meds.iterrows():
    patid = row['PATID']
    order_number = row['order_number']
    order_unique_id = row['order_unique_id']
    
    # Check if there is an administration record for this medication
    admin_record = administration_df[
        (administration_df['PATID'] == patid) &
        (administration_df['order_number'] == order_number) &
        (administration_df['order_unique_id'] == order_unique_id)
    ]
    
    if not admin_record.empty:
        # Count the number of records to be removed
        count_to_remove = len(calendar_df[
            (calendar_df['PATID'] == patid) &
            (calendar_df['order_number'] == order_number) &
            (calendar_df['order_unique_id'] == order_unique_id)
        ])
        
        removed_records_count += count_to_remove

        # Remove all records for this PATID and medication from calendar_df
        calendar_df = calendar_df[
            ~((calendar_df['PATID'] == patid) &
              (calendar_df['order_number'] == order_number) &
              (calendar_df['order_unique_id'] == order_unique_id))
        ]
    else:
        # Remove all except the most recent record
        medication_records = calendar_df[
            (calendar_df['PATID'] == patid) &
            (calendar_df['order_number'] == order_number) &
            (calendar_df['order_unique_id'] == order_unique_id)
        ]
        
        # Get the most recent record
        most_recent_record = medication_records.loc[medication_records['datetime'].idxmax()]
        
        # Count the number of records to be removed
        count_to_remove = len(medication_records) - 1
        removed_records_count += count_to_remove
        
        # Drop all records for this PATID and medication
        calendar_df = calendar_df[
            ~((calendar_df['PATID'] == patid) &
              (calendar_df['order_number'] == order_number) &
              (calendar_df['order_unique_id'] == order_unique_id))
        ]
         
        # Add back the most recent record
        calendar_df = pd.concat([calendar_df, most_recent_record.to_frame().T], ignore_index=True)
        
        retained_records_count += 1

print(f"Number of records removed: {removed_records_count}")
print(f"Number of records retained as the most recent: {retained_records_count}")

# Save the updated calendar_df to the temp_data.pkl file
data = {}

# Update the data dictionary with the new dataframes
data['administration_df'] = administration_df
data['calendar_df'] = calendar_df

with open(data_file, 'wb') as f:
    pickle.dump(data, f)

print(f"Data saved to {data_file}")
