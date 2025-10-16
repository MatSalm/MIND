import os
import pickle
import pyodbc
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load the dataframe from the .pkl file
current_dir = os.getcwd()
pkl_file_path = os.path.join(current_dir, 'temp_data.pkl')

with open(pkl_file_path, 'rb') as f:
    df = pickle.load(f)

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

# Construct the connection string (with 60-second timeout)
conn_stringCWS = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"PORT={port};"
    f"DATABASE={databaseCWS};"
    f"UID={username};"
    f"PWD={password};"
    "Timeout=60"
)

# Retry logic: up to 4 attempts to connect
MAX_ATTEMPTS = 4
conn = None
for attempt in range(MAX_ATTEMPTS):
    try:
        print(f"Attempt {attempt + 1} to connect to the database...")
        conn = pyodbc.connect(conn_stringCWS)
        print("Successfully connected to the database.")
        break  # Exit the loop on successful connection
    except pyodbc.Error as e:
        print(f"Connection attempt {attempt + 1} failed: {e}")
        if attempt < MAX_ATTEMPTS - 1:
            print("Retrying...")
        else:
            print("Failed to connect after 4 attempts. Exiting script.")
            exit(1)

try:
    # Create a SQL query
    sql_query = "SELECT * FROM SYSTEM.view_client_episode_history"

    # Use pandas to execute the SQL query and store the result in a DataFrame
    client_episode_history_df = pd.read_sql(sql_query, conn)
    print("client_episode_history_df loaded successfully")

finally:
    if conn:
        conn.close()

# Create a mapping from 'PATID' and 'EPISODE_NUMBER' to 'date_of_discharge' 
# for records where 'date_of_discharge' is not null
mapping = (
    client_episode_history_df[
        client_episode_history_df['date_of_discharge'].notnull()
    ]
    .set_index(['PATID', 'EPISODE_NUMBER'])['date_of_discharge']
)

# Create a copy of 'order_stop_eff_date' before the operation
df['order_stop_eff_date_before'] = df['order_stop_eff_date']

# Replace 'order_stop_eff_date' in df with 'date_of_discharge' where available
df['order_stop_eff_date'] = df.set_index(['PATID', 'EPISODE_NUMBER']).index.map(mapping)
df['order_stop_eff_date'] = df['order_stop_eff_date'].fillna(df['order_stop_eff_date_before'])

# Count how many rows were updated
affected_records = df[df['order_stop_eff_date'] != df['order_stop_eff_date_before']].shape[0]
print(f"Number of records in df affected by adjusting prescription with discharge dates: {affected_records}")

# Drop the 'order_stop_eff_date_before' column
df = df.drop(columns=['order_stop_eff_date_before'])

# Explode the admin_hrs_default column to show times a med is to be taken
df['admin_hrs_default'] = (
    df['admin_hrs_default']
    .str.split(' - ')
    .apply(lambda x: [item.strip() for item in x if item.strip()])
)
df = df.explode('admin_hrs_default').reset_index(drop=True)

# Convert the admin_hrs_default into military time
def convert_to_military(time_str):
    try:
        return datetime.strptime(time_str, "%I:%M %p").strftime("%H:%M")
    except ValueError:
        return time_str

df['admin_hrs_default'] = df['admin_hrs_default'].apply(convert_to_military)

# Explode the days_administered_code column to show days a med is to be taken
df['days_administered_code'] = df['days_administered_code'].str.split('&')
df = df.explode('days_administered_code').reset_index(drop=True)

# Convert order start/stop times to military time
df['order_start_time'] = df['order_start_time'].apply(convert_to_military)
df['order_stop_eff_time'] = df['order_stop_eff_time'].apply(convert_to_military)

# Remove the existing .pkl file if it exists
if os.path.exists(pkl_file_path):
    os.remove(pkl_file_path)

# Save the updated dataframe to the .pkl file
with open(pkl_file_path, 'wb') as f:
    pickle.dump(df, f)

print(f"Dataframe saved to {pkl_file_path}")
