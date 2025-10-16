import os
import pickle
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from time import sleep

# Load environment variables
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Define the path to the data file
data_file = 'temp_data.pkl'

# Load the calendar_df from the .pkl file
if os.path.exists(data_file):
    with open(data_file, 'rb') as f:
        data = pickle.load(f)
        if 'calendar_df' in data:
            calendar_df = data['calendar_df']
            print("calendar_df loaded successfully:")
        else:
            raise KeyError("calendar_df not found in the loaded data.")
else:
    raise FileNotFoundError(f"{data_file} does not exist.")

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

# Construct the connection string with the timeout parameter
conn_stringCWS = (
    f'DRIVER={{{driver}}};'
    f'SERVER={server};'
    f'PORT={port};'
    f'DATABASE={databaseCWS};'
    f'UID={username};'
    f'PWD={password};'
    f'Timeout=60'
)

# Retry logic for connecting to the database
max_retries = 4
conn = None
for attempt in range(1, max_retries + 1):
    try:
        print(f"Attempt {attempt} to connect to the database...")
        conn = pyodbc.connect(conn_stringCWS)
        print("Database connection successful.")
        break  # Exit loop if connection is successful
    except pyodbc.Error as e:
        print(f"Database connection failed on attempt {attempt}: {e}")
        if attempt == max_retries:
            print("Maximum retry attempts reached. Exiting.")
            exit(1)
        print("Retrying in 5 seconds...")
        sleep(5)

# Query and process data
try:
    # Query the SYSTEM.view_client_episode_history
    query = "SELECT * FROM SYSTEM.view_client_episode_history"

    # Execute the query and load the data into a DataFrame
    client_episode_history_df = pd.read_sql(query, conn)
    print("client_episode_history_df loaded successfully:")

    # Ensure the EPN_uniqueid column is in the correct format for sorting
    client_episode_history_df['EPN_uniqueid'] = client_episode_history_df['EPN_uniqueid'].astype(str)

    # Extract the numeric part for sorting and identify the most recent records
    client_episode_history_df['EPN_uniqueid_numeric'] = client_episode_history_df['EPN_uniqueid'].apply(lambda x: int(x.split('.')[1]))
    most_recent_episode_df = client_episode_history_df.loc[
        client_episode_history_df.groupby(['PATID', 'EPISODE_NUMBER'])['EPN_uniqueid_numeric'].idxmax()
    ]

    # Drop the temporary numeric column
    most_recent_episode_df = most_recent_episode_df.drop(columns=['EPN_uniqueid_numeric'])

    # Merge the program_value column from the most recent episode records into the calendar_df
    calendar_df = calendar_df.merge(
        most_recent_episode_df[['PATID', 'EPISODE_NUMBER', 'program_value']], 
        on=['PATID', 'EPISODE_NUMBER'], 
        how='left'
    )

    print("Updated calendar_df with program_value column:")

    # Save the updated calendar_df to the temp_data.pkl file
    data['calendar_df'] = calendar_df
    with open(data_file, 'wb') as f:
        pickle.dump(data, f)

    print(f"Filtered calendar_df saved to {data_file}")

finally:
    if conn:
        conn.close()
        print("Database connection closed.")
