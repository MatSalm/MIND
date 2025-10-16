import os
import pickle
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import pyodbc
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

# Check the columns of calendar_df to verify 'date' exists
print("Columns in calendar_df:", calendar_df.columns)

# If 'date' column is missing, identify the issue
if 'date' not in calendar_df.columns:
    print("Error: 'date' column is missing from calendar_df")
else:
    # Ensure the 'date' column is in datetime format
    calendar_df['date'] = pd.to_datetime(calendar_df['date'])

# Build the connection string
conn_str = (
    f"DRIVER={{{os.getenv('database_driver_name')}}};"
    f"SERVER={os.getenv('database_server')};"
    f"PORT={os.getenv('database_port')};"
    f"DATABASE={os.getenv('databaseCWS')};"
    f"UID={os.getenv('database_username')};"
    f"PWD={os.getenv('database_password')};"
    f"Timeout=60"  # Set a timeout of 60 seconds
)

# Function to connect to the database with retry logic
def connect_with_retry(retries=4, delay=10):
    attempt = 0
    while attempt < retries:
        try:
            conn = pyodbc.connect(conn_str)
            print("Database connection established.")
            return conn
        except pyodbc.Error as e:
            print(f"Database connection attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("All retry attempts failed. Exiting.")
                raise

# Establish the database connection with retries
conn = connect_with_retry()

# Query to select data from eMAR_rescheduled_hours
query = "SELECT * FROM eMAR.eMAR_rescheduled_hours"
emar_df = pd.read_sql(query, conn)

# Ensure date columns are in datetime format
calendar_df['date'] = pd.to_datetime(calendar_df['date'])
emar_df['original_date'] = pd.to_datetime(emar_df['original_date'])
emar_df['rescheduled_date'] = pd.to_datetime(emar_df['rescheduled_date'])

# Convert times to a consistent string format (HH:MM) if necessary
calendar_df['admin_hrs_default'] = calendar_df['admin_hrs_default'].astype(str)
emar_df['original_time'] = emar_df['original_time'].apply(lambda x: x.strftime('%H:%M'))
emar_df['rescheduled_time'] = emar_df['rescheduled_time'].apply(lambda x: x.strftime('%H:%M'))

# Ensure no duplicate index before merging
calendar_df = calendar_df.reset_index(drop=True)
emar_df = emar_df.reset_index(drop=True)

# Merge the dataframes on the common columns including original date and time
merged_df = pd.merge(calendar_df, emar_df, 
                     left_on=['PATID', 'order_unique_id', 'date', 'admin_hrs_default'], 
                     right_on=['PATID', 'order_unique_id', 'original_date', 'original_time'], 
                     how='left')

# Calculate the number of affected records
affected_dates = (calendar_df['date'] != merged_df['rescheduled_date']) & merged_df['rescheduled_date'].notna()
affected_times = (calendar_df['admin_hrs_default'] != merged_df['rescheduled_time']) & merged_df['rescheduled_time'].notna()
affected_records = affected_dates | affected_times
num_affected_records = affected_records.sum()

# Print the number of affected records
print(f"Number of affected records: {num_affected_records}")

# Update only the affected rows in calendar_df
calendar_df.loc[affected_dates, 'date'] = merged_df.loc[affected_dates, 'rescheduled_date']
calendar_df.loc[affected_times, 'admin_hrs_default'] = merged_df.loc[affected_times, 'rescheduled_time']

# Save the calendar dataframe to a .pkl file
current_dir = os.getcwd()
calendar_pkl_path = os.path.join(current_dir, 'temp_data.pkl')
with open(calendar_pkl_path, 'wb') as f:
    pickle.dump(calendar_df, f)

print(f"Calendar dataframe saved to {calendar_pkl_path}")
