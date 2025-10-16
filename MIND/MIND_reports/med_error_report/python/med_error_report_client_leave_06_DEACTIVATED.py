 
import sys
import os
import pickle
import json
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Function to clean and standardize the time format
def clean_time_format(time_str):
    # Standardize "Noon" and "Midnight" times
    if 'Noon' in time_str:
        return '12:00 PM'
    elif 'Midnight' in time_str:
        return '12:00 AM'
    
    # Check if the time is already in 24-hour format
    try:
        pd.to_datetime(time_str, format='%H:%M')
        return pd.to_datetime(time_str, format='%H:%M').strftime('%I:%M %p')
    except ValueError:
        pass

    # Assume the time is in 12-hour format and return it as is
    return time_str

# Determine if running in a Jupyter notebook
if 'ipykernel' in sys.modules:
    data_file = os.path.join(os.getcwd(), 'temp_data.pkl')
else:
    data_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), 'temp_data.pkl')

# Load the dataframe from the .pkl file
with open(data_file, 'rb') as f:
    calendar_df = pickle.load(f)

# Clean and standardize the 'admin_hrs_default' column
calendar_df['admin_hrs_default'] = calendar_df['admin_hrs_default'].apply(clean_time_format)

# Combine 'date' and 'admin_hrs_default' into a single datetime column
calendar_df['datetime'] = pd.to_datetime(calendar_df['date'].astype(str) + ' ' + calendar_df['admin_hrs_default'])

# Load environment variables from the specific path
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Get database connection parameters from environment variables
server = os.getenv('database_server')
port = os.getenv('database_port')
database = os.getenv('databasePM')  # Assuming you want to use the AVPM database
username = os.getenv('database_username')
password = os.getenv('database_password')
driver = os.getenv('database_driver_name')

# Verify that all necessary environment variables are loaded
required_vars = [server, port, database, username, password, driver]
missing_vars = [var for var in required_vars if not var]
if missing_vars:
    print(f"Error: Missing required environment variables: {missing_vars}")
    exit(1)

# Establish the database connection with error handling
conn = None
try:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"PORT={port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    print("Database connection successful.")

    # Query the AVPM.SYSTEM.leaves_history_outon joined to leaves_history_return_from
    query = """
    SELECT 
        lo.PATID,
        lo.leave_date,
        lo.leave_time,
        lr.return_date,
        lr.return_time
    FROM 
        AVPM.SYSTEM.leaves_history_outon lo
    LEFT JOIN 
        AVPM.SYSTEM.leaves_history_return_from lr
    ON 
        lo.LEA_uniqueid = lr.jointto_leave_history_outon
    """
    
    # Load the data into a DataFrame
    leave_df = pd.read_sql(query, conn)
    print("Data loaded successfully from database.")
    
except pyodbc.Error as e:
    print(f"Database connection failed: {e}")
    print("Check if the ODBC driver is installed and the connection parameters are correct.")
    exit(1)
except Exception as e:
    print(f"Failed to load data from database: {e}")
    if conn:
        conn.close()
    exit(1)
finally:
    if conn:
        conn.close()


# Ensure date and time columns are in datetime format
leave_df['leave_date'] = pd.to_datetime(leave_df['leave_date'])
leave_df['return_date'] = pd.to_datetime(leave_df['return_date'], errors='coerce')

# Convert 12-hour time format to 24-hour time format
leave_df['leave_time'] = pd.to_datetime(leave_df['leave_time'], format='%I:%M %p').dt.strftime('%H:%M:%S')
leave_df['return_time'] = pd.to_datetime(leave_df['return_time'], format='%I:%M %p', errors='coerce').dt.strftime('%H:%M:%S')

# Combine date and time into a single datetime column
leave_df['hold_start_datetime'] = pd.to_datetime(leave_df['leave_date'].astype(str) + ' ' + leave_df['leave_time'])
leave_df['resume_datetime'] = pd.to_datetime(leave_df['return_date'].astype(str) + ' ' + leave_df['return_time'], errors='coerce')

# Handle records with no resume date and time
leave_df['resume_datetime'].fillna(pd.Timestamp.max, inplace=True)


# Check if 'PATID' column exists in calendar_df
if 'PATID' not in calendar_df.columns:
    print("Error: 'PATID' column not found in calendar_df")
    exit(1)

# Initialize a counter for affected records
affected_records_count = 0

# Remove records from calendar_df based on leave periods
for index, row in leave_df.iterrows():
    if pd.isnull(row['resume_datetime']):
        # Remove all records from hold_start_datetime onwards if no resume_datetime
        condition = (
            (calendar_df['PATID'] == row['PATID']) &
            (calendar_df['datetime'] >= row['hold_start_datetime'])
        )
    else:
        # Remove records between hold_start_datetime and resume_datetime
        condition = (
            (calendar_df['PATID'] == row['PATID']) &
            (calendar_df['datetime'] >= row['hold_start_datetime']) &
            (calendar_df['datetime'] <= row['resume_datetime'])
        )
    affected_records_count += calendar_df[condition].shape[0]
    calendar_df = calendar_df[~condition]

# Print the number of affected records
print(f"Number of affected records: {affected_records_count}")





 
# Save the updated calendar_df to a .pkl file
updated_calendar_pkl_path = os.path.join(os.getcwd(), 'temp_data.pkl')
with open(updated_calendar_pkl_path, 'wb') as f:
    pickle.dump(calendar_df, f)

print(f"Updated calendar dataframe saved to {updated_calendar_pkl_path}")


 



