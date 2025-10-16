import os
import pickle
import json
from dotenv import load_dotenv
import pyodbc
import pandas as pd
from time import sleep

# Load environment variables
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

def load_data(data_file, param_file):
    try:
        # Load data from the data file
        if os.path.exists(data_file):
            with open(data_file, 'rb') as f:
                df = pickle.load(f)
            print(f"Data loaded from {data_file}")
        else:
            raise FileNotFoundError(f"Data file {data_file} not found")

        # Read parameters from the param file
        if os.path.exists(param_file):
            with open(param_file, 'r') as f:
                parameters = json.load(f)
            print(f"Parameters loaded from {param_file}")
        else:
            raise FileNotFoundError(f"Parameter file {param_file} not found")

        if not isinstance(parameters, dict):
            raise ValueError("Parameters should be a dictionary.")

        print("Parameters:")
        for key, value in parameters.items():
            print(f"{key}: {value}")

        return df, parameters

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

# Direct execution
current_dir = os.getcwd()
data_file = os.path.join(current_dir, 'temp_data.pkl')
param_file = os.path.join(current_dir, 'temp_params.json')

df, parameters = load_data(data_file, param_file)

def load_rescheduled_hours(conn_string, retries=4, delay=5):
    attempts = 0
    while attempts < retries:
        try:
            # Define the SQL query to fetch rescheduled hours
            sql_query = """
            SELECT *
            FROM eMAR.eMAR_hrs_of_admin_hist
            """

            # Connect to the database
            conn = pyodbc.connect(conn_string)

            # Execute the query and load the data into a DataFrame
            rescheduled_hours_df = pd.read_sql_query(sql_query, conn)

            # Close the connection
            conn.close()

            print("Rescheduled hours data loaded successfully")
            return rescheduled_hours_df

        except pyodbc.Error as e:
            attempts += 1
            print(f"Connection attempt {attempts} failed: {str(e)}")
            if attempts < retries:
                print(f"Retrying in {delay} seconds...")
                sleep(delay)
            else:
                raise

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
conn_string = f'DRIVER={{{driver_name}}};SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password};Timeout=60'

# Load rescheduled hours data
rescheduled_hours_df = load_rescheduled_hours(conn_string)

# Display the loaded rescheduled hours DataFrame
print(f"Rescheduled hours DataFrame shape: {rescheduled_hours_df.shape}")

# Function to extract the relevant part of the ID
def extract_prescription_id(id_column):
    return id_column.str.extract(r'(^.*ORD\d+\.\d{3})')[0]

# Extract the relevant part of the ID
rescheduled_hours_df['prescription_id'] = extract_prescription_id(rescheduled_hours_df['ID'])

# Sort the DataFrame to ensure the most recent records are last
rescheduled_hours_df = rescheduled_hours_df.sort_values(by=['admin_hrs_edit_eff_date', 'admin_hrs_edit_eff_time', 'prescription_id'])

# Drop duplicates, keeping the last occurrence (most recent edit)
rescheduled_hours_df = rescheduled_hours_df.drop_duplicates(subset=['admin_hrs_edit_eff_date', 'admin_hrs_edit_eff_time', 'prescription_id'], keep='last')

# Reset index for cleanliness
rescheduled_hours_df = rescheduled_hours_df.reset_index(drop=True)

# Display the cleaned DataFrame
print(f"Cleaned Rescheduled Hours DataFrame shape: {rescheduled_hours_df.shape}")

# Create a mask to identify matching records
match_mask = rescheduled_hours_df.apply(
    lambda row: ((df['PATID'] == row['PATID']) & (df['order_unique_id'] == row['order_unique_id'])).any(), axis=1)

# Count the number of matching records
num_matches = match_mask.sum()

print(f"Number of matching records in rescheduled_hours_df: {num_matches}")

# Ensure `admin_hrs_edit_eff_date` is a datetime object for proper sorting
rescheduled_hours_df['admin_hrs_edit_eff_date'] = pd.to_datetime(rescheduled_hours_df['admin_hrs_edit_eff_date'], format='%Y-%m-%d')
rescheduled_hours_df['admin_hrs_edit_eff_time'] = pd.to_datetime(rescheduled_hours_df['admin_hrs_edit_eff_time'], format='%H:%M:%S').dt.time

# Combine date and time into a single datetime column for sorting
rescheduled_hours_df['admin_hrs_edit_datetime'] = rescheduled_hours_df.apply(
    lambda row: pd.Timestamp.combine(row['admin_hrs_edit_eff_date'], row['admin_hrs_edit_eff_time']), axis=1)

# Sort `rescheduled_hours_df` by `admin_hrs_edit_datetime` in ascending order
rescheduled_hours_df = rescheduled_hours_df.sort_values(by='admin_hrs_edit_datetime')

# Match prescriptions and update the df dataframe iteratively
def update_prescriptions_iteratively(df, rescheduled_hours_df):
    df = df.copy()  # Avoid modifying the original dataframe
    for _, resched_row in rescheduled_hours_df.iterrows():
        # Find the most recent matching row in df for the current PATID and order_unique_id
        match_mask = (df['PATID'] == resched_row['PATID']) & (df['order_unique_id'] == resched_row['order_unique_id'])
        if match_mask.any():
            latest_record_idx = df[match_mask].index[-1]

            # Create a duplicate of the most recent record before updating it
            duplicated_record = df.loc[latest_record_idx].copy()
            duplicated_record['order_start_date'] = resched_row['admin_hrs_edit_eff_date'].strftime('%Y-%m-%d')
            duplicated_record['order_start_time'] = resched_row['admin_hrs_edit_eff_time'].strftime('%H:%M:%S')
            duplicated_record['admin_hrs_default'] = resched_row['admin_hrs_edit']

            # Append the duplicated record to df
            df = pd.concat([df, duplicated_record.to_frame().T], ignore_index=True)

            # Update the order_stop_eff_date and order_stop_eff_time of the original record
            df.loc[latest_record_idx, 'order_stop_eff_date'] = resched_row['admin_hrs_edit_eff_date'].strftime('%Y-%m-%d')
            df.loc[latest_record_idx, 'order_stop_eff_time'] = resched_row['admin_hrs_edit_eff_time'].strftime('%H:%M:%S')

    return df

# Update the df dataframe with rescheduled hours iteratively
df_updated = update_prescriptions_iteratively(df, rescheduled_hours_df)

# Display the updated DataFrame
print("Updated DataFrame head:")
print(df_updated.head(10))
print(f"Updated DataFrame shape: {df_updated.shape}")

# Verify the number of added records
print(f"Number of added records: {len(df_updated) - len(df)}")

# Path to the .pkl file in the current working directory
pkl_file_path = os.path.join(os.getcwd(), 'temp_data.pkl')

# Remove the existing .pkl file if it exists
if os.path.exists(pkl_file_path):
    os.remove(pkl_file_path)

# Save the updated dataframe to the .pkl file
with open(pkl_file_path, 'wb') as f:
    pickle.dump(df_updated, f)

print(f"Dataframe saved to {pkl_file_path}")
