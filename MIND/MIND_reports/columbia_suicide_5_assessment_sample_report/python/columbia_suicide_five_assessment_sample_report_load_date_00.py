import pandas as pd
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
import pickle
import json
import glob

# Load environmental variables from .env file
load_dotenv()

# Database connection details from .env file
database_server = os.getenv('database_server')
database_port = os.getenv('database_port')
databaseCWS = os.getenv('databaseCWS')
database_username = os.getenv('database_username')
database_password = os.getenv('database_password')
database_driver_name = os.getenv('database_driver_name')

# Database connection using the structured connection string
connection_string = (
    f"DRIVER={{{database_driver_name}}};"
    f"SERVER={database_server};"
    f"PORT={database_port};"
    f"DATABASE={databaseCWS};"
    f"UID={database_username};"
    f"PWD={database_password};"
)
try:
    conn = pyodbc.connect(connection_string)
    print("Connected to the database successfully")
except Exception as e:
    print(f"Failed to connect to the database: {e}")
    sys.exit(1)

# Define the current date
current_date = datetime.now()

# Determine the current quarter and set start and end dates for the last quarter
quarter_starts = [(1, 1), (4, 1), (7, 1), (10, 1)]
quarter_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
current_quarter = (current_date.month - 1) // 3
start_date = datetime(current_date.year if current_quarter != 0 else current_date.year - 1, *quarter_starts[current_quarter - 1])
end_date = datetime(current_date.year if current_quarter != 0 else current_date.year - 1, *quarter_ends[current_quarter - 1])

# Load previously sampled PATIDs
history_path = os.path.join('..', 'columbia_sample_history')
if not os.path.exists(history_path):
    os.makedirs(history_path)  # Create the directory if it doesn't exist

history_files = glob.glob(os.path.join(history_path, '*.txt'))
previously_sampled_patids = []
for filename in history_files:
    with open(filename, 'r') as file:
        previously_sampled_patids.extend(file.read().splitlines())

# Construct the SQL query with the corrected field name and without Staff_Step_Taken condition
test_names = ["'TEST'", "'test'", "'Test'", "'testing'", "'TESTING'", "'Testing'"]
exclusion_clause = f"AND s.PATID NOT IN ({', '.join('?' for _ in previously_sampled_patids)})" if previously_sampled_patids else ""
query = f"""
SELECT s.PATID, s.columbia_assessment_date AS Assess_Date, d.patient_home_phone
FROM SYSTEM.Columbia_Assessment s
JOIN SYSTEM.client_curr_demographics d ON s.PATID = d.PATID
WHERE s.columbia_assessment_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
AND (d.patient_name_first NOT IN ({', '.join(test_names)})
AND d.patient_name_last NOT IN ({', '.join(test_names)}))
{exclusion_clause}
"""

# Execute the query and fetch the data into a DataFrame
params = tuple(previously_sampled_patids) if previously_sampled_patids else ()
try:
    df = pd.read_sql(query, conn, params=params)
    print(f"Data fetched successfully. Number of rows fetched: {len(df)}")
except Exception as e:
    print(f"Error executing SQL query: {e}")
    sys.exit(1)

# Close the database connection
conn.close()

# Ensure we have distinct PATID
distinct_df = df[['PATID', 'patient_home_phone']].drop_duplicates()

# Calculate the sample size (5% of the distinct PATID records) or at least one sample
sample_size = max(1, int(0.05 * len(distinct_df)))
print(f"Sample size calculated: {sample_size}")

# Apply weights: higher for rows with a non-null phone number
weights = distinct_df['patient_home_phone'].notna().astype(int) * 1.5 + 1
sample_df = distinct_df.sample(n=sample_size, weights=weights, random_state=42)
print(f"Sampled dataframe created with {len(sample_df)} rows.")

# Save the sample dataframe to a pickle file using 'wb' mode (binary write)
try:
    with open('temp_data.pkl', 'wb') as f:
        pickle.dump(sample_df, f)
    print("Sample dataframe saved to temp_data.pkl")
except Exception as e:
    print(f"Error saving sample data to pickle: {e}")
    sys.exit(1)

# Save sampled PATIDs to a text file with timestamp
history_filename = datetime.now().strftime('columbia_sample_history_%Y%m%d_%H%M%S.txt')
history_full_path = os.path.join(history_path, history_filename)
try:
    with open(history_full_path, 'w') as file:
        for patid in sample_df['PATID']:
            file.write(f"{patid}\n")
    print(f"Sampled PATID history saved to {history_filename}.")
except Exception as e:
    print(f"Error saving PATID history: {e}")
    sys.exit(1)

# Save start and end dates to temp_params.json
params = {
    'start_date': start_date.strftime('%Y-%m-%d'),
    'end_date': end_date.strftime('%Y-%m-%d')
}
try:
    with open('temp_params.json', 'w') as f:
        json.dump(params, f, indent=4)
    print("Parameters saved to temp_params.json")
except Exception as e:
    print(f"Error saving parameters to JSON: {e}")
    sys.exit(1)
