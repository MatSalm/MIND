 
import os
import sys
import pickle
import pandas as pd

# Determine the path to the data file
if 'ipykernel' in sys.modules:
    data_file = os.path.join(os.getcwd(), 'temp_data.pkl')
else:
    data_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), 'temp_data.pkl')

# Load existing data from the .pkl file
if os.path.exists(data_file):
    with open(data_file, 'rb') as f:
        data = pickle.load(f)
        
    # Ensure the data contains the required dataframes
    if 'administration_df' in data and 'calendar_df' in data:
        administration_df = data['administration_df']
        calendar_df = data['calendar_df']
        print("DataFrames loaded successfully.")

    else:
        raise KeyError("The data file does not contain 'administration_df' and 'calendar_df'.")
else:
    raise FileNotFoundError(f"The data file {data_file} does not exist.")

 
# Ensure datetime columns are in datetime format for accurate comparison
calendar_df['datetime'] = pd.to_datetime(calendar_df['datetime'])
administration_df['scheduled_admin_timestamp'] = pd.to_datetime(administration_df['scheduled_admin_timestamp'])

# Create a unique identifier in administration_df for matching
administration_df['unique_id'] = administration_df.apply(
    lambda row: f"{row['PATID']}_{row['order_number']}_{row['order_unique_id']}_{row['scheduled_admin_timestamp']}",
    axis=1
)

# Create a unique identifier in calendar_df for matching
calendar_df['unique_id'] = calendar_df.apply(
    lambda row: f"{row['PATID']}_{row['order_number']}_{row['order_unique_id']}_{row['datetime']}",
    axis=1
)

# Remove records from calendar_df that have a matching record in administration_df
calendar_df = calendar_df[~calendar_df['unique_id'].isin(administration_df['unique_id'])]

# Drop the unique_id column as it's no longer needed
calendar_df = calendar_df.drop(columns=['unique_id'])

 
# Ensure the time columns are in the correct format
def ensure_time_format(time_str):
    # Remove any extra seconds part if present
    return time_str.split(':')[0] + ':' + time_str.split(':')[1]

# Apply the formatting function to the time columns
calendar_df['order_start_time'] = calendar_df['order_start_time'].apply(ensure_time_format)
calendar_df['order_stop_eff_time'] = calendar_df['order_stop_eff_time'].apply(ensure_time_format)

# Convert the 'order_start_date' and 'order_start_time' columns to a datetime object
calendar_df['order_start_datetime'] = pd.to_datetime(calendar_df['order_start_date'].astype(str) + ' ' + calendar_df['order_start_time'].astype(str), format='%Y-%m-%d %H:%M')

# Convert the 'order_stop_eff_date' and 'order_stop_eff_time' columns to a datetime object
calendar_df['order_stop_datetime'] = pd.to_datetime(calendar_df['order_stop_eff_date'].astype(str) + ' ' + calendar_df['order_stop_eff_time'].astype(str), format='%Y-%m-%d %H:%M')

# Drop records where 'order_start_datetime' is greater than 'datetime'
calendar_df = calendar_df[calendar_df['order_start_datetime'] <= calendar_df['datetime']]

# Drop records where 'datetime' is greater than 'order_stop_datetime'
calendar_df = calendar_df[calendar_df['datetime'] <= calendar_df['order_stop_datetime']]

# Drop the temporary datetime columns as they are no longer needed
calendar_df = calendar_df.drop(columns=['order_start_datetime', 'order_stop_datetime'])


# Save the updated calendar_df to the temp_data.pkl file
data['calendar_df'] = calendar_df
with open(data_file, 'wb') as f:
    pickle.dump(data, f)

print(f"Filtered calendar_df saved to {data_file}")


