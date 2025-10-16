 

import os
import pickle
import pandas as pd
from dotenv import load_dotenv

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


 

# Keep only the required columns
calendar_df = calendar_df[['PATID', 'date', 'admin_hrs_default', 'admin_instruct_formatted', 'med_descr_ext_formatted', 'order_code_description', 'program_value']]

# Strip out the time part from the date column
calendar_df['date'] = pd.to_datetime(calendar_df['date']).dt.strftime('%Y_%m_%d')


 
calendar_df.to_pickle("temp_data.pkl")


 
# # Save the calendar_df to a CSV file
# csv_file_path = os.path.join(os.getcwd(), 'calendar_df.csv')
# calendar_df.to_csv(csv_file_path, index=False)
# print(f"calendar_df saved to {csv_file_path}")


