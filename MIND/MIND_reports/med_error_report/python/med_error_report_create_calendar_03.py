import os
import pickle
import json
import sys
import pandas as pd
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from dotenv import load_dotenv
import re
import configparser

# Load environment variables
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Function to get the last rolling year's dates
def get_last_rolling_year_dates():
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    one_year_ago = yesterday - timedelta(days=365)
    return one_year_ago, yesterday

# Load parameters from config.ini
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.getcwd()), 'config', 'config.ini')
config.read(config_path)

calendar_start_date_str = config.get('calendar', 'calendar_start_date', fallback=None)
calendar_stop_date_str = config.get('calendar', 'calendar_stop_date', fallback=None)

if not calendar_start_date_str or not calendar_stop_date_str:
    print("Invalid or missing calendar dates. Assigning default dates for the last rolling year.")
    calendar_start_date, calendar_stop_date = get_last_rolling_year_dates()
else:
    calendar_start_date = pd.to_datetime(calendar_start_date_str)
    calendar_stop_date = pd.to_datetime(calendar_stop_date_str)

calendar_start_date_str = calendar_start_date.strftime('%Y-%m-%d %H:%M:%S')
calendar_stop_date_str = calendar_stop_date.strftime('%Y-%m-%d %H:%M:%S')

print(f"Calendar start date: {calendar_start_date_str}")
print(f"Calendar stop date: {calendar_stop_date_str}")

# Determine if running in a Jupyter notebook
if 'ipykernel' in sys.modules:
    data_file = os.path.join(os.getcwd(), 'temp_data.pkl')
else:
    data_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), 'temp_data.pkl')

# Load the dataframe from the .pkl file
with open(data_file, 'rb') as f:
    df = pickle.load(f)

# Function to check if the time is in HH:MM format
def is_valid_time_format(time_str):
    try:
        datetime.strptime(time_str.strip(), "%H:%M")
        return True
    except ValueError:
        return False



def parse_time(time_str):
    # Normalize and clean up known issues or extra text in time strings
    time_str = time_str.strip()
    if 'Noon' in time_str:
        time_str = '12:00 PM'  # Convert '12:00 Noon PM' or similar to '12:00 PM'

    # Use regex to ensure only valid time and AM/PM parts are considered
    match = re.search(r'(\d{1,2}:\d{2})\s*(AM|PM)?', time_str, re.IGNORECASE)
    if match:
        time_str = ' '.join(part for part in match.groups() if part)  # Reconstruct time string with valid parts

    try:
        # Try parsing the cleaned-up time considering AM/PM notation
        return datetime.strptime(time_str, '%I:%M %p').time()
    except ValueError:
        try:
            # Fallback to 24-hour time format parsing
            return datetime.strptime(time_str, '%H:%M').time()
        except ValueError:
            # Log or handle cases where time cannot be parsed
            print(f"Failed to parse time: {time_str}")
            return None

# Create an empty list for the calendar entries
calendar_list = []

for index, row in df.iterrows():
    order_start_date = pd.to_datetime(row['order_start_date'])
    order_stop_eff_date = pd.to_datetime(row['order_stop_eff_date'])  # Correct column for stop date
    
    order_start_time = parse_time(row['order_start_time'])
    order_stop_eff_time = parse_time(row['order_stop_eff_time'])  # Correct column for stop time
    admin_hrs_default_time = parse_time(row['admin_hrs_default'])
    
    if None in (order_start_time, order_stop_eff_time, admin_hrs_default_time):
        continue  # Skip rows where times could not be parsed

    # Generate a list of dates between the start and stop dates
    dates = [dt.date() for dt in rrule(DAILY, dtstart=calendar_start_date, until=calendar_stop_date)]
    
    for date in dates:
        if row['daily_admin_code'] == 'D':
            # Handle daily schedules
            if date >= order_start_date.date() and date <= order_stop_eff_date.date():
                if (date == order_start_date.date() and admin_hrs_default_time >= order_start_time) or \
                (date == order_stop_eff_date.date() and admin_hrs_default_time <= order_stop_eff_time) or \
                (date > order_start_date.date() and date < order_stop_eff_date.date()):
                    new_row = row.copy()
                    new_row['date'] = date
                    calendar_list.append(new_row)

        elif row['daily_admin_code'] == 'N':
            # Handle schedules based on days of the week or every nth day
            if pd.isnull(row['every_nth_day_factor']) and pd.notnull(row['days_administered_code']):
                # Weekly schedules
                weekday = str((date.weekday() + 1) % 7 + 1)
                if date >= order_start_date.date() and date <= order_stop_eff_date.date() and weekday in row['days_administered_code']:
                    if (date == order_start_date.date() and admin_hrs_default_time >= order_start_time) or \
                    (date == order_stop_eff_date.date() and admin_hrs_default_time <= order_stop_eff_time) or \
                    (date > order_start_date.date() and date < order_stop_eff_date.date()):
                        new_row = row.copy()
                        new_row['date'] = date
                        calendar_list.append(new_row)

            elif pd.notnull(row['every_nth_day_factor']):
                # Schedules every nth day
                nth_day = int(row['every_nth_day_factor'])
                start_index = (order_start_date - calendar_start_date).days % nth_day
                if date >= order_start_date.date() and date <= order_stop_eff_date.date() and (date - order_start_date.date()).days % nth_day == 0:
                    if (date == order_start_date.date() and admin_hrs_default_time >= order_start_time) or \
                    (date == order_stop_eff_date.date() and admin_hrs_default_time <= order_stop_eff_time) or \
                    (date > order_start_date.date() and date < order_stop_eff_date.date()):
                        new_row = row.copy()
                        new_row['date'] = date
                        calendar_list.append(new_row)

# Convert the list into a DataFrame
calendar_df = pd.DataFrame(calendar_list)

# Check if 'PATID' and 'date' columns are present, then re-order the columns
if 'PATID' in calendar_df.columns and 'date' in calendar_df.columns:
    cols = ['PATID', 'date'] + [col for col in calendar_df.columns if col not in ['PATID', 'date']]
    calendar_df = calendar_df[cols]

# Sort the DataFrame by 'PATID' and 'date' in ascending order
calendar_df = calendar_df.sort_values(by=['PATID', 'date'])

# Save the calendar dataframe to a .pkl file
current_dir = os.getcwd()
calendar_pkl_path = os.path.join(current_dir, 'temp_data.pkl')
with open(calendar_pkl_path, 'wb') as f:
    pickle.dump(calendar_df, f)

print(f"Calendar dataframe saved to {calendar_pkl_path}")

# Save the parameters to temp_params.json file
params = {
    "calendar_start_date": calendar_start_date.strftime('%Y-%m-%d'),
    "calendar_stop_date": calendar_stop_date.strftime('%Y-%m-%d')
}

json_file = os.path.join(current_dir, 'temp_params.json')
with open(json_file, 'w') as f:
    json.dump(params, f)
