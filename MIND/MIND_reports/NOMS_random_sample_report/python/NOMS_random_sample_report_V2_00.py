import os
import pyodbc
import pandas as pd
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from configparser import ConfigParser
import glob

# Load environment variables from MIND.env
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Configure logging
log_directory = os.path.join('C:\\MIND\\MIND_reports\\NOMS_random_sample_report', 'logs')
os.makedirs(log_directory, exist_ok=True)
log_file_path = os.path.join(log_directory, f'noms_report_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.log')
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Starting NOMs Random Sample Report Script")

# Load configuration
config_file_path = os.path.join('C:\\MIND\\MIND_reports\\NOMS_random_sample_report', 'config', 'config.ini')
config = ConfigParser()
config.read(config_file_path)

# Extract configuration values
days = int(config.get("sqlquery", "days"))  # Maximum days for lookup (not used in query)
nomsdate = int(config.get("nomsdate", "days_lookback"))  # Only clients admitted in this period
program_list = config.get("nomsdate", "program_list")

# Database credentials from environment variables
database_driver = os.getenv('database_driver_name')
database_server = os.getenv('database_server')
database_port = os.getenv('database_port')
database_username = os.getenv('database_username')
database_password = os.getenv('database_password')

databasePM = os.getenv('databasePM')
databaseCWS = os.getenv('databaseCWS')

# Function to establish database connections with retry logic
def connect_with_retries(driver, server, port, database, username, password, retries=4, timeout=60):
    for attempt in range(retries):
        try:
            logging.info(f"Attempt {attempt + 1} to connect to {database}.")
            conn = pyodbc.connect(
                f"DRIVER={driver};SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password}",
                timeout=timeout
            )
            logging.info(f"Successfully connected to {database}.")
            return conn
        except pyodbc.OperationalError as e:
            logging.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(5)  # Wait before retrying
            else:
                logging.error(f"All {retries} connection attempts failed.")
                raise

# Establish connections with retries
connPM = connect_with_retries(database_driver, database_server, database_port, databasePM, database_username, database_password)
connCWS = connect_with_retries(database_driver, database_server, database_port, databaseCWS, database_username, database_password)

# Query admissions for the past `nomsdate` days
admission_query = f"""
    SELECT PATID, EPISODE_NUMBER, admission_date, program_value
    FROM SYSTEM.admission_data 
    WHERE admission_date >= DATEADD(day, -{nomsdate}, GETDATE())
"""
df = pd.read_sql(admission_query, connPM)

# Check if we retrieved any data
if df.empty:
    logging.warning("No admissions found within the last {nomsdate} days.")
    print(f"No admissions found within the last {nomsdate} days. Exiting script.")
    exit()

# Query for valid services
services_query = "SELECT DISTINCT PATID, EPISODE_NUMBER FROM SYSTEM.cw_patient_notes"
df_services = pd.read_sql(services_query, connCWS)

# Check if we retrieved any service data
if df_services.empty:
    logging.warning("No valid services found for patients.")
    print("No services found. Exiting script.")
    exit()

# Merge to retain only records with services
df = df.merge(df_services, on=['PATID', 'EPISODE_NUMBER'], how='inner')

# Load program list
program_df = pd.read_csv(program_list)

# Filter only clients from valid programs
df = df[df['program_value'].isin(program_df['program_value'])]
df['admission_date'] = pd.to_datetime(df['admission_date'])

# Check if data remains after program filtering
if df.empty:
    logging.warning("No clients remain after filtering by program list.")
    print("No valid clients remain after filtering. Exiting script.")
    exit()

# Exclude previously sampled PATIDs
previously_sampled_patids = set()
noms_history_files = glob.glob(os.path.join(log_directory, '*_noms_history.txt'))
for file_path in noms_history_files:
    with open(file_path, 'r') as file:
        previously_sampled_patids.update(file.read().splitlines())

df = df[~df['PATID'].isin(previously_sampled_patids)]

# Check if data remains after excluding previously sampled clients
if df.empty:
    logging.warning("No clients remain after excluding previously sampled PATIDs.")
    print("No new clients to sample. Exiting script.")
    exit()

# Sample selection
samples = []
for program in df['program_value'].unique():
    program_df = df[df['program_value'] == program]
    sample_size = max(1, int(0.1 * len(program_df)))  # Take 10% of population, but at least 1
    if not program_df.empty:
        samples.append(program_df.sample(sample_size))

# Validate if any samples were selected
if not samples:
    logging.warning("No clients were selected for sampling.")
    print("No samples were selected. Exiting script.")
    exit()

# Create final DataFrame
samples_df = pd.concat(samples).drop_duplicates(subset='PATID')[['PATID', 'program_value']]

# Save new sampled PATIDs to history log
report_date = datetime.now().strftime('%Y_%m_%d')
new_noms_history_file_path = os.path.join(log_directory, f'{report_date}_noms_history.txt')
with open(new_noms_history_file_path, 'w') as file:
    for patid in samples_df['PATID']:
        file.write(f'{patid}\n')

# Generate Excel file
excel_filename = os.path.join(log_directory, f"NOMS_Sample_Report_{report_date}.xlsx")
samples_df.to_excel(excel_filename, index=False)

# Log completion
logging.info(f"NOMs sample report saved to {excel_filename}")
print(f"NOMs sample report for {report_date} saved successfully.")
