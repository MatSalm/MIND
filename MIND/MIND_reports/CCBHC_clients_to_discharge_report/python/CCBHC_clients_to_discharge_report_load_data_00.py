import os
import sys
import pyodbc
import pandas as pd
from datetime import datetime
import pickle
import configparser
import json
import time
import glob

# Ensure proper usage by checking the number of command-line arguments
if len(sys.argv) != 3:
    print("Usage: python 00.py <data_file> <param_file>")
    sys.exit(1)

# Extract the data and parameter file paths from the command-line arguments
data_file = sys.argv[1]
param_file = sys.argv[2]

# Helper function for connecting with retries
def get_db_connection(conn_string, max_retries=4, timeout=60):
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"Attempt {attempt + 1} of {max_retries} to connect to the database...")
            conn = pyodbc.connect(conn_string, timeout=timeout)
            print("Database connection successful.")
            return conn
        except pyodbc.OperationalError as e:
            print(f"Database connection failed on attempt {attempt + 1}: {e}")
            attempt += 1
            time.sleep(5)
    raise Exception(f"Could not connect to the database after {max_retries} attempts.")

try:
    # ------------------------------------------------
    # 1) Load existing data (if any) + read parameters
    # ------------------------------------------------
    if os.path.exists(data_file):
        with open(data_file, 'rb') as f:
            data = pickle.load(f)
    else:
        data = None

    with open(param_file, 'r', encoding='utf-8') as f:
        parameters = json.load(f)

    if not isinstance(parameters, dict):
        raise ValueError("Parameters should be a dictionary.")

    # Read config.ini
    config_file_path = os.path.join('..', 'config', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file_path)

    days_since_last_service = int(config.get('report', 'days_since_last_service'))
    use_program_list = config.get('report', 'use_ccbhc_program_list')

    # Load valid CCBHC programs from CSV
    program_list_path = os.path.join('..', 'config', 'program_list.csv')
    df_program_list = pd.read_csv(program_list_path)
    valid_programs = set(df_program_list['program_value'].dropna().unique())

    # Get today's date
    today = datetime.today()

    # --------------------------------------------
    # 2) Connect to the CWS database
    # --------------------------------------------
    cws_conn_string = (
        f"DRIVER={{{os.getenv('database_driver_name')}}};"
        f"SERVER={os.getenv('database_server')};"
        f"PORT={os.getenv('database_port')};"
        f"DATABASE={os.getenv('databaseCWS')};"
        f"UID={os.getenv('database_username')};"
        f"PWD={os.getenv('database_password')}"
    )
    cws_conn = get_db_connection(cws_conn_string)

    # --------------------------------------------
    # 3) Retrieve service notes (cw_patient_notes, Miscellaneous_Note_V2)
    # --------------------------------------------
    query_pn = """
    SELECT
        PATID,
        EPISODE_NUMBER,
        date_of_service,
        service_charge_code
    FROM AVCWS.SYSTEM.cw_patient_notes
    """
    df_pn = pd.read_sql(query_pn, cws_conn)

    query_mn = """
    SELECT
        PATID,
        EPISODE_NUMBER,
        Assess_Date AS date_of_service,
        Reason_Value AS service_charge_code
    FROM AVCWS.SYSTEM.Miscellaneous_Note_V2
    """
    df_mn = pd.read_sql(query_mn, cws_conn)

    df_combined_notes = pd.concat([df_pn, df_mn], ignore_index=True)
    df_combined_notes.dropna(subset=['PATID', 'EPISODE_NUMBER', 'date_of_service'], inplace=True)
    df_combined_notes['PATID'] = df_combined_notes['PATID'].astype(str)
    df_combined_notes['EPISODE_NUMBER'] = df_combined_notes['EPISODE_NUMBER'].astype(str)
    df_combined_notes['date_of_service'] = pd.to_datetime(df_combined_notes['date_of_service'])

    # --------------------------------------------
    # 4) Retrieve active clients from view_client_episode_history
    # --------------------------------------------
    query_ceh = """
    SELECT
        PATID,
        EPISODE_NUMBER,
        program_value
    FROM SYSTEM.view_client_episode_history
    WHERE date_of_discharge IS NULL
    """
    df_ceh = pd.read_sql(query_ceh, cws_conn)
    df_ceh['PATID'] = df_ceh['PATID'].astype(str)
    df_ceh['EPISODE_NUMBER'] = df_ceh['EPISODE_NUMBER'].astype(str)

    
    # Merge to get the last date_of_service per PATID + EPISODE_NUMBER
    df_merged = pd.merge(
        df_combined_notes,
        df_ceh,
        on=['PATID', 'EPISODE_NUMBER'],
        how='inner'
    )

    df_last_dates = df_merged.groupby(['PATID', 'EPISODE_NUMBER', 'program_value'], as_index=False)['date_of_service'].max()
    df_last_dates.rename(columns={'date_of_service': 'last_date_of_service'}, inplace=True)

    # Merge back for the final df_all_clients
    df_all_clients = pd.merge(
        df_last_dates,
        df_combined_notes,
        left_on=['PATID', 'EPISODE_NUMBER', 'last_date_of_service'],
        right_on=['PATID', 'EPISODE_NUMBER', 'date_of_service'],
        how='left'
    )
    df_all_clients.drop(columns=['date_of_service'], inplace=True)

    # Rename columns for consistency
    df_all_clients.rename(columns={
        'PATID': 'Client ID',
        'EPISODE_NUMBER': 'Episode Number',
        'program_value': 'Program',
        'last_date_of_service': 'Last Date of Service',
        'service_charge_code': 'Most Recent Service Charge Code'
    }, inplace=True)

    # Compute days since last service + recommended discharge logic
    df_all_clients['Days Since Most Recent Service'] = (today - df_all_clients['Last Date of Service']).dt.days
    df_all_clients['Is Client Recommended for Discharge'] = df_all_clients['Days Since Most Recent Service'].apply(
        lambda x: 'Yes' if x >= 90 else 'No'
    )
    # --------------------------------------------
    # 5b) Check NOMS Discharges for ALL clients
    # --------------------------------------------

    # (a) Fetch NOMS records, alias PATID → PATID_2
    query_noms_discharge = """
    SELECT
        PATID              AS PATID_2,
        Assessment_Type_Value,
        Discharge_Date,
        Discharge_Status_Value
    FROM AVCWS.SYSTEM.NOMS
    WHERE Option_Desc = 'NOMs'
    """
    df_noms = pd.read_sql(query_noms_discharge, cws_conn)

    # —— make sure PATID_2 is the same type as df_all_clients['Client ID']:
    df_noms['PATID_2'] = df_noms['PATID_2'].astype(str)

    # (b) Clean & filter
    df_noms['Discharge_Date'] = pd.to_datetime(df_noms['Discharge_Date'], errors='coerce')
    df_noms = df_noms[df_noms['Discharge_Date'].notna()]

    # (c) Aggregate into one string per PATID_2
    def format_noms_discharge(group):
        entries = []
        for _, row in group.iterrows():
            parts = []
            if pd.notna(row['Assessment_Type_Value']):
                parts.append(str(row['Assessment_Type_Value']))
            parts.append(str(row['Discharge_Date'].date()))
            if pd.notna(row['Discharge_Status_Value']):
                parts.append(f"({row['Discharge_Status_Value']})")
            entries.append(': '.join(parts[:2]) + (f" {parts[2]}" if len(parts) > 2 else ""))
        return ', '.join(entries)

    noms_series = df_noms.groupby('PATID_2').apply(format_noms_discharge)

    # (d) Build a DataFrame from that Series
    noms_df = pd.DataFrame({
        'PATID_2': list(noms_series.index),
        'Does Client Have A NOMS Discharge?': list(noms_series.values)
    })

    # —— again, ensure the helper key is string:
    noms_df['PATID_2'] = noms_df['PATID_2'].astype(str)

    # (e) Merge into your master (df_all_clients has 'Client ID' as str) and drop helper
    df_all_clients = (
        df_all_clients
        .merge(noms_df,
                left_on='Client ID',
                right_on='PATID_2',
                how='left')
        .drop(columns=['PATID_2'])
    )

    # (f) Fill any missing values
    df_all_clients['Does Client Have A NOMS Discharge?'].fillna('', inplace=True)

    new_col = 'Does Client Have A NOMS Discharge?'
    if new_col in df_all_clients.columns:
        cols = [c for c in df_all_clients.columns if c != new_col] + [new_col]
        df_all_clients = df_all_clients[cols]
        
    # --------------------------------------------
    # 5) Check NOMS sampling for ALL clients
    # --------------------------------------------
    noms_directory = r'C:\reports\NOMS_random_sample_report\logs'
    noms_files = glob.glob(os.path.join(noms_directory, '*noms_history.txt'))
    noms_client_ids = set()

    for file in noms_files:
        with open(file, 'r') as f:
            noms_client_ids.update(line.strip() for line in f if line.strip().isdigit())

    # Mark each record that belongs to a client who has a NOMS
    df_all_clients['Has Client Been Included In NOMs Sampling'] = df_all_clients['Client ID'].apply(
        lambda x: 'Yes' if x in noms_client_ids else 'No'
    )
    # If ANY record is Yes for that Client ID, make them ALL Yes
    df_all_clients['Has Client Been Included In NOMs Sampling'] = (
        df_all_clients
        .groupby('Client ID')['Has Client Been Included In NOMs Sampling']
        .transform(lambda grp: 'Yes' if 'Yes' in grp.values else 'No')
    )


    # --------------------------------------------
    # 6) Connect to the PM database for appt_data
    # --------------------------------------------
    pm_conn_string = (
        f"DRIVER={{{os.getenv('database_driver_name')}}};"
        f"SERVER={os.getenv('database_server')};"
        f"PORT={os.getenv('database_port')};"
        f"DATABASE={os.getenv('databasePM')};"
        f"UID={os.getenv('database_username')};"
        f"PWD={os.getenv('database_password')}"
    )
    pm_conn = get_db_connection(pm_conn_string)

    query_appt = """
    SELECT
        PATID,
        EPISODE_NUMBER,
        CAST(appointment_date AS date) AS appointment_date
    FROM AVPM.SYSTEM.appt_data
    WHERE appointment_date > GETDATE()  -- future appointments only
    """
    df_appt = pd.read_sql(query_appt, pm_conn)
    pm_conn.close()

    df_appt['PATID'] = df_appt['PATID'].astype(str)
    df_appt['EPISODE_NUMBER'] = df_appt['EPISODE_NUMBER'].astype(str)
    df_appt['appointment_date'] = pd.to_datetime(df_appt['appointment_date']).dt.date

    # Earliest future appointment for each client/episode
    df_future_appt = df_appt.groupby(['PATID', 'EPISODE_NUMBER'], as_index=False)['appointment_date'].min()
    df_future_appt.rename(columns={'appointment_date': "Client's Next Scheduled Appointment"}, inplace=True)

    # Merge future appointment info
    df_all_clients = pd.merge(
        df_all_clients,
        df_future_appt,
        left_on=['Client ID', 'Episode Number'],
        right_on=['PATID', 'EPISODE_NUMBER'],
        how='left'
    )
    df_all_clients.drop(columns=['PATID', 'EPISODE_NUMBER'], inplace=True)

    # --------------------------------------------
    # 7) De-duplicate all exact duplicates
    # --------------------------------------------
    df_all_clients.drop_duplicates(inplace=True)

    # --------------------------------------------
    # 8) Multi-Program Logic for CCBHC Clients Only
    # --------------------------------------------
    # If client is in multiple CCBHC programs, and ANY one of those programs is NOT recommended for discharge,
    # then none of those CCBHC program records should be recommended for discharge.
    ccbhc_mask = df_all_clients['Program'].isin(valid_programs)

    df_all_clients.loc[ccbhc_mask, 'Is Client Recommended for Discharge'] = (
        df_all_clients[ccbhc_mask]
        .groupby('Client ID')['Is Client Recommended for Discharge']
        .transform(lambda group: 'No' if 'No' in group.values else 'Yes')
    )

    # --------------------------------------------
    # 9) Create df_clients_to_discharge (CCBHC Only)
    # --------------------------------------------
    df_clients_to_discharge = df_all_clients[
        (df_all_clients['Program'].isin(valid_programs)) &
        (df_all_clients['Days Since Most Recent Service'] >= days_since_last_service) &
        (df_all_clients['Is Client Recommended for Discharge'] == 'Yes')
    ]

    # Update parameters with today's date
    parameters['report_date'] = today.strftime('%Y-%m-%d')
    with open(param_file, 'w', encoding='utf-8') as f:
        json.dump(parameters, f, indent=4)
    print(f"Report date {parameters['report_date']} saved in {param_file}")

    # --------------------------------------------
    # 10) Save final data
    # --------------------------------------------
    with open(data_file, 'wb') as f:
        pickle.dump(
            {
                # "all_clients": includes all records (CCBHC + non-CCBHC) with the standard discharge logic
                'all_clients': df_all_clients,
                # "clients_to_discharge": ONLY the CCBHC subset that meets discharge criteria
                'clients_to_discharge': df_clients_to_discharge
            },
            f
        )

    print(
        f"Data processing complete. {len(df_all_clients)} total records, "
        f"{len(df_clients_to_discharge)} CCBHC records recommended for discharge."
    )

except Exception as e:
    print(f"Error: {str(e)}")
    raise
