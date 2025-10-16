import pyodbc
import os
import re
import pandas as pd
from datetime import date, timedelta, datetime
import hashlib
import paramiko
from dotenv import load_dotenv
import shutil
import time

# Load environment variables from the .env file
load_dotenv(dotenv_path='C:/MIND/MIND/MIND_config/MIND.env')

# Validate the config files for security check
def is_valid_file_path(file_path):
    if not os.path.isabs(file_path):
        return False
    url_regex = r"^https?://[^\s/\\]+\.[^\s/\\]+$"
    if re.match(url_regex, file_path):
        return False
    return os.path.exists(file_path)

# Get database credentials from environment variables
server = os.getenv("database_server")
port = os.getenv("database_port")
databaseCWS = os.getenv("databaseCWS")
databasePM = os.getenv("databasePM")
username = os.getenv("database_username")
password = os.getenv("database_password")

# Get email credentials from environment variables
smtp_port = os.getenv("EMAIL_smtp_port")
smtp_server = os.getenv("EMAIL_smtp_server")
smtp_email = os.getenv("EMAIL_smtp_email")

# Get SFTP credentials from environment variables
sftp_hostname = os.getenv("inphonite_sftp_hostname")
sftp_username = os.getenv("inphonite_sftp_username")
sftp_password = os.getenv("inphonite_sftp_password")
sftp_port = int(os.getenv("inphonite_sftp_port"))

conn = None
df = None

# Construct the connection string
conn_stringPM = f'DRIVER={{InterSystems IRIS ODBC35}};SERVER={server};PORT={port};DATABASE={databasePM};UID={username};PWD={password}'

# Retry logic for database connection
def connect_to_database(conn_string, max_retries=4, base_timeout=10):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempting to connect to the database (Attempt {attempt}/{max_retries})...")
            conn = pyodbc.connect(conn_string, timeout=base_timeout * attempt)
            print("Connected to the database.")
            return conn
        except pyodbc.Error as e:
            print(f"Database connection attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(5)  # Wait before retrying
    return None

try:
    # Connect to the database with retry logic
    conn = connect_to_database(conn_stringPM)
    cursor = conn.cursor()

    # Define start and end dates for appointments (tomorrow to 7 days after)
    today = date.today()
    start_date_1 = today + timedelta(days=1)
    end_date_1 = start_date_1 + timedelta(days=7)

    # First query: Appointments
    sql_1 = f"""
    SELECT
        a.PATID,
        a.STAFFID,
        a.SERVICE_CODE,
        a.service_description,
        a.appointment_date,
        a.appointment_end_time,
        a.appointment_start_time,
        a.location_value,
        a.patient_name,
        a.program_value,
        a.site_name,
        a.staff_name,
        p.patient_home_phone,
        p.client_email_addr,
        p.communication_pref_value,
        p.patient_cell_phone,
        p.patient_name_first,
        p.patient_name_last,
        p.preferred_name,
        p.primary_language_value,
        p.ss_demographics_dict_2_value,
        scd.discipline_value
    FROM SYSTEM.appt_data a
    LEFT JOIN SYSTEM.patient_current_demographics p ON a.PATID = p.PATID
    LEFT JOIN SYSTEM.staff_current_demographics scd ON a.STAFFID = scd.STAFFID
    WHERE a.appointment_date >= '{start_date_1.strftime('%Y-%m-%d')}'
        AND a.appointment_date <= '{end_date_1.strftime('%Y-%m-%d')}'
        AND (a.disposition_value NOT IN ('Cancelled', 'Missed Visit') OR a.disposition_value IS NULL)
        AND a.site_name != 'Medication Appointment Reminder Calls'
    """

    cursor.execute(sql_1)
    columns_1 = [column[0] for column in cursor.description]
    data_1 = [dict(zip(columns_1, row)) for row in cursor.fetchall()]
    df = pd.DataFrame(data_1)

    # Second query: Medication Appointment Reminders
    start_date_2 = today
    end_date_2 = today + timedelta(days=13)

    sql_2 = f"""
    SELECT
        a.PATID,
        a.STAFFID,
        a.SERVICE_CODE,
        a.service_description,
        a.appointment_date,
        a.appointment_end_time,
        a.appointment_start_time,
        a.location_value,
        a.patient_name,
        a.program_value,
        a.site_name,
        a.staff_name,
        p.patient_home_phone,
        p.client_email_addr,
        p.communication_pref_value,
        p.patient_cell_phone,
        p.patient_name_first,
        p.patient_name_last,
        p.preferred_name,
        p.primary_language_value,
        p.ss_demographics_dict_2_value,
        scd.discipline_value
    FROM SYSTEM.appt_data a
    LEFT JOIN SYSTEM.patient_current_demographics p ON a.PATID = p.PATID
    LEFT JOIN SYSTEM.staff_current_demographics scd ON a.STAFFID = scd.STAFFID
    WHERE a.appointment_date >= '{start_date_2.strftime('%Y-%m-%d')}'
        AND a.appointment_date <= '{end_date_2.strftime('%Y-%m-%d')}'
        AND a.site_name = 'Medication Appointment Reminder Calls'
    """

    cursor.execute(sql_2)
    columns_2 = [column[0] for column in cursor.description]
    data_2 = [dict(zip(columns_2, row)) for row in cursor.fetchall()]
    JIT_df = pd.DataFrame(data_2)

except Exception as e:
    print("An error occurred querying the database:", e)

# Load the site_list.csv into a list
config_folder = os.path.join(os.path.dirname(os.getcwd()), 'config')
site_list_path = os.path.join(config_folder, 'site_list.csv')

if os.path.exists(site_list_path):
    site_list = pd.read_csv(site_list_path, header=None)[0].tolist()
else:
    print(f"Error: {site_list_path} does not exist.")
    site_list = []

if site_list:
    df = df[df['site_name'].isin(site_list)]
else:
    print("No valid site names to filter by.")


# Data transformation functions
def convert_to_military(time_str):
    dt = datetime.strptime(time_str, "%I:%M %p")
    return dt.strftime("%H:%M")

def extract_first_name(name_str, preferred_name):
    if preferred_name:
        return preferred_name.split()[0]
    try:
        return name_str.split(",")[1].split()[0].strip()
    except:
        return None

def transform_ok_to_leave(value):
    return "YES" if value in ["Leave Message", "No Entry"] else "NO"

def handle_email(row):
    return row["CLTEMAIL"] if row["CONTACT_PREFERENCE"] == "Email" else None

def perform_operations(df):
    df = df.rename(columns={
        "appointment_date": "APPT_DATE",
        "PATID": "CASENO",
        "SERVICE_CODE": "SERVICECD",
        "service_description": "TYPEDESC",
        "appointment_start_time": "BEGTIME",
        "staff_name": "STAFFNAME",
        "patient_home_phone": "CLIENT_PHONE",
        "primary_language_value": "PRIMARY_LANGUAGE",
        "patient_cell_phone": "CLIENT_CELL",
        "communication_pref_value": "CONTACT_PREFERENCE",
        "client_email_addr": "CLTEMAIL",
        "patient_name": "CLIENTNAME",
        "ss_demographics_dict_2_value": "OK_TO_LEAVE_VOICEMAIL",
        "location_value": "place_of_service",  # Rename location_value to place_of_service
    })

    df["APPT_DATE"] = pd.to_datetime(df["APPT_DATE"]).dt.strftime('%m-%d-%Y')
    df["BEGTIME"] = df["BEGTIME"].apply(convert_to_military)
    df["CLIENTNAME"] = df.apply(lambda row: extract_first_name(row["CLIENTNAME"], row["preferred_name"]), axis=1)
    df["OK_TO_LEAVE_VOICEMAIL"] = df["OK_TO_LEAVE_VOICEMAIL"].apply(transform_ok_to_leave)
    df["CLTEMAIL"] = df.apply(handle_email, axis=1)
    return df

# Transform dataframes
if df is not None:
    df = perform_operations(df)
else:
    print("No data found for appointments.")

if JIT_df is not None:
    JIT_df = perform_operations(JIT_df)
else:
    print("No data found for medication appointment reminders.")

def modify_staff_name(row):
    category = row["discipline_value"]
    lastname = row["STAFFNAME"].split(",")[0].strip()
    if category == "MD - Medical Doctor":
        return f"{lastname},Doctor"
    elif category in ("RN - Registered Nurse", "LPN - Licensed Practical Nurse"):
        return f"{lastname},Nurse"
    return row["STAFFNAME"]

# Apply staff name modifications
if df is not None:
    df["STAFFNAME"] = df.apply(modify_staff_name, axis=1)

if JIT_df is not None:
    JIT_df["STAFFNAME"] = JIT_df.apply(modify_staff_name, axis=1)

# Check for records with no contact information and print their CASENO
if df is not None:
    no_contact_info_df = df[
        (df["CLIENT_PHONE"].isnull() | (df["CLIENT_PHONE"] == "")) &
        (df["CLIENT_CELL"].isnull() | (df["CLIENT_CELL"] == "")) &
        (df["CLTEMAIL"].isnull() | (df["CLTEMAIL"] == ""))
    ]

    if not no_contact_info_df.empty:
        print("CASENO with no contact information:")
        print(no_contact_info_df["CASENO"].tolist())
    else:
        print("All records have at least one form of contact information.")

# Columns to be removed
columns_to_remove = [
    "STAFFID", "appointment_end_time", "program_value",
    "patient_name_first", "discipline_value",
    "patient_name_last", "preferred_name"
]

if df is not None:
    df = df.drop(columns=columns_to_remove)

if JIT_df is not None:
    JIT_df = JIT_df.drop(columns=columns_to_remove)

# Convert DataFrame to CSV
if df is not None:
    filename = "APP_" + datetime.now().strftime('%Y_%m_%d') + ".csv"
    df.to_csv(filename, index=False)

if JIT_df is not None:
    filename_JIT = "JIT_" + datetime.now().strftime('%Y_%m_%d') + ".csv"
    JIT_df.to_csv(filename_JIT, index=False)

# Define the archive directory path
archive_dir = os.path.join(os.path.dirname(os.getcwd()), 'archived_exports')
os.makedirs(archive_dir, exist_ok=True)

# Copy the CSV files to the archive directory
if df is not None:
    shutil.copy(filename, os.path.join(archive_dir, filename))

if JIT_df is not None:
    shutil.copy(filename_JIT, os.path.join(archive_dir, filename_JIT))

# Function to calculate the checksum of a file (SHA-256)
def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# SFTP transfer with AES-CTR encryption and checksum verification
try:
    if df is not None:
        checksum_local_filename = calculate_checksum(filename)
    if JIT_df is not None:
        checksum_local_filename_JIT = calculate_checksum(filename_JIT)

    transport = paramiko.Transport((sftp_hostname, sftp_port))
    transport.get_security_options().ciphers = ['aes256-ctr', 'aes192-ctr', 'aes128-ctr']
    transport.connect(username=sftp_username, password=sftp_password)

    sftp = paramiko.SFTPClient.from_transport(transport)

    if df is not None:
        sftp.put(filename, "/inbox/" + filename)
    if JIT_df is not None:
        sftp.put(filename_JIT, "/inbox/" + filename_JIT)

    if df is not None:
        local_temp_file = "downloaded_" + filename
        sftp.get("/inbox/" + filename, local_temp_file)
        checksum_remote_filename = calculate_checksum(local_temp_file)

        if checksum_local_filename == checksum_remote_filename:
            print(f"Checksum verified for {filename}, file transfer successful and verified.")
        else:
            print(f"Checksum mismatch for {filename}, file may have been tampered with during transfer.")
        os.remove(local_temp_file)

    if JIT_df is not None:
        local_temp_file_JIT = "downloaded_" + filename_JIT
        sftp.get("/inbox/" + filename_JIT, local_temp_file_JIT)
        checksum_remote_filename_JIT = calculate_checksum(local_temp_file_JIT)

        if checksum_local_filename_JIT == checksum_remote_filename_JIT:
            print(f"Checksum verified for {filename_JIT}, file transfer successful and verified.")
        else:
            print(f"Checksum mismatch for {filename_JIT}, file may have been tampered with during transfer.")
        os.remove(local_temp_file_JIT)

    sftp.close()
    transport.close()

    # Remove local files
    if df is not None:
        os.remove(filename)
    if JIT_df is not None:
        os.remove(filename_JIT)

    print("Files sent, verified, and removed successfully!")

except Exception as e:
    print("SFTP error:", e)
