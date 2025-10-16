#!/usr/bin/env python3
"""
bamboo_health_client_export_report_send_03.py

Uploads the Submission_PatientPing_PatientRoster CSV file to the SFTP server.
Loads report date from a JSON file and SFTP credentials from MIND.env.

Usage:
    python bamboo_health_client_export_report_send_03.py <data_pickle> <params_json>
    python bamboo_health_client_export_report_send_03.py --params temp_params.json
"""
import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

import paramiko
from dotenv import load_dotenv

# Load environment variables from MIND.env
load_dotenv(override=False)

# Parse CLI arguments
cli = argparse.ArgumentParser(description="Upload PatientRoster CSV via SFTP.")
cli.add_argument("data_pickle", nargs="?", default=None, help="(ignored)")
cli.add_argument("params_json", nargs="?", default="temp_params.json", help="Path to JSON params file")
cli.add_argument("--params", "-p", dest="params", help="Override params JSON file", default=None)
args = cli.parse_args()

param_file = Path(args.params or args.params_json)
if not param_file.exists():
    sys.exit(f"Params file not found: {param_file}")

# Read report_date from JSON
try:
    params = json.loads(param_file.read_text(encoding="utf-8"))
except json.JSONDecodeError as e:
    sys.exit(f"Error parsing params JSON: {e}")

report_date = params.get("report_date")
if not report_date:
    sys.exit("Missing 'report_date' in params file.")

try:
    date_str = datetime.strptime(report_date, "%Y-%m-%d").strftime("%Y%m%d")
except ValueError:
    sys.exit(f"Invalid report_date '{report_date}', expected YYYY-MM-DD.")

local_file = f"Submission_PatientPing_PatientRoster_{date_str}.csv"
if not Path(local_file).exists():
    sys.exit(f"Roster CSV not found: {local_file}")

# Read SFTP connection info from env vars
host       = os.getenv("bamboo_sftp_hostname", "").strip()
username   = os.getenv("bamboo_sftp_username", "").strip()
port       = int(os.getenv("bamboo_sftp_port", "22").strip())
key_file   = os.getenv("bamboo_sftp_private_key_file_path", "").strip()
remote_dir = os.getenv("bamboo_sftp_remote_path", "").strip()

missing = [n for n, v in [
    ("bamboo_sftp_hostname", host),
    ("bamboo_sftp_username", username),
    ("bamboo_sftp_private_key_file_path", key_file),
    ("bamboo_sftp_remote_path", remote_dir)
] if not v]
if missing:
    sys.exit(f"Missing env vars: {', '.join(missing)}")

# Connect and upload
print(f"Connecting to {host}:{port} as {username}...")
client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.RejectPolicy())

try:
    pkey = paramiko.RSAKey.from_private_key_file(key_file)
except Exception as e:
    sys.exit(f"Failed to load private key '{key_file}': {e}")

try:
    client.connect(
        hostname=host,
        port=port,
        username=username,
        pkey=pkey,
        allow_agent=False,
        look_for_keys=False
    )
    sftp = client.open_sftp()
    remote_path = remote_dir.rstrip("/") + f"/{local_file}"
    print(f"Uploading {local_file} to {remote_path}...")
    sftp.put(local_file, remote_path)
    print("Upload complete.")
finally:
    try:
        sftp.close()
        client.close()
    except:
        pass

print("[OK] Upload script finished successfully.")

# Cleanup after successful upload
print("Cleaning up local files...")
try:
    # Remove exported CSV
    Path(local_file).unlink()
    print(f"Deleted: {local_file}")
    
    # Remove pickle file if provided
    if args.data_pickle and Path(args.data_pickle).exists():
        Path(args.data_pickle).unlink()
        print(f"Deleted: {args.data_pickle}")
    
    # Remove param JSON file if not manually specified with --params
    if not args.params and param_file.exists():
        param_file.unlink()
        print(f"Deleted: {param_file.name}")

except Exception as cleanup_err:
    print(f"Warning: Cleanup failed with error: {cleanup_err}")

print("[OK] Upload and cleanup completed.")
