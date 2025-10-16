#!/usr/bin/env python3
"""
bamboo_health_client_export_report_load_data_00.py
-----------------------------------------------------------------
Pull raw AVPM / AVCWS data for the Bamboo-Health roster and
pickle it (together with SFTP creds). 01.py will do the renaming
and final column ordering to match the roster specification.

Revision history
----------------
2025-05-13 … Initial refactor
2025-05-22 … Insurance section slimmed; plan-type logic removed
2025-05-25 … Added clean-up pass that blanks SQL NULL / NaN / literal
             "No Entry" across *all* dataframes so the roster has no
             disqualifying values.
2025-05-22 … **FIX** - policy number now comes from
             SYSTEM.billing_guar_subs_data.subs_policy (joined on
             EPISODE_NUMBER, PATID, GUARANTOR_ID); guarantor name still
             from SYSTEM.billing_guar_table.
2025-06-xx … **NEW** - skip any guarantors listed in
             non_insurance_guarantors in [report] of config.ini.
"""

import os
import sys
import json
import pickle
import time
import argparse
import configparser
import pyodbc
from pathlib import Path
from datetime import date

import pandas as pd
from dotenv import load_dotenv

# ─────────────── 0. helpers ────────────────────────────────────────────────
def clean_value(val):
    """
    Replace NULL / NaN / literal 'No Entry' (case-insensitive) with ''.
    Pass all other values through unchanged.
    """
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    if isinstance(val, str) and val.strip().lower() in {"", "no entry", "none", "null", "nan"}:
        return ""
    return val


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply *clean_value* to every object-dtype column in *df* in-place,
    then return *df* for convenience chaining.
    """
    obj_cols = df.select_dtypes(include="object").columns
    for col in obj_cols:
        df[col] = df[col].apply(clean_value)
    return df
# ───────────────────────────────────────────────────────────────────────────

# ─────────────── 1. CLI ────────────────────────────────────────────────────
cli = argparse.ArgumentParser()
cli.add_argument("data_file")
cli.add_argument("param_file")
cli.add_argument("--config", "-c")
args = cli.parse_args()

DATA_FILE  = args.data_file
PARAM_FILE = args.param_file

# ─────────────── 2. env / INI helpers ─────────────────────────────────────
env_file = Path(__file__).parents[2] / "MIND.env"
if env_file.exists():
    load_dotenv(env_file, override=False)

cfg = configparser.ConfigParser()
cfg.read(args.config or Path(__file__).parent.parent / "config" / "config.ini")
ini = cfg["database"] if "database" in cfg else {}

# pull out non-insurance guarantors list
non_ins_list = [
    name.strip()
    for name in cfg.get("report", "non_insurance_guarantors", fallback="").split(",")
    if name.strip()
]

def first(*keys, default=None):
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return default

def cfg_or_env(ini_key, *env_keys, fallback=""):
    return first(*env_keys, default=ini.get(ini_key, "") or fallback)

driver  = cfg_or_env("driver",    "ODBC_DRIVER",  "database_driver_name", fallback="ODBC Driver 18 for SQL Server")
server  = cfg_or_env("host",      "DB_SERVER",    "database_server")
port    = cfg_or_env("port",      "DB_PORT",      "database_port", fallback="1433")
user    = cfg_or_env("user",      "DB_USER",      "database_username")
pwd     = cfg_or_env("password",  "DB_PASS",      "database_password")
db_pm   = cfg_or_env("namePM",    "DB_NAME_PM",   "databasePM")
db_cws  = cfg_or_env("nameCWS",   "DB_NAME_CWS",  "databaseCWS")

sftp_host        = cfg_or_env("sftp_hostname",            "BAMBOO_SFTP_HOSTNAME",            "bamboo_sftp_hostname",            fallback="submissions.healthcarecoordination.net")
sftp_user        = cfg_or_env("sftp_username",            "BAMBOO_SFTP_USERNAME",            "bamboo_sftp_username")
sftp_pass        = cfg_or_env("sftp_password",            "BAMBOO_SFTP_PASSWORD",            "bamboo_sftp_password")
sftp_port        = int(cfg_or_env("sftp_port",            "BAMBOO_SFTP_PORT",                "bamboo_sftp_port",                fallback="22"))
sftp_private_key = cfg_or_env("sftp_private_key_file_path","BAMBOO_SFTP_PRIVATE_KEY_FILE_PATH","bamboo_sftp_private_key_file_path")
sftp_remote_path = cfg_or_env("sftp_remote_path",          "BAMBOO_SFTP_REMOTE_PATH",         "bamboo_sftp_remote_path")

if driver not in pyodbc.drivers():
    sys.exit("ODBC driver not installed. Available: " + ", ".join(pyodbc.drivers()))

def conn_str(db):
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};PORT={port};DATABASE={db};"
        f"UID={user};PWD={pwd};Encrypt=no"
    )

def connect(cs, retries=8, tout=60):
    for i in range(1, retries + 1):
        try:
            return pyodbc.connect(cs, timeout=tout)
        except pyodbc.Error as e:
            print(f"  connection attempt {i}/{retries} failed -> {e}")
            time.sleep(5)
    raise RuntimeError("DB connection failed.")

# ─────────────── 3. load pickle / params ──────────────────────────────────
data = {}
if Path(DATA_FILE).exists():
    try:
        with open(DATA_FILE, "rb") as f:
            loaded = pickle.load(f)
            if isinstance(loaded, dict):
                data = loaded
            else:
                print("Warning: existing pickle is not a dict - starting fresh.")
    except Exception as e:
        print(f"Warning: could not read existing pickle ({e}); starting fresh.")

with open(PARAM_FILE, "r", encoding="utf-8") as f:
    params = json.load(f)

# ─────────────── 4. connect to AVPM / AVCWS ───────────────────────────────
pm_conn  = connect(conn_str(db_pm))
cws_conn = connect(conn_str(db_cws))

print("Running queries…")

# ─────────────── 5. open episodes ─────────────────────────────────────────
df_episode = pd.read_sql(
    """
    SELECT PATID, EPISODE_NUMBER, program_value
    FROM   SYSTEM.episode_history
    WHERE  date_of_discharge IS NULL
    """,
    pm_conn,
)
patid_sql = ",".join(f"'{pid}'" for pid in df_episode.PATID.unique())

# ─────────────── 6. latest finalised note ─────────────────────────────────
df_notes = pd.read_sql(
    f"""
SELECT PATID, EPISODE_NUMBER, program_value,
       practitioner_id AS STAFFID, practitioner_name, date_of_service
FROM (
  SELECT n.PATID, n.EPISODE_NUMBER, e.program_value,
         n.practitioner_id, n.practitioner_name, n.date_of_service,
         ROW_NUMBER() OVER (
           PARTITION BY n.PATID, n.EPISODE_NUMBER
           ORDER BY n.date_of_service DESC
         ) rn
  FROM SYSTEM.cw_patient_notes n
  JOIN SYSTEM.view_client_episode_history e
    ON e.PATID = n.PATID
   AND e.EPISODE_NUMBER = n.EPISODE_NUMBER
   AND e.date_of_discharge IS NULL
  WHERE n.draft_final_code = 'F'
    AND n.PATID IN ({patid_sql})
) x
WHERE rn = 1
""",
    cws_conn,
)

# ─────────────── 7. provider directory ────────────────────────────────────
staff_ids = df_notes.STAFFID.dropna().unique().tolist()
staff_sql = ",".join(str(s) for s in staff_ids) if staff_ids else "''"

df_staff = pd.read_sql(
    f"""
    SELECT STAFFID,
           staff_name,
           prac_credentials_value,
           NPI_number
    FROM   SYSTEM.staff_enrollment_history
    WHERE  STAFFID IN ({staff_sql})
    """,
    pm_conn,
)

if not df_staff.empty:
    split = df_staff["staff_name"].str.split(",", n=1, expand=True)
    df_staff["FIRST_NAME"] = split[1].str.strip()
    df_staff["LAST_NAME"]  = split[0].str.strip()
else:
    df_staff["FIRST_NAME"] = df_staff["LAST_NAME"] = ""

df_staff.rename(
    columns={
        "prac_credentials_value": "HONORIFICS",
        "NPI_number": "NPI",
    },
    inplace=True,
)

# ─────────────── 8. demographics ──────────────────────────────────────────
df_demo = pd.read_sql(
    f"""
    SELECT *
    FROM   SYSTEM.patient_current_demographics
    WHERE  PATID IN ({patid_sql})
    """,
    pm_conn,
)

# ─────────────── 9. two most recent billed guarantors ─────────────────────
print("Pulling guarantor info for active clients…")

CHUNK_SQL = """
SELECT  b.PATID,
        g.guarantor_name    AS INSURER,
        s.subs_policy       AS POLICY_NUMBER,
        MAX(b.date_of_service) AS last_billed
FROM    SYSTEM.billing_tx_charge_detail b
LEFT    JOIN SYSTEM.billing_guar_table g
       ON b.GUARANTOR_ID = g.GUARANTOR_ID
LEFT    JOIN SYSTEM.billing_guar_subs_data s
       ON b.PATID = s.PATID
      AND b.GUARANTOR_ID = s.GUARANTOR_ID
      AND b.EPISODE_NUMBER = s.EPISODE_NUMBER
WHERE   b.PATID IN ({patid_in})
GROUP BY b.PATID, g.guarantor_name, s.subs_policy
"""

chunk_size = 5000
patids     = df_episode.PATID.unique().tolist()
frames     = []

for idx in range(0, len(patids), chunk_size):
    chunk = patids[idx: idx + chunk_size]
    sql   = CHUNK_SQL.format(patid_in=",".join(f"'{p}'" for p in chunk))
    frame = pd.read_sql(sql, pm_conn)
    frames.append(frame)
    print(f"  -> chunk {idx//chunk_size+1}: {len(frame):,} rows")

df_guar = pd.concat(frames, ignore_index=True)

# filter out non-insurance guarantors
if non_ins_list:
    print(" Excluding non-insurance guarantors:", non_ins_list)
    df_guar = df_guar[~df_guar["INSURER"].isin(non_ins_list)]

df_guar = df_guar.sort_values(["PATID", "last_billed", "INSURER"], ascending=[True, False, False])
df_guar["rn"] = df_guar.groupby("PATID").cumcount() + 1

if df_guar.empty:
    df_insurance = pd.DataFrame(columns=[
        "PATID",
        "INSURER_1", "INSURANCE_NUMBER_1",
        "INSURER_2", "INSURANCE_NUMBER_2",
    ])
else:
    df_insurance = (
        df_guar[df_guar["rn"] <= 2]
        .pivot_table(
            index="PATID",
            columns="rn",
            values=["INSURER", "POLICY_NUMBER"],
            aggfunc="first"
        )
        .rename(columns={
            ("INSURER", 1): "INSURER_1",
            ("POLICY_NUMBER", 1): "INSURANCE_NUMBER_1",
            ("INSURER", 2): "INSURER_2",
            ("POLICY_NUMBER", 2): "INSURANCE_NUMBER_2",
        })
        .reset_index()
    )

df_insurance = clean_df(df_insurance)
print(f"Guarantor rows after pivot: {len(df_insurance):,}")

# ─────────────── 10. program enrolment (up to 10) ─────────────────────────
prog_seen = (
    df_notes.groupby(["PATID", "program_value"], as_index=False)["date_of_service"].max()
    .rename(columns={"date_of_service": "last_service"})
    .sort_values(["PATID", "last_service"], ascending=[True, False])
)
prog_seen["rn"] = prog_seen.groupby("PATID").cumcount() + 1
df_prog_enroll = (
    prog_seen[prog_seen["rn"] <= 10]
    .pivot(index="PATID", columns="rn", values="program_value")
    .add_prefix("PROGRAM_")
    .reset_index()
)

# ─────────────── 11. program contacts ─────────────────────────────────────
prog_vals = prog_seen["program_value"].dropna().unique()
df_prog_defs = (
    pd.read_sql(
        f"""
        SELECT program_value,
               program_X_fax_number   AS FAX,
               program_X_phone_number AS PHONE
        FROM   SYSTEM.table_program_definition
        WHERE  program_value IN ({','.join(f"'{v}'" for v in prog_vals)})
        """,
        pm_conn,
    ) if prog_vals.size else pd.DataFrame()
)

# ─────────────── 12. facility defaults ────────────────────────────────────
df_practice = pd.read_sql(
    """
    SELECT provider_name, provider_phone
    FROM   SYSTEM.table_facility_defaults
    WHERE  FACILITY = '1'
    """,
    pm_conn,
)

practice_email = cfg.get("report", "practice_main_email", fallback="")
if df_practice.empty:
    df_practice = pd.DataFrame({
        "PRACTICE_NAME":  [""],
        "PRACTICE_PHONE": [""],
        "PRACTICE_EMAIL": [practice_email],
    })
else:
    df_practice.rename(columns={
        "provider_name":  "PRACTICE_NAME",
        "provider_phone": "PRACTICE_PHONE",
    }, inplace=True)
    df_practice["PRACTICE_EMAIL"] = practice_email

# ─────────────── 13. clean all frames ─────────────────────────────────────
for df in (df_episode, df_notes, df_staff, df_demo, df_prog_enroll, df_prog_defs, df_practice):
    clean_df(df)

pm_conn.close()
cws_conn.close()

# ─────────────── 14. save raw frames ──────────────────────────────────────
data.update(
    df_episode      = df_episode,
    df_notes        = df_notes,
    df_staff        = df_staff,
    df_demo         = df_demo,
    df_insurance    = df_insurance,
    df_prog_enroll  = df_prog_enroll,
    df_prog_defs    = df_prog_defs,
    df_practice     = df_practice,
    sftp_credentials = dict(
        host             = sftp_host,
        user             = sftp_user,
        password         = sftp_pass,
        port             = sftp_port,
        private_key_file = sftp_private_key,
        remote_path      = sftp_remote_path,
    ),
)

with open(DATA_FILE, "wb") as f:
    pickle.dump(data, f)

params["report_date"] = date.today().strftime("%Y-%m-%d")
with open(PARAM_FILE, "w", encoding="utf-8") as f:
    json.dump(params, f, indent=4)

print(f"[OK] raw data saved -> {DATA_FILE}  (report_date={params['report_date']})")
