#!/usr/bin/env python3
# 00.py – Data loader for CCBHC I‑SERV Sub‑measures 1 & 2

import os, sys, time, json, pickle, configparser
from datetime import datetime
import pandas as pd, pyodbc
from dotenv import load_dotenv

# ───────────────────────────────────────────────────────
# CLI arguments
# ────────────────────────────────────────────────────
if len(sys.argv) != 3:
    print("Usage: python 00.py <data_file.pkl> <param_file.json>")
    sys.exit(1)

data_file, param_file = sys.argv[1], sys.argv[2]

if os.path.exists(data_file):
    try:
        os.remove(data_file)
    except OSError as e:
        print(f"[WARN] Could not delete old {data_file}: {e}", file=sys.stderr)

# ────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────

def get_db_connection(conn_string, max_retries=4, timeout=60):
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[DB] attempt {attempt}/{max_retries}")
            return pyodbc.connect(conn_string, timeout=timeout)
        except pyodbc.Error:
            if attempt == max_retries:
                raise
            time.sleep(5)

def build_windows(my):
    start = pd.Timestamp(f"{my}-01-01")
    end   = pd.Timestamp(f"{my}-12-31")
    return {
        "MY_START"   : start,
        "MY_END"     : end,
        "DENOM_START": start - pd.DateOffset(months=6),
        "DENOM_END"  : end   - pd.DateOffset(days=30),
    }

# ────────────────────────────────────────────────────
# Load parameters and config
# ────────────────────────────────────────────────────
with open(param_file, "r", encoding="utf-8") as f:
    params = json.load(f)

load_dotenv("C:/MIND/MIND/MIND_config/MIND.env")

cfg = configparser.ConfigParser()
cfg.read(os.path.join("..", "config", "config.ini"))

exclude_classes_raw = cfg.get("report", "financial_classes_to_excldue", fallback="")
EXCLUDE_CLASSES = [s.strip() for s in exclude_classes_raw.split(",") if s.strip()]

# Determine MEASURE_YEAR with config fallback
try:
    measure_year_param = int(params.get("measure_year", "").strip())
except Exception:
    measure_year_param = None

if not measure_year_param:
    try:
        cfg_measure_year = int(cfg.get("report", "measure_year", fallback="").strip())
    except Exception:
        cfg_measure_year = None
    measure_year_param = cfg_measure_year or datetime.today().year

MEASURE_YEAR = measure_year_param
WIN = build_windows(MEASURE_YEAR)

# Update param file with final measure year
params["measure_year"] = str(MEASURE_YEAR)
with open(param_file, "w", encoding="utf-8") as f:
    json.dump(params, f, indent=2)

print(f"[INFO] Measurement Year = {MEASURE_YEAR}")
print(f"[INFO] Excluding financial classes: {EXCLUDE_CLASSES}")

# DB connections
drv, srv, prt = os.getenv("database_driver_name"), os.getenv("database_server"), os.getenv("database_port")
uid, pwd      = os.getenv("database_username"), os.getenv("database_password")
cws_db, pm_db = os.getenv("databaseCWS"), os.getenv("databasePM")

if not all([drv, srv, prt, uid, pwd, cws_db, pm_db]):
    raise ValueError("Missing one or more DB env‑vars")

CWS_CONN = f"DRIVER={{{drv}}};SERVER={srv};PORT={prt};DATABASE={cws_db};UID={uid};PWD={pwd}"
PM_CONN  = f"DRIVER={{{drv}}};SERVER={srv};PORT={prt};DATABASE={pm_db};UID={uid};PWD={pwd}"

# SQL queries
appt_sql = """
SELECT  a.PATID, a.patient_name, a.STAFFID, a.staff_name,
        a.appointment_date, a.appointment_start_time, a.appointment_end_time,
        ad.orig_entry_date
FROM    AVPM.SYSTEM.appt_data a
JOIN    AVPM.SYSTEM.AppointmentData ad
      ON a.STAFFID = ad.STAFFID
     AND a.appointment_date       = ad.appointment_date
     AND a.appointment_start_time = ad.appointment_start_time
     AND a.appointment_end_time   = ad.appointment_end_time
WHERE   ad.orig_entry_date BETWEEN ? AND ?
"""

notes_sql = """
SELECT PATID, EPISODE_NUMBER, date_of_service, service_charge_code
FROM   AVCWS.SYSTEM.cw_patient_notes
WHERE  draft_final_code = 'F'
  AND  date_of_service BETWEEN ? AND ?
  AND  service_charge_code IS NOT NULL
"""

assessment_sql = """
SELECT PATID, Assess_Date
FROM   AVCWS.SYSTEM.Comprehensive_Assessment
WHERE  Assess_Date BETWEEN ? AND ?
"""

demo_sql = """
SELECT PATID,
       race_value          AS race,
       ethnic_origin_value AS ethnicity,
       patient_sex_value   AS sex,
       date_of_birth       AS dob
FROM   AVPM.SYSTEM.patient_current_demographics
"""

coverage_sql = """
SELECT  e.PATID, EPISODE_NUMBER, e.GUARANTOR_ID,
        e.cov_effective_date AS eff,
        e.cov_expiration_date AS exp,
        g.financial_class_value
FROM    AVPM.SYSTEM.billing_guar_emp_data e
LEFT JOIN AVPM.SYSTEM.billing_guar_table g
       ON e.GUARANTOR_ID = g.GUARANTOR_ID
WHERE   e.cov_effective_date <= ?
  AND  (e.cov_expiration_date >= ? OR e.cov_expiration_date IS NULL)
"""

# Load DataFrames
with get_db_connection(PM_CONN) as conn_pm:
    df_appt = pd.read_sql(appt_sql, conn_pm, params=(WIN["DENOM_START"].date(), WIN["MY_END"].date()))
    df_demo = pd.read_sql(demo_sql, conn_pm)

with get_db_connection(CWS_CONN) as conn_cws:
    df_notes = pd.read_sql(notes_sql, conn_cws, params=(WIN["DENOM_START"].date(), WIN["MY_END"].date()))
    df_assess = pd.read_sql(assessment_sql, conn_cws, params=(WIN["DENOM_START"].date(), WIN["MY_END"].date()))

with get_db_connection(PM_CONN) as conn_pm:
    df_cov = pd.read_sql(coverage_sql, conn_pm, params=(WIN["MY_END"].date(), WIN["DENOM_START"].date()))

# Clean and filter
for df in (df_appt, df_notes, df_assess, df_demo, df_cov):
    if "PATID" in df.columns:
        df["PATID"] = df["PATID"].astype(str)

df_assess["Assess_Date"] = pd.to_datetime(df_assess["Assess_Date"])

# Remove test data
test_mask = df_appt["patient_name"].str.upper().str.contains("TEST", na=False)
test_patids = set(df_appt.loc[test_mask, "PATID"])

df_appt   = df_appt.loc[~test_mask].drop(columns=["patient_name"])
df_notes  = df_notes[~df_notes["PATID"].isin(test_patids)]
df_assess = df_assess[~df_assess["PATID"].isin(test_patids)]
df_demo   = df_demo[~df_demo["PATID"].isin(test_patids)]
df_cov    = df_cov[~df_cov["PATID"].isin(test_patids)]

# Filter coverage
df_cov = df_cov[df_cov["financial_class_value"].notna()]
df_cov = df_cov[~df_cov["financial_class_value"].isin(EXCLUDE_CLASSES)]
df_cov.loc[:, "is_medicaid"] = df_cov["financial_class_value"] == "Medicaid"

# Save payload
payload = {
    "windows":      {k: str(v) for k, v in WIN.items()},
    "appointments": df_appt,
    "notes":        df_notes,
    "assessments":  df_assess,
    "demographics": df_demo,
    "coverage":     df_cov,
    "excluded_patids_TEST": list(test_patids),
}

with open(data_file, "wb") as f:
    pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

print(
    "[OK] MY", MEASURE_YEAR,
    "appts",   len(df_appt),
    "notes",   len(df_notes),
    "assess",  len(df_assess),
    "demos",   len(df_demo),
    "cov",     len(df_cov),
    "excluded TEST PATIDs", len(test_patids),
    data_file, flush=True,
)
