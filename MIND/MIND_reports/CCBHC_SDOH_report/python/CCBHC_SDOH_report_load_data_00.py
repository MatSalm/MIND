import os, sys, time, json, pickle, configparser
from datetime import datetime
import pandas as pd, numpy as np, pyodbc
from dotenv import load_dotenv

# ------------------------------ CLI
if len(sys.argv) != 3:
    print("Usage: python 00.py <data_file.pkl> <param_file.json>")
    sys.exit(1)

data_file, param_file = sys.argv[1:3]
if os.path.exists(data_file):
    try:
        os.remove(data_file)
    except OSError:
        pass

# ------------------------------ Helpers
def get_db_connection(conn_str, retries=4, timeout=60):
    for i in range(retries):
        try:
            print(f"[DB] attempt {i + 1}/{retries}")
            cn = pyodbc.connect(conn_str, timeout=timeout, autocommit=True)
            cn.cursor().arraysize = 10000
            return cn
        except pyodbc.Error as e:
            if i == retries - 1:
                raise
            print("  connection failed:", e)
            time.sleep(5)

def split_range(txt: str):
    if not txt:
        return 0, 99999
    lo, _, hi = txt.partition("-")
    return int(lo or 0), int(hi or 99999)

# ------------------------------ Config / params
with open(param_file, "r", encoding="utf-8") as f:
    params = json.load(f)

load_dotenv("C:/MIND/MIND/MIND_config/MIND.env")

cfg = configparser.ConfigParser()
cfg.read(os.path.join("..", "config", "config.ini"), encoding="utf-8")

raw_codes = cfg.get("report", "SDOH_cpt_codes", fallback="")
SDOH_CPT_CODES = [c.strip() for c in raw_codes.split(",") if c.strip()]

exclude_classes_raw = cfg.get("report", "financial_classes_to_exclude", fallback="")
EXCLUDE_CLASSES = [s.strip() for s in exclude_classes_raw.split(",") if s.strip()]

MEASURE_YEAR = int(params.get("measure_year") or datetime.today().year)
START_DATE, END_DATE = f"{MEASURE_YEAR}-01-01", f"{MEASURE_YEAR}-12-31"
params["measure_year"] = str(MEASURE_YEAR)
with open(param_file, "w", encoding="utf-8") as f:
    json.dump(params, f, indent=2)

print(f"[INFO] Measurement Year: {MEASURE_YEAR}")
print(f"[INFO] CPT whitelist entries: {len(SDOH_CPT_CODES)}")
print(f"[INFO] Excluding financial classes: {EXCLUDE_CLASSES}")

# ------------------------------ Connections
drv, srv, prt = os.getenv("database_driver_name"), os.getenv("database_server"), os.getenv("database_port")
uid, pwd        = os.getenv("database_username"), os.getenv("database_password")
cws_db, pm_db   = os.getenv("databaseCWS"), os.getenv("databasePM")
if not all([drv, srv, prt, uid, pwd, cws_db, pm_db]):
    raise RuntimeError("Missing DB env-vars")

CWS_CONN = f"DRIVER={{{drv}}};SERVER={srv};PORT={prt};DATABASE={cws_db};UID={uid};PWD={pwd}"
PM_CONN = f"DRIVER={{{drv}}};SERVER={srv};PORT={prt};DATABASE={pm_db};UID={uid};PWD={pwd}"

# ------------------------------ SQL
NOTES_SQL = """
SELECT
        FACILITY,
        PATID,
        date_of_service,
        service_charge_code,
        location_code,
        practitioner_id,
        practitioner_name,
        service_duration,
        service_program_value,
        EPISODE_NUMBER,
        practitioner_name AS "Service Provider"
FROM AVCWS.SYSTEM.cw_patient_notes
WHERE draft_final_value = 'Final'
  AND date_of_service BETWEEN ? AND ?
  AND service_charge_code IS NOT NULL
"""

HRSN_SQL = """
SELECT PATID,
       Assess_Date,
       Draft_Final_Value,
       Data_Entry_By_Login AS "Staff Completed Assessment",
       Data_Entry_By_Login,
       Data_Entry_Date,
       Data_Entry_Time
FROM AVCWS.SYSTEM.HRSN_Screening_tool
WHERE Assess_Date BETWEEN ? AND ?
"""

EPISODE_SQL = """
SELECT PATID, EPISODE_NUMBER, program_value
FROM   AVCWS.SYSTEM.view_client_episode_history
"""

DEMO_SQL = "SELECT PATID, date_of_birth FROM AVCWS.SYSTEM.client_curr_demographics"

RACE_ETH_SQL = """
SELECT PATID,
        race_value AS race,
        ethnic_origin_value AS ethnicity
FROM   AVPM.SYSTEM.patient_current_demographics
"""

MAP_BASE = """
SELECT SERVICE_CODE,
        COALESCE(cpt_code, ub_04_code) AS base_code,
        modifier_x_ref,
        duration_range
FROM   {table}
WHERE  effective_date <= ?
  AND (end_date IS NULL OR end_date >= ?)
  AND location_code IS NULL
  AND program_code  IS NULL
  AND practitioner_category_code IS NULL
  AND age_range IS NULL
"""

LG_SQL  = MAP_BASE.format(table="system.billing_tx_max_liab_by_guar")
LG2_SQL = MAP_BASE.format(table="system.billing_tx_master_fee_table")

COVERAGE_SQL = """
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

# ------------------------------ Fetch
with get_db_connection(CWS_CONN) as cn:
    df_notes = pd.read_sql(NOTES_SQL, cn, params=(START_DATE, END_DATE))
    df_hrsn  = pd.read_sql(HRSN_SQL,  cn, params=(START_DATE, END_DATE))
    df_demo  = pd.read_sql(DEMO_SQL,  cn)
    df_epi   = pd.read_sql(EPISODE_SQL, cn)

with get_db_connection(PM_CONN) as cn:
    df_lg  = pd.read_sql(LG_SQL,  cn, params=(END_DATE, START_DATE))
    df_lg2 = pd.read_sql(LG2_SQL, cn, params=(END_DATE, START_DATE))
    df_cov = pd.read_sql(COVERAGE_SQL, cn, params=(END_DATE, START_DATE))
    df_re  = pd.read_sql(RACE_ETH_SQL, cn)

# ------------------------------ Map transform
for m in (df_lg, df_lg2):
    m["CPT"] = m["base_code"].fillna("").str.strip() + m["modifier_x_ref"].fillna("").str.strip()
    lo_hi = m["duration_range"].apply(split_range)
    m["dur_lo"] = lo_hi.str[0]
    m["dur_hi"] = lo_hi.str[1]

svc_map = (pd.concat([df_lg, df_lg2], ignore_index=True)
           .query("CPT != ''")
           .drop_duplicates(["SERVICE_CODE", "CPT", "dur_lo", "dur_hi"], keep="first"))

# ------------------------------ Notes + demo merge
df_notes.PATID = df_notes.PATID.astype(str)
df_demo.PATID  = df_demo.PATID.astype(str)
df_re.PATID    = df_re.PATID.astype(str)
df_epi.PATID   = df_epi.PATID.astype(str)

notes_full = df_notes.merge(df_demo, on="PATID", how="left") \
                     .merge(df_re, on="PATID", how="left") \
                     .merge(df_epi, on=["PATID", "EPISODE_NUMBER"], how="left")

notes_full["date_of_service"]   = pd.to_datetime(notes_full["date_of_service"], errors="coerce")
notes_full["date_of_birth"]     = pd.to_datetime(notes_full["date_of_birth"], errors="coerce")
notes_full["service_duration"]  = pd.to_numeric(notes_full["service_duration"], errors="coerce")

# attach CPT (cartesian, then duration filter)
notes_full = notes_full.merge(svc_map,
                    left_on="service_charge_code",
                    right_on="SERVICE_CODE",
                    how="left")

mask = (
    notes_full["service_duration"].isna()
    | (
        (notes_full["service_duration"] >= notes_full["dur_lo"])
        & (notes_full["service_duration"] <= notes_full["dur_hi"])
    )
)

notes_full = notes_full[mask & notes_full["CPT"].isin(SDOH_CPT_CODES)]
notes = notes_full.drop_duplicates("PATID")

# ------------------------------ Insurance cleanup
df_cov["PATID"] = df_cov["PATID"].astype(str)
df_cov = df_cov[df_cov["financial_class_value"].notna()]
df_cov = df_cov[~df_cov["financial_class_value"].isin(EXCLUDE_CLASSES)]
df_cov["is_medicaid"] = df_cov["financial_class_value"] == "Medicaid"

coverage_flags = (df_cov.groupby("PATID")["is_medicaid"]
                         .any()
                         .reset_index()
                         .rename(columns={"is_medicaid": "Insurance"}))

coverage_flags["Insurance"] = coverage_flags["Insurance"].map(
    {True: "Medicaid", False: "Other Insurance"}
)

notes_full = notes_full.merge(coverage_flags, on="PATID", how="left")
notes_full["Insurance"] = notes_full["Insurance"].fillna("No Entry (Insurance)")

# ------------------------------ Output
payload = {
    "windows": {"MY_START": START_DATE, "MY_END": END_DATE},
    "notes":   notes,
    "notes_full": notes_full,
    "hrsn":    df_hrsn.astype({"PATID": str}),
    "hrsn_all": df_hrsn.copy().astype({"PATID": str}),
    "sdoh_cpt_codes": SDOH_CPT_CODES,
    "coverage": df_cov,
    "demo": df_re
}

with open(data_file, "wb") as f:
    pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

print(f"[OK] {len(notes):,} unique clients with a final HRSN assessment + valid CPT", flush=True)
