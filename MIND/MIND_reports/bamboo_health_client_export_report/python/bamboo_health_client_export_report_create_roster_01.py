#!/usr/bin/env python3
"""
bamboo_health_client_export_report_create_roster_01.py
-----------------------------------------------------------------
Build the **final Bamboo-Health roster** dataframe.

Reads the raw pickle produced by 00.py, merges/cleans everything, and
writes back a pickle containing **roster_df** with exactly the
approved columns.

Strict format validation follows the *ALLOWED FORMAT* rules.
Illegal characters are stripped (not dropped), except any patient
whose first or last name contains "test" (case-insensitive) is
removed entirely. Policy numbers (insurance_number fields) are
preserved intact (only NaNs are replaced with empty strings).

All code ASCII-safe for Windows cmd.exe.
"""

import sys
import pickle
import re
from datetime import datetime

import pandas as pd

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
if len(sys.argv) != 3:
    print("Usage: python 01.py <data_file> <param_file>")
    sys.exit(1)

data_file, param_file = sys.argv[1], sys.argv[2]

# ------------------------------------------------------------------
# Load raw frames from pickle
# ------------------------------------------------------------------
with open(data_file, "rb") as f:
    raw = pickle.load(f)

needed = ["df_demo", "df_notes", "df_insurance", "df_staff", "df_practice"]
missing = [n for n in needed if n not in raw]
if missing:
    raise RuntimeError("Missing frame(s) in pickle: " + ", ".join(missing))

df_demo      = raw["df_demo"].copy()
df_notes     = raw["df_notes"].copy()
df_insurance = raw["df_insurance"].copy()
df_staff     = raw["df_staff"].copy()
df_practice  = raw["df_practice"].copy()

# ------------------------------------------------------------------
# Flatten df_insurance in case of MultiIndex columns
# ------------------------------------------------------------------
if hasattr(df_insurance.columns, "to_flat_index"):
    flat = df_insurance.columns.to_flat_index()
    df_insurance.columns = [
        "_".join(str(p) for p in col if p not in (None, "")) if isinstance(col, tuple)
        else str(col)
        for col in flat
    ]
# ------------------------------------------------------------------
# Ensure correct policy-number column names
# ------------------------------------------------------------------
# If 00.py left these as POLICY_NUMBER_1/2, rename them now so
# the downstream merge and fillna target INSURANCE_NUMBER_1/2.
rename_map_ins = {}
if "POLICY_NUMBER_1" in df_insurance.columns:
    rename_map_ins["POLICY_NUMBER_1"] = "INSURANCE_NUMBER_1"
if "POLICY_NUMBER_2" in df_insurance.columns:
    rename_map_ins["POLICY_NUMBER_2"] = "INSURANCE_NUMBER_2"
if rename_map_ins:
    df_insurance.rename(columns=rename_map_ins, inplace=True)

# ------------------------------------------------------------------
# Pivot programs & align providers by recency
# ------------------------------------------------------------------
prog_notes = df_notes[["PATID", "program_value", "STAFFID", "date_of_service"]].copy()
prog_notes.sort_values(
    ["PATID", "program_value", "date_of_service"],
    ascending=[True, True, False],
    inplace=True
)
prog_seen = prog_notes.drop_duplicates(subset=["PATID", "program_value"], keep="first")
prog_seen["last_service"] = prog_seen["date_of_service"]
prog_seen.sort_values(["PATID", "last_service"], ascending=[True, False], inplace=True)
prog_seen["rn"] = prog_seen.groupby("PATID").cumcount() + 1

df_programs = (
    prog_seen[prog_seen["rn"] <= 10]
      .pivot(index="PATID", columns="rn", values="program_value")
      .add_prefix("PROGRAM_")
      .reset_index()
)

df_providers = (
    prog_seen[prog_seen["rn"] <= 2]
      .pivot(index="PATID", columns="rn", values="STAFFID")
      .add_prefix("STAFFID_")
      .reset_index()
)

# ------------------------------------------------------------------
# Merge everything together
# ------------------------------------------------------------------
base = (
    df_demo
      .merge(df_insurance, on="PATID", how="left")
      .merge(df_programs,  on="PATID", how="left")
      .merge(df_providers, on="PATID", how="left")
      .merge(df_practice,  how="cross")
)

# ------------------------------------------------------------------
# Initialize INSURANCE_PLAN columns as blank
# ------------------------------------------------------------------
base["INSURANCE_PLAN_1"] = ""
base["INSURANCE_PLAN_2"] = ""

# ------------------------------------------------------------------
# Join provider details for STAFFID_1 & STAFFID_2
# ------------------------------------------------------------------
prov = df_staff[["STAFFID", "FIRST_NAME", "LAST_NAME", "HONORIFICS", "NPI"]].drop_duplicates(subset=["STAFFID"])
base = (
    base
      .merge(prov.add_suffix("_1"), left_on="STAFFID_1", right_on="STAFFID_1", how="left")
      .merge(prov.add_suffix("_2"), left_on="STAFFID_2", right_on="STAFFID_2", how="left")
)

# ------------------------------------------------------------------
# Ensure PROGRAM_1..PROGRAM_10 and STAFFID_1..STAFFID_2 exist
# ------------------------------------------------------------------
for i in range(1, 11):
    c = f"PROGRAM_{i}"
    if c not in base.columns:
        base[c] = ""
for p in (1, 2):
    c = f"STAFFID_{p}"
    if c not in base.columns:
        base[c] = ""

# ------------------------------------------------------------------
# Rename to final roster headers
# ------------------------------------------------------------------
rename_map = {
    "PATID":                      "PATIENT_ID",
    "patient_name_first":         "PATIENT_FIRST_NAME",
    "patient_name_middle":        "PATIENT_MIDDLE_INITIAL",
    "patient_name_last":          "PATIENT_LAST_NAME",
    "patient_name_suffix_value":  "PATIENT_SUFFIX",
    "date_of_birth":              "PATIENT_DOB",
    "patient_sex_code":           "PATIENT_GENDER",
    "patient_add_street_1":       "PATIENT_ADDRESS_1",
    "patient_add_street_2":       "PATIENT_ADDRESS_2",
    "patient_add_city":           "PATIENT_ADDRESS_CITY",
    "patient_add_state_code":     "PATIENT_ADDRESS_STATE",
    "patient_add_zipcode":        "PATIENT_ADDRESS_ZIP",
    "patient_cell_phone":         "PATIENT_PHONE_MOBILE",
    "patient_home_phone":         "PATIENT_PHONE_HOME",
    "FIRST_NAME_1":               "ATTRIBUTED_PROVIDER_FIRST_NAME_1",
    "LAST_NAME_1":                "ATTRIBUTED_PROVIDER_LAST_NAME_1",
    "HONORIFICS_1":               "ATTRIBUTED_PROVIDER_HONORIFICS_1",
    "NPI_1":                      "ATTRIBUTED_PROVIDER_NPI_1",
    "FIRST_NAME_2":               "ATTRIBUTED_PROVIDER_FIRST_NAME_2",
    "LAST_NAME_2":                "ATTRIBUTED_PROVIDER_LAST_NAME_2",
    "HONORIFICS_2":               "ATTRIBUTED_PROVIDER_HONORIFICS_2",
    "NPI_2":                      "ATTRIBUTED_PROVIDER_NPI_2",
    "PRACTICE_NAME":              "PRACTICE_NAME_1",
    "PRACTICE_PHONE":             "PRACTICE_PHONE_1",
    "PRACTICE_EMAIL":             "PRACTICE_EMAIL_1",
}
base.rename(columns=rename_map, inplace=True)

# ------------------------------------------------------------------
# Allowed-format cleaning helpers
# ------------------------------------------------------------------
ID_CHARS             = r"A-Za-z0-9\-_\/\.\| "
FIRST_NAME_CHARS     = r"A-Za-z'\- \.\,"
MIDDLE_INITIAL_CHARS = r"A-Za-z"
LAST_NAME_CHARS      = r"A-Za-z0-9'\- \.\,"
SUFFIX_CHARS         = r"A-Za-z\."
ADDRESS_CHARS        = r"A-Za-z0-9\-\#\\&'.,:\/ "
CITY_CHARS           = r"A-Za-z0-9\-\./' "
STATE_CHARS          = r"A-Za-z"
ZIP_CHARS_US         = r"0-9\-"
INSURER_CHARS        = r"A-Za-z0-9&,\-\/: "
PROVIDER_NAME_CHARS  = r"A-Za-z'\.\,&\(\)\-"
HONORIFICS_CHARS     = r"A-Za-z\-\ "
PRACTICE_NAME_CHARS  = r"A-Za-z0-9\-\#\/\&\(\);\:\_\+ "
EMAIL_CHARS          = r"A-Za-z0-9\-\_\@\%\+\. "
PROGRAM_CHARS        = r"A-Za-z0-9,\.\&'#/+\(\);\:\_\-\[\]\$\| "

def strip_chars(val, allowed, max_len):
    if pd.isna(val):
        return ""
    return re.sub(f"[^{allowed}]", "", str(val))[:max_len]

def clean_date(val):
    try:
        dt = pd.to_datetime(val)
        return "" if dt > datetime.today() else dt.strftime("%Y-%m-%d")
    except:
        return ""

_allowed_genders = {"M","F","U","O","X","MALE","FEMALE","UNKNOWN","OTHER","NON-BINARY"}
def clean_gender(val):
    v = str(val).strip().upper()
    return v if v in _allowed_genders else ""

def digits_only(val, length):
    if pd.isna(val):
        return ""
    d = re.sub(r"\D", "", str(val))
    return d if len(d) == length else ""

# ------------------------------------------------------------------
# Field-by-field cleanup
# ------------------------------------------------------------------
base["PATIENT_ID"]             = base["PATIENT_ID"].apply(lambda v: strip_chars(v, ID_CHARS, 50))
base["PATIENT_FIRST_NAME"]     = base["PATIENT_FIRST_NAME"].apply(lambda v: strip_chars(v, FIRST_NAME_CHARS, 50))
base["PATIENT_MIDDLE_INITIAL"] = base["PATIENT_MIDDLE_INITIAL"].apply(lambda v: strip_chars(v, MIDDLE_INITIAL_CHARS, 1))
base["PATIENT_LAST_NAME"]      = base["PATIENT_LAST_NAME"].apply(lambda v: strip_chars(v, LAST_NAME_CHARS, 50))
base["PATIENT_SUFFIX"]         = base["PATIENT_SUFFIX"].apply(lambda v: strip_chars(v, SUFFIX_CHARS, 5))
base["PATIENT_DOB"]            = base["PATIENT_DOB"].apply(clean_date)
base["PATIENT_GENDER"]         = base["PATIENT_GENDER"].apply(clean_gender)

base["PATIENT_ADDRESS_1"]    = base["PATIENT_ADDRESS_1"].apply(lambda v: strip_chars(v, ADDRESS_CHARS, 100))
base["PATIENT_ADDRESS_2"]    = base["PATIENT_ADDRESS_2"].apply(lambda v: strip_chars(v, ADDRESS_CHARS, 100))
base["PATIENT_ADDRESS_CITY"] = base["PATIENT_ADDRESS_CITY"].apply(lambda v: strip_chars(v, CITY_CHARS, 30))
base["PATIENT_ADDRESS_STATE"]= base["PATIENT_ADDRESS_STATE"].apply(lambda v: strip_chars(v, STATE_CHARS, 2).upper())
base["PATIENT_ADDRESS_ZIP"]  = base["PATIENT_ADDRESS_ZIP"].apply(lambda v: strip_chars(v, ZIP_CHARS_US, 10))
for ph in ["PATIENT_PHONE_MOBILE","PATIENT_PHONE_HOME","PRACTICE_PHONE_1"]:
    if ph in base.columns:
        base[ph] = base[ph].apply(lambda v: digits_only(v, 10))

# strip illegal chars on insurer names
base["INSURER_1"] = base.get("INSURER_1","").apply(lambda v: strip_chars(v, INSURER_CHARS, 60))
base["INSURER_2"] = base.get("INSURER_2","").apply(lambda v: strip_chars(v, INSURER_CHARS, 60))

# preserve raw policy numbers intact
if "INSURANCE_NUMBER_1" in base.columns:
    base["INSURANCE_NUMBER_1"] = base["INSURANCE_NUMBER_1"].fillna("").astype(str)
else:
    base["INSURANCE_NUMBER_1"] = ""
if "INSURANCE_NUMBER_2" in base.columns:
    base["INSURANCE_NUMBER_2"] = base["INSURANCE_NUMBER_2"].fillna("").astype(str)
else:
    base["INSURANCE_NUMBER_2"] = ""

for p in ("1","2"):
    base[f"ATTRIBUTED_PROVIDER_FIRST_NAME_{p}"] = base[f"ATTRIBUTED_PROVIDER_FIRST_NAME_{p}"].apply(
        lambda v: strip_chars(v, PROVIDER_NAME_CHARS, 50)
    )
    base[f"ATTRIBUTED_PROVIDER_LAST_NAME_{p}"]  = base[f"ATTRIBUTED_PROVIDER_LAST_NAME_{p}"].apply(
        lambda v: strip_chars(v, PROVIDER_NAME_CHARS, 60)
    )
    base[f"ATTRIBUTED_PROVIDER_HONORIFICS_{p}"] = base[f"ATTRIBUTED_PROVIDER_HONORIFICS_{p}"].apply(
        lambda v: strip_chars(v, HONORIFICS_CHARS, 10)
    )
    base[f"ATTRIBUTED_PROVIDER_NPI_{p}"]       = base[f"ATTRIBUTED_PROVIDER_NPI_{p}"].apply(
        lambda v: digits_only(v, 10)
    )

base["PRACTICE_NAME_1"]  = base["PRACTICE_NAME_1"].apply(lambda v: strip_chars(v, PRACTICE_NAME_CHARS, 100))
base["PRACTICE_EMAIL_1"] = base["PRACTICE_EMAIL_1"].apply(lambda v: strip_chars(v, EMAIL_CHARS, 50))

for i in range(1,11):
    base[f"PROGRAM_{i}"] = base[f"PROGRAM_{i}"].apply(lambda v: strip_chars(v, PROGRAM_CHARS, 100))

# ------------------------------------------------------------------
# Drop any record where first or last name contains "test"
# ------------------------------------------------------------------
mask = (
    ~base["PATIENT_FIRST_NAME"].str.contains("test", case=False, na=False)
    & ~base["PATIENT_LAST_NAME"].str.contains("test", case=False, na=False)
)
base = base.loc[mask]

# ------------------------------------------------------------------
# Final column set (exact order)
# ------------------------------------------------------------------
final_cols = [
    "PATIENT_ID","PATIENT_FIRST_NAME","PATIENT_MIDDLE_INITIAL",
    "PATIENT_LAST_NAME","PATIENT_SUFFIX","PATIENT_DOB","PATIENT_GENDER",
    "PATIENT_ADDRESS_1","PATIENT_ADDRESS_2","PATIENT_ADDRESS_CITY",
    "PATIENT_ADDRESS_STATE","PATIENT_ADDRESS_ZIP",
    "PATIENT_PHONE_MOBILE","PATIENT_PHONE_HOME",
    "INSURER_1","INSURANCE_PLAN_1","INSURANCE_NUMBER_1",
    "INSURER_2","INSURANCE_PLAN_2","INSURANCE_NUMBER_2",
    "ATTRIBUTED_PROVIDER_FIRST_NAME_1","ATTRIBUTED_PROVIDER_LAST_NAME_1",
    "ATTRIBUTED_PROVIDER_HONORIFICS_1","ATTRIBUTED_PROVIDER_NPI_1",
    "PRACTICE_NAME_1","PRACTICE_PHONE_1","PRACTICE_EMAIL_1",
    "ATTRIBUTED_PROVIDER_FIRST_NAME_2","ATTRIBUTED_PROVIDER_LAST_NAME_2",
    "ATTRIBUTED_PROVIDER_HONORIFICS_2","ATTRIBUTED_PROVIDER_NPI_2",
] + [f"PROGRAM_{i}" for i in range(1,11)]

for col in final_cols:
    if col not in base.columns:
        base[col] = ""

roster_df = base[final_cols].fillna("")

# ------------------------------------------------------------------
# Persist roster back to pickle
# ------------------------------------------------------------------
with open(data_file, "wb") as f:
    pickle.dump({"roster_df": roster_df}, f)

print(f"[OK] roster built - {len(roster_df):,} rows, {len(roster_df.columns)} columns -> {data_file}")
