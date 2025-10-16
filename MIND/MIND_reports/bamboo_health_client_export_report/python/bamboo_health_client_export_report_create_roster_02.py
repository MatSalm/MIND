#!/usr/bin/env python3
"""
bamboo_health_client_export_report_create_roster_02.py
-----------------------------------------------------
Export the cleaned patient dataframe to a roster CSV, but only after
validating every field against the strict format requirements.
Any record failing validation is skipped.

Usage
-----
    python 02.py <data_file> [<param_file>]

Arguments
---------
    data_file   Pickle written by 01.py; must contain key "roster_df".
    param_file  (optional) JSON with {"file_prefix": "..."}  ← accepted but ignored.

Output filename
---------------
    Submission_PatientPing_PatientRoster_YYYYMMDD.csv
    (saved in the same directory as this script)
"""

import os
import sys
import pickle
import re
import pandas as pd
from datetime import datetime
import csv

# ------------------------------------------------------------------
# Check argv length
# ------------------------------------------------------------------
if len(sys.argv) not in (2, 3):
    print("Usage: python 02.py <data_file> [<param_file>]")
    sys.exit(1)

data_file  = sys.argv[1]
# param_file = sys.argv[2] if len(sys.argv) == 3 else None  # ignored

# ------------------------------------------------------------------
# Load roster_df from pickle
# ------------------------------------------------------------------
try:
    with open(data_file, "rb") as f:
        data = pickle.load(f)
    df = data["roster_df"].astype(str).fillna("")
except (FileNotFoundError, KeyError) as err:
    print(f"Error: {err}  – did 01.py finish successfully?")
    sys.exit(1)

# ------------------------------------------------------------------
# Build output filename
# ------------------------------------------------------------------
date_str = datetime.today().strftime("%Y%m%d")
file_name = f"Submission_PatientPing_PatientRoster_{date_str}.csv"
out_path = os.path.join(os.path.dirname(__file__), file_name)

# ------------------------------------------------------------------
# Prepare validators
# ------------------------------------------------------------------
# 2-letter US states + DC + common provinces
VALID_STATES = {
    *["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
      "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
      "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
      "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
      "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"],
    *["AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"]
}

# Patterns
_pat = lambda chars, length: re.compile(rf"^[{chars}]{{1,{length}}}$")
_PATIENT_ID        = _pat(r"A-Za-z0-9\-_\/\.\| {", 50)
_FIRST_NAME        = _pat(r"A-Za-z'\- \.\,", 50)
_MIDDLE_INIT       = _pat(r"A-Za-z", 5)
_LAST_NAME         = _pat(r"A-Za-z0-9'\- \.\,", 50)
_SUFFIX            = _pat(r"A-Za-z\.", 5)
_DOB               = re.compile(r"""^[0-9\/\-]{1,10}$""")
_GENDER_ALLOWED    = {"M","F","U","O","X","MALE","FEMALE","UNKNOWN","OTHER","NON-BINARY"}
_ADDRESS           = _pat(r"A-Za-z0-9\-\#\\&'.,:\/ ", 100)
_CITY              = _pat(r"A-Za-z0-9\-\./' ", 30)
_STATE             = re.compile(r"^[A-Za-z]{2}$")
_ZIP_US            = re.compile(r"^\d{5}(-\d{4})?$")
_ZIP_CA            = re.compile(r"^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$")
_PHONE             = re.compile(r"^\d{10}$")
_INSURER           = _pat(r"A-Za-z0-9&,\-\/: ", 60)
_PLAN              = _pat(r"A-Za-z0-9&,\-\/: ", 15)
_POLICY_NUM        = _pat(r"A-Za-z0-9\-\_\.\|\{ ", 75)
_PROV_NAME         = _pat(r"A-Za-z'\.\,&\(\)\-", 50)
_PROV_NAME_L       = _pat(r"A-Za-z'\.\,&\(\)\-", 60)
_HONORIFICS        = _pat(r"A-Za-z\-\ ", 10)
_EMAIL             = _pat(r"A-Za-z0-9\-\_\@\%\+\. ", 50)
_PROGRAM           = _pat(r"A-Za-z0-9,\.\&'#/+\(\);\:\_\-\[\]\$\| ", 100)
_SSN4              = re.compile(r"^[0-9]{4}$")
_SSN9              = re.compile(
    r"^(?!219099999|078051120)(?!666|000|9\d{2})\d{3}(?!00)\d{2}(?!0{4})\d{4}$"
)

def valid_row(r):
    # required
    if not _PATIENT_ID.match(r.PATIENT_ID):              return False
    if not _FIRST_NAME.match(r.PATIENT_FIRST_NAME):      return False
    if r.PATIENT_MIDDLE_INITIAL and not _MIDDLE_INIT.match(r.PATIENT_MIDDLE_INITIAL): return False
    if not _LAST_NAME.match(r.PATIENT_LAST_NAME):        return False
    if r.PATIENT_SUFFIX and not _SUFFIX.match(r.PATIENT_SUFFIX): return False
    if not _DOB.match(r.PATIENT_DOB):                    return False
    try:
        dt = datetime.strptime(r.PATIENT_DOB, "%Y-%m-%d")
        if dt > datetime.today(): return False
    except:
        return False
    if r.PATIENT_GENDER.upper() not in _GENDER_ALLOWED:  return False

    # optional SSN
    ssn = r.get("PATIENT_SSN","")
    if ssn:
        ssn_plain = ssn.replace("-", "")
        if not (_SSN4.match(ssn_plain) or _SSN9.match(ssn_plain)): return False

    # address
    for fld, pat in [
        ("PATIENT_ADDRESS_1", _ADDRESS),
        ("PATIENT_ADDRESS_2", _ADDRESS),
        ("PATIENT_ADDRESS_CITY", _CITY),
    ]:
        v = r.get(fld,"")
        if v and not pat.match(v): return False
        # void any containing forbidden words:
        if any(k in v.lower() for k in ("bad","needs","no address")): return False

    st = r.get("PATIENT_ADDRESS_STATE","")
    if st and (not _STATE.match(st) or st.upper() not in VALID_STATES): return False

    zp = r.get("PATIENT_ADDRESS_ZIP","")
    if zp:
        if st in VALID_STATES - set(["AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"]):
            if not _ZIP_US.match(zp): return False
        else:
            if not _ZIP_CA.match(zp): return False

    for ph in ("PATIENT_PHONE_MOBILE","PATIENT_PHONE_HOME"):
        v = r.get(ph,"")
        if v and not _PHONE.match(re.sub(r"\D","",v)): return False

    # insurer / plan / number
    if r.INSURER_1 and not _INSURER.match(r.INSURER_1): return False
    if r.INSURANCE_PLAN_1 and not _PLAN.match(r.INSURANCE_PLAN_1): return False
    if r.INSURANCE_NUMBER_1 and not _POLICY_NUM.match(r.INSURANCE_NUMBER_1): return False
    if r.INSURER_2 and not _INSURER.match(r.INSURER_2): return False
    if r.INSURANCE_PLAN_2 and not _PLAN.match(r.INSURANCE_PLAN_2): return False
    if r.INSURANCE_NUMBER_2 and not _POLICY_NUM.match(r.INSURANCE_NUMBER_2): return False

    # providers 1 & 2
    for p in ("1","2"):
        fn = r.get(f"ATTRIBUTED_PROVIDER_FIRST_NAME_{p}","")
        ln = r.get(f"ATTRIBUTED_PROVIDER_LAST_NAME_{p}","")
        ho = r.get(f"ATTRIBUTED_PROVIDER_HONORIFICS_{p}","")
        ni = r.get(f"ATTRIBUTED_PROVIDER_NPI_{p}","")
        if fn and not _PROV_NAME.match(fn): return False
        if ln and not _PROV_NAME_L.match(ln): return False
        if ho and not _HONORIFICS.match(ho): return False
        if ni and not re.fullmatch(r"\d{10}", ni): return False

    # practice
    pn = r.get("PRACTICE_NAME_1","")
    if pn and not _pat(r"A-Za-z0-9\-\#\/\&\(\);\:\_\+ ",100).match(pn): return False
    pe = r.get("PRACTICE_PHONE_1","")
    if pe and not _PHONE.match(re.sub(r"\D","",pe)): return False
    em = r.get("PRACTICE_EMAIL_1","")
    if em and not _EMAIL.match(em): return False

    # programs
    for i in range(1,11):
        pr = r.get(f"PROGRAM_{i}","")
        if pr and not _PROGRAM.match(pr): return False

    return True

# ------------------------------------------------------------------
# Apply validation and drop bad records
# ------------------------------------------------------------------
valid_mask = df.apply(valid_row, axis=1)
num_bad = len(df) - valid_mask.sum()
if num_bad:
    print(f"Skipped {num_bad:,} invalid record(s) out of {len(df):,}")

df_valid = df[valid_mask]

# ------------------------------------------------------------------
# Write CSV (UTF-8, minimal quoting, no index)
# ------------------------------------------------------------------
df_valid.to_csv(
    out_path,
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_MINIMAL
)

print(f"02.py complete: {len(df_valid):,} records -> {out_path}")

