#!/usr/bin/env python3
# 01.py - Calculate SDOH metrics and raw data for reporting

import os
import sys
import pickle
import pandas as pd
from datetime import datetime

if len(sys.argv) < 2:
    print("Usage: python 01.py <data_file.pkl>")
    sys.exit(1)

data_file = sys.argv[1]

with open(data_file, "rb") as f:
    payload = pickle.load(f)

notes = payload["notes"]
notes_full = payload["notes_full"].copy()
df_hrsn = payload["hrsn"]
df_hrsn_all = payload.get("hrsn_all", df_hrsn)

# ------------------------------------------------------------------
# Merge insurance to notes (use the original mixed‑case column before we normalise and drop it).
# ------------------------------------------------------------------
notes["insurance"] = notes["PATID"].map(
    notes_full.drop_duplicates("PATID").set_index("PATID")["Insurance"]
)

# ------------------------------------------------------------------
# Normalise "No Entry" values
# ------------------------------------------------------------------

def _clean(series: pd.Series, label: str) -> pd.Series:
    series = series.fillna("").str.strip()
    series.loc[series.isin(["", "No Entry"])]= f"No Entry ({label})"
    return series

notes["insurance"] = _clean(notes["insurance"], "Insurance")
notes["ethnicity"] = _clean(notes["ethnicity"], "Ethnicity")
notes["race"] = _clean(notes["race"], "Race")

notes_full["insurance"] = _clean(notes_full["Insurance"], "Insurance")
notes_full["ethnicity"] = _clean(notes_full["ethnicity"], "Ethnicity")
notes_full["race"] = _clean(notes_full["race"], "Race")

if "Insurance" in notes_full.columns:
    notes_full.drop(columns=["Insurance"], inplace=True)

# ------------------------------------------------------------------
# Who is screened?
# ------------------------------------------------------------------
screened = set(df_hrsn["PATID"])
notes["screened"] = notes["PATID"].isin(screened).astype(int)

# ------------------------------------------------------------------
# Build the summary table
# ------------------------------------------------------------------

def _safe_sort(values):
    return sorted(v for v in values if pd.notnull(v))

out = []
groups = {
    "Insurance": _safe_sort(notes["insurance"].unique()),
    "Ethnicity": _safe_sort(notes["ethnicity"].unique()),
    "Race": _safe_sort(notes["race"].unique()),
}

RESERVED = {"Total Unique Clients"}

for group_name, categories in groups.items():
    key = group_name.lower()
    for category in categories:
        if category in RESERVED:
            continue
        sub = notes[notes[key] == category]
        denom = sub["PATID"].nunique()
        num = sub[sub["screened"] == 1]["PATID"].nunique()
        pct = round(num / denom * 100, 2) if denom else 0.0

        out.extend([
            {
                "field": "Numerator (Clients with at least one standardized HRSN screening note)",
                "category": category,
                "value": num,
            },
            {
                "field": "Denominator (Clients with valid CPT and demographics)",
                "category": category,
                "value": denom,
            },
            {
                "field": "% Screened (Numerator ÷ Denominator)",
                "category": category,
                "value": f"{pct}%",
            },
        ])

# ------------------------------------------------------------------
# Add total‑row entries
# ------------------------------------------------------------------
total_denom = notes["PATID"].nunique()
total_num = notes[notes["screened"] == 1]["PATID"].nunique()
total_pct = round(total_num / total_denom * 100, 2) if total_denom else 0.0

out.extend([
    {
        "field": "Numerator (Clients with at least one standardized HRSN screening note)",
        "category": "Total Unique Clients",
        "value": total_num,
    },
    {
        "field": "Denominator (Clients with valid CPT and demographics)",
        "category": "Total Unique Clients",
        "value": total_denom,
    },
    {
        "field": "% Screened (Numerator ÷ Denominator)",
        "category": "Total Unique Clients",
        "value": f"{total_pct}%",
    },
])

summary_df = pd.DataFrame(out)
payload["sdoh_summary_v2"] = summary_df

# ------------------------------------------------------------------
# Raw data sheet
# ------------------------------------------------------------------
notes_full["Age"] = notes_full["date_of_birth"].apply(
    lambda dob: int((datetime.today() - dob).days / 365.25) if pd.notnull(dob) else None
)

notes_full["Screening Complete"] = notes_full["PATID"].isin(screened).map({True: "Yes", False: None})
notes_full["Screening Not Completed"] = notes_full["Screening Complete"].isna().map({True: "Yes", False: None})

rename_map = {
    "PATID": "Client ID",
    "date_of_service": "Date of Service",
    "location_code": "Location Code",
    "Service Provider": "Service Provider",
    "service_charge_code": "Service Code",
    "CPT": "CPT Code",
    "service_program_value": "Program Value",
    "insurance": "Insurance",
    "ethnicity": "Ethnicity",
    "race": "Race",
    "Age": "Age",
    "Staff Completed Assessment": "Staff Completed Assessment",
}

columns_order = [
    "Client ID",
    "Date of Service",
    "Location Code",
    "Service Provider",
    "Service Code",
    "CPT Code",
    "Program Value",
    "Insurance",
    "Ethnicity",
    "Race",
    "Age",
    "Staff Completed Assessment",
    "Screening Complete",
    "Screening Not Completed",
]

if "Staff Completed Assessment" not in notes_full.columns and "Data_Entry_By_Login" in df_hrsn_all.columns:
    # Prioritize final over draft assessments
    hrsn_sorted = df_hrsn_all.sort_values(
        by=["PATID", "Draft_Final_Value"],
        key=lambda col: col.map({"Final": 0, "Draft": 1})  # Final first
    )
    staff_map = hrsn_sorted.drop_duplicates("PATID").set_index("PATID")["Data_Entry_By_Login"]
    notes_full["Staff Completed Assessment"] = notes_full["PATID"].map(staff_map)


df_raw_data = notes_full.rename(columns=rename_map)[columns_order]
payload["sdoh_raw_data"] = df_raw_data

# ------------------------------------------------------------------
# HRSN assessment all records sheet
# ------------------------------------------------------------------
if not df_hrsn_all.empty:
    df_hrsn_all_out = df_hrsn_all[[
        "PATID", "Assess_Date", "Data_Entry_By_Login", "Data_Entry_Date", "Data_Entry_Time", "Draft_Final_Value"
    ]].rename(columns={
        "PATID": "Client ID",
        "Assess_Date": "Assessment Date",
        "Data_Entry_By_Login": "Staff Completed Assessment",
        "Data_Entry_Date": "Entry Date",
        "Data_Entry_Time": "Entry Time",
        "Draft_Final_Value": "Assessment Status"
    })
    payload["sdoh_all_hrsn"] = df_hrsn_all_out

# ------------------------------------------------------------------
# Save back to the pickle file
# ------------------------------------------------------------------
with open(data_file, "wb") as f:
    pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

print("[OK] SDOH metrics and raw data updated.")
