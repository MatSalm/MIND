#!/usr/bin/env python3
# 02.py – Output SDOH summary to Excel (I-SERV style layout with grouped headers, color, and raw data sheet)

import os
import sys
import pickle
from datetime import datetime
import pandas as pd

if len(sys.argv) < 2:
    print("Usage: python 02.py <data_file.pkl>")
    sys.exit(1)

data_file = sys.argv[1]
script_dir = os.path.dirname(os.path.abspath(__file__))

# ──────────────────────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────────────────────
with open(data_file, "rb") as f:
    payload = pickle.load(f)

if "sdoh_summary_v2" not in payload:
    raise ValueError("Pickle file missing 'sdoh_summary_v2' key")

summary       = payload["sdoh_summary_v2"].copy()
notes_df      = payload.get("notes", pd.DataFrame())
df_raw        = payload.get("sdoh_raw_data", pd.DataFrame())
df_hrsn_all   = payload.get("sdoh_all_hrsn", pd.DataFrame())
cpt_code_list = payload.get("sdoh_cpt_codes", [])
windows       = payload.get("windows", {})

# ──────────────────────────────────────────────────────────────────────────
# Context labels
# ──────────────────────────────────────────────────────────────────────────
try:
    measure_year = int(pd.to_datetime(windows.get("MY_START")).year)
except Exception:
    measure_year = datetime.today().year

run_dt    = datetime.today()
run_label = run_dt.strftime("%Y-%m-%d")
run_stamp = run_dt.strftime("%Y%m%d")

output_file = os.path.join(
    script_dir,
    f"SDOH_Summary_Report_MY{measure_year}_{run_stamp}.xlsx",
)

# ──────────────────────────────────────────────────────────────────────────
# Totals for header table
# ──────────────────────────────────────────────────────────────────────────

total_unique_clients = notes_df["PATID"].nunique()
screened_total       = notes_df.get("screened", pd.Series(dtype=int)).sum()
pct                  = (
    f"{round(screened_total / total_unique_clients * 100, 2)}%"
    if total_unique_clients else "0.0%"
)

# ──────────────────────────────────────────────────────────────────────────
# Field / category definitions
# ──────────────────────────────────────────────────────────────────────────
fields = [
    "Numerator (Clients with at least one standardized HRSN screening note)",
    "Denominator (Clients with valid CPT and demographics)",
    "% Screened (Numerator ÷ Denominator)",
]

race_vals = [
    "American Indian or Alaska Native",
    "Asian",
    "Black or African American",
    "Multiracial",
    "White",
    "Chose Not to Disclose",
    "No Entry (Race)",
]
ethnicity_vals = [
    "Hispanic or Latino",
    "Non Hispanic or Latino",
    "Refused to Report Ethnicity",
    "No Entry (Ethnicity)",
]
insurance_vals     = ["Medicaid", "Other Insurance", "No Entry (Insurance)"]
ordered_categories = insurance_vals + ethnicity_vals + race_vals
col_sections       = {
    "Insurance": insurance_vals,
    "Ethnicity": ethnicity_vals,
    "Race": race_vals,
}

# ──────────────────────────────────────────────────────────────────────────
# Build summary DataFrame
# ──────────────────────────────────────────────────────────────────────────
header = ["", "Total Unique Clients", *ordered_categories]
rows   = [header]

for field in fields:
    row = [field]

    if "Numerator" in field:
        row.append(screened_total)
    elif "Denominator" in field:
        row.append(total_unique_clients)
    elif "% Screened" in field:
        row.append(pct)
    else:
        row.append("")

    for cat in ordered_categories:
        val  = summary.loc[(summary["field"] == field) & (summary["category"] == cat)]
        cell = val["value"].values[0] if not val.empty else ""
        row.append(cell)

    rows.append(row)

df_out = pd.DataFrame(rows[1:], columns=rows[0])

# ──────────────────────────────────────────────────────────────────────────
# Excel output
# ──────────────────────────────────────────────────────────────────────────
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    df_out.to_excel(writer, index=False, sheet_name="SDOH Summary", startrow=8)
    worksheet = writer.sheets["SDOH Summary"]
    workbook  = writer.book

    # Header lines
    worksheet.write(0, 0, "SDOH Screening Summary")
    worksheet.write(1, 0, f"Measure Year: {measure_year}")
    worksheet.write(2, 0, f"Run Date: {run_label}")
    worksheet.write(3, 0, "Numerator: Clients with at least one standardized HRSN screening")
    worksheet.write(4, 0, "Denominator: Clients with at least one eligible service and valid demographics")
    worksheet.write(5, 0, "Counts below reflect stratification by Insurance Type, Ethnicity, and Race")
    worksheet.write(6, 0, "Report includes only clients aged 18 and older")

    # Group header formats
    fmt_insurance = workbook.add_format({"bold": True, "align": "center", "bg_color": "#F4B183", "border": 1})
    fmt_ethnicity = workbook.add_format({"bold": True, "align": "center", "bg_color": "#9DC3E6", "border": 1})
    fmt_race      = workbook.add_format({"bold": True, "align": "center", "bg_color": "#A9D18E", "border": 1})

    # Group header row (row-index 7)
    worksheet.write(7, 0, "")  # top-left cell
    col_idx = 1
    for section_name, categories in [("Total", ["Total Unique Clients"])] + list(col_sections.items()):
        start = col_idx
        col_idx += len(categories)
        fmt = (
            fmt_insurance if section_name == "Insurance" else
            fmt_ethnicity if section_name == "Ethnicity" else
            fmt_race if section_name == "Race" else
            workbook.add_format({"bold": True, "align": "center"})
        )
        if col_idx - 1 > start:
            worksheet.merge_range(7, start, 7, col_idx - 1, section_name, fmt)
        else:
            worksheet.write(7, start, section_name, fmt)

    # ------------------ auto-fit (summary sheet) -------------------------
    for i in range(len(df_out.columns)):
        col_data = df_out.iloc[:, i].astype(str)
        max_len1 = col_data.str.len().max()
        max_len1 = 0 if pd.isna(max_len1) else int(max_len1)
        max_len  = max(max_len1, len(str(df_out.columns[i]))) + 2
        worksheet.set_column(i, i, max_len)

    # ------------------ raw data sheet -----------------------------------
    if not df_raw.empty:
        df_raw.to_excel(writer, index=False, sheet_name="SDOH Raw Data")
        raw_ws = writer.sheets["SDOH Raw Data"]
        for i in range(len(df_raw.columns)):
            col_data = df_raw.iloc[:, i].astype(str)
            max_len1 = col_data.str.len().max()
            max_len1 = 0 if pd.isna(max_len1) else int(max_len1)
            max_len  = max(max_len1, len(str(df_raw.columns[i]))) + 2
            raw_ws.set_column(i, i, max_len)

    # ------------------ all HRSN assessments -----------------------------
    if not df_hrsn_all.empty:
        df_hrsn_all.to_excel(writer, index=False, sheet_name="All HRSN Assessments")
        hrsn_ws = writer.sheets["All HRSN Assessments"]
        for i in range(len(df_hrsn_all.columns)):
            col_data = df_hrsn_all.iloc[:, i].astype(str)
            max_len1 = col_data.str.len().max()
            max_len1 = 0 if pd.isna(max_len1) else int(max_len1)
            max_len  = max(max_len1, len(str(df_hrsn_all.columns[i]))) + 2
            hrsn_ws.set_column(i, i, max_len)

    # ------------------ CPT codes list sheet -----------------------------
    if cpt_code_list:
        df_codes = pd.DataFrame({"Eligible CPT Codes": cpt_code_list})
        df_codes.to_excel(writer, index=False, sheet_name="Eligible CPT Codes")
        codes_ws = writer.sheets["Eligible CPT Codes"]
        max_len = max(df_codes["Eligible CPT Codes"].astype(str).map(len).max(), len("Eligible CPT Codes")) + 2
        codes_ws.set_column(0, 0, max_len)

print(f"[OK] Excel saved to {output_file}")
