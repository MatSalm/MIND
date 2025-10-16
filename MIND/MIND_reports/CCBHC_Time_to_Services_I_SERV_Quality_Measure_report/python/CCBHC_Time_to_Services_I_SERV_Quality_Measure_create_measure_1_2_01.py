#!/usr/bin/env python3
# 01.py – Generate I-SERV-1 (time to first assessment) and I-SERV-2 (time to first service)
#            summary pivot tables + raw data exports
#
#  KEY RULES (UPDATED 2025-06-04)
#  -----------------------------
#  • Only appointments within the current measurement year (MY) can qualify a client as NEW.
#  • A "NEW" client is one whose MY appointment follows >=180 days (≈6 months) since their last appointment (even if that prior appt was in the prior year).
#  • Clients are excluded from NEW status if they had a billable service in the 180 days prior to their index appointment.

import os, sys, json, pickle
import numpy as np
import pandas as pd

if len(sys.argv) < 3:
    print("Usage: python 01.py <data_file.pkl> <param_file.json>")
    sys.exit(1)

data_file, param_file = sys.argv[1], sys.argv[2]

with open(data_file, "rb") as f:
    payload = pickle.load(f)

with open(param_file, "r", encoding="utf-8") as f:
    params = json.load(f)

measure_year = int(params.get("measure_year", ""))
WIN = {k: pd.to_datetime(v) for k, v in payload["windows"].items()}

# Load data
df_appt   = payload["appointments"].copy()
df_notes  = payload["notes"].copy()
df_assess = payload["assessments"].copy()
df_demo   = payload["demographics"].copy()
df_cov    = payload.get("coverage")

# Coerce date fields
df_appt["appointment_date"] = pd.to_datetime(df_appt["appointment_date"])
df_appt["orig_entry_date"] = pd.to_datetime(df_appt["orig_entry_date"], errors="coerce")
df_notes["date_of_service"] = pd.to_datetime(df_notes["date_of_service"], errors="coerce")
df_assess["Assess_Date"] = pd.to_datetime(df_assess["Assess_Date"], errors="coerce")
df_demo["dob"] = pd.to_datetime(df_demo["dob"], errors="coerce")

# Sort appts and build rolling gap calc
df_appt = df_appt.sort_values(["PATID", "appointment_date"])
df_appt["prev_appt"] = df_appt.groupby("PATID")["appointment_date"].shift(1)
df_appt["days_since_prev"] = (df_appt["appointment_date"] - df_appt["prev_appt"]).dt.days

# Get ALL appts with 180+ day gap or no previous appt
candidates = df_appt[(df_appt["prev_appt"].isna()) | (df_appt["days_since_prev"] >= 180)]

# Limit to those whose qualifying appt is in the measure year
index_appts = candidates[candidates["appointment_date"].dt.year == measure_year].copy()

# Keep earliest orig_entry_date per client
index_appts = index_appts.sort_values("orig_entry_date").groupby("PATID").first().reset_index()

# Remove clients with service within 180 days BEFORE their index appt
recent_notes = df_notes.merge(index_appts[["PATID", "appointment_date"]], on="PATID")
recent_notes = recent_notes[recent_notes["date_of_service"] < recent_notes["appointment_date"]]
recent_notes["days_before"] = (recent_notes["appointment_date"] - recent_notes["date_of_service"]).dt.days
bad_patids = recent_notes[recent_notes["days_before"] < 180]["PATID"].unique()
df_index = index_appts[~index_appts["PATID"].isin(bad_patids)].copy()

# Merge demographics
base = df_index.merge(df_demo, on="PATID", how="left")

# First service/assessment AFTER orig_entry_date
svc = base.merge(df_notes, on="PATID", how="left")
svc = svc[svc["date_of_service"] > svc["orig_entry_date"]]

# Keep full row of first service (including service_charge_code)
svc = svc.sort_values("date_of_service")
svc_first = svc.groupby("PATID").first().reset_index()
svc_min = svc_first[["PATID", "date_of_service", "service_charge_code"]].rename(
    columns={"date_of_service": "first_service"}
)

ass = base.merge(df_assess, on="PATID", how="left")
ass = ass[ass["Assess_Date"] > ass["orig_entry_date"]]
ass_min = ass.groupby("PATID")["Assess_Date"].min().reset_index(name="first_assess")

serv_pop = base.merge(svc_min, on="PATID", how="left").dropna(subset=["first_service"])
ass_pop  = base.merge(ass_min, on="PATID", how="left").dropna(subset=["first_assess"])

# Business day calc
def busdays(start, end):
    ok = start.notna() & end.notna()
    out = pd.Series(np.nan, index=start.index)
    out[ok] = np.busday_count(start[ok].values.astype("datetime64[D]"), end[ok].values.astype("datetime64[D]"))
    return out

serv_pop["bdays_to_service"] = busdays(serv_pop["orig_entry_date"], serv_pop["first_service"])
ass_pop["bdays_to_assess"] = busdays(ass_pop["orig_entry_date"], ass_pop["first_assess"])

serv_pop["SERV2_within_10"] = serv_pop["bdays_to_service"] <= 10
ass_pop["SERV1_within_10"] = ass_pop["bdays_to_assess"] <= 10

# Insurance logic
if df_cov is not None:
    df_cov["eff"] = pd.to_datetime(df_cov["eff"], errors="coerce")
    df_cov["exp"] = pd.to_datetime(df_cov["exp"], errors="coerce")
    cov_valid = df_cov[df_cov["financial_class_value"].notna()].dropna(subset=["eff"])

    def ins_type(patid, ref):
        if pd.isna(ref): return "No Entry (Insurance)"
        covs = cov_valid[cov_valid["PATID"] == patid]
        covs = covs[(covs["eff"] <= ref) & ((covs["exp"].isna()) | (covs["exp"] >= ref))]
        if covs.empty: return "No Entry (Insurance)"
        return "Medicaid" if (covs["financial_class_value"] == "Medicaid").any() else "Other"

    serv_pop["Insurance Type"] = serv_pop.apply(lambda r: ins_type(r.PATID, r.first_service), axis=1)
    ass_pop["Insurance Type"] = ass_pop.apply(lambda r: ins_type(r.PATID, r.first_assess), axis=1)

# Age bands
for df in (serv_pop, ass_pop):
    df["age"] = (WIN["MY_END"] - df["dob"]).dt.days // 365
    df["age_band"] = df["age"].apply(lambda x: "12-17 yo" if 12 <= x <= 17 else ("18+ yo" if x >= 18 else "<12"))

serv_pop = serv_pop[serv_pop["age_band"].isin(["12-17 yo", "18+ yo"])]
ass_pop = ass_pop[ass_pop["age_band"].isin(["12-17 yo", "18+ yo"])]

# Pivot builder
def pivot(df_source, measure, day_col, flag_col):
    out = {}
    for age in ["12-17 yo", "18+ yo"]:
        df_age = df_source[df_source["age_band"] == age].copy()
        if df_age.empty:
            out[age] = pd.DataFrame()
            continue
        dims = [
            ("race", df_age["race"].fillna("No Entry (Race)").replace("No Entry", "No Entry (Race)").unique()),
            ("ethnicity", df_age["ethnicity"].fillna("No Entry (Ethnicity)").replace("No Entry", "No Entry (Ethnicity)").unique()),
            ("sex", df_age["sex"].fillna("No Entry (Sex)").replace("No Entry", "No Entry (Sex)").unique()),
            ("Insurance Type", df_age["Insurance Type"].fillna("No Entry (Insurance)").unique())
        ]
        rows = [
            "Numerator (Sum of Business Days)",
            "Denominator (Client Count)",
            "Average Business Days to Be Seen",
            "% Within 10 Business Days"
        ]
        cols = ["Total Unique Clients"] + [v for _, vs in dims for v in sorted(vs)]
        tbl = pd.DataFrame(index=rows, columns=cols).astype(float).fillna(0.0)

        tbl.loc["Numerator (Sum of Business Days)", "Total Unique Clients"] = df_age[day_col].sum()
        tbl.loc["Denominator (Client Count)", "Total Unique Clients"] = len(df_age)
        tbl.loc["Average Business Days to Be Seen", "Total Unique Clients"] = df_age[day_col].mean()
        tbl.loc["% Within 10 Business Days", "Total Unique Clients"] = df_age[flag_col].mean() * 100

        for dim, vs in dims:
            df_age[dim] = df_age[dim].fillna(f"No Entry ({dim.title()})").replace("No Entry", f"No Entry ({dim.title()})")
            for v in vs:
                sub = df_age[df_age[dim] == v]
                if sub.empty:
                    continue
                tbl.loc["Numerator (Sum of Business Days)", v] = sub[day_col].sum()
                tbl.loc["Denominator (Client Count)", v] = len(sub)
                tbl.loc["Average Business Days to Be Seen", v] = sub[day_col].mean()
                tbl.loc["% Within 10 Business Days", v] = sub[flag_col].mean() * 100

        out[age] = tbl.round(2).fillna(0)
    return out

pivot_results = {
    "I-SERV-1": pivot(ass_pop, "I-SERV-1", "bdays_to_assess", "SERV1_within_10"),
    "I-SERV-2": pivot(serv_pop, "I-SERV-2", "bdays_to_service", "SERV2_within_10")
}

# Raw export prep
serv_pop.loc[:, "Existing or New Client"] = "New"
ass_pop = ass_pop.copy()
ass_pop["Existing or New Client"] = "New"

raw_cols = [
    "PATID", "age", "Existing or New Client", "appointment_date", "orig_entry_date", "staff_name",
    "race", "ethnicity", "Insurance Type"
]

raw_service = serv_pop[raw_cols + ["service_charge_code", "first_service", "bdays_to_service"]].rename(columns={
    "PATID": "Client ID", "age": "Age", "appointment_date": "Appointment Date",
    "orig_entry_date": "Appointment Created Date", "staff_name": "Appointment Providers",
    "first_service": "First Service Date", "bdays_to_service": "Business days to first service",
    "race": "Race", "ethnicity": "Ethnicity", "Insurance Type": "Insurance",
    "service_charge_code": "Service Code"
})

raw_assess = ass_pop[raw_cols + ["first_assess", "bdays_to_assess"]].rename(columns={
    "PATID": "Client ID", "age": "Age", "appointment_date": "Appointment Date",
    "orig_entry_date": "Appointment Created Date", "staff_name": "Appointment Providers",
    "first_assess": "First Assessment Date", "bdays_to_assess": "Business days to first assessment",
    "race": "Race", "ethnicity": "Ethnicity", "Insurance Type": "Insurance"
})

for df in [raw_service, raw_assess]:
    for col in ["Appointment Date", "Appointment Created Date", df.columns[-2]]:
        df[col] = pd.to_datetime(df[col]).dt.date

payload.update({
    "results": pivot_results,
    "measure_year": measure_year,
    "raw_eval": raw_assess,
    "raw_service": raw_service
})

with open(data_file, "wb") as f:
    pickle.dump(payload, f)

print(f"[OK] I-SERV pivots + raw exports (MY {measure_year}) saved to {data_file}")
