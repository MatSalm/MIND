#!/usr/bin/env python
# -*- coding: ascii -*-

"""
All_Program_Regulatory_Audit - 00.py
Quarterly random sampling that (a) caps each program at 5 % of its
eligible population (floor, min 1), (b) never exceeds that cap,
(c) never re-audits a previously sampled client in that program.

Updated: 2025-06-04 (Rev D)
"""

import os, time, math, pyodbc, configparser
import pandas as pd, numpy as np
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from dotenv import load_dotenv

# - Sampling window: first 3 months of 4-month look-back, ending 30 days ago -
today = date.today()
look_back = today - relativedelta(months=4)
start_date = look_back
end_date = today - timedelta(days=30)  # exclusive
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')
print("Sampling window (inclusive start, exclusive end):", start_date_str, "to", end_date_str)

# - DB Connection -
load_dotenv()
conn_str = (
    "DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={};".format(
        os.getenv('database_driver_name'),
        os.getenv('database_server'),
        os.getenv('database_port'),
        os.getenv('databaseCWS'),
        os.getenv('database_username'),
        os.getenv('database_password'))
)

conn = None
for attempt in range(1, 5):
    try:
        print(f"Attempt {attempt}/4 connecting to database ...")
        conn = pyodbc.connect(conn_str, timeout=60)
        print("Database connection established.")
        break
    except pyodbc.Error as exc:
        print("Connection attempt failed:", exc)
        if attempt == 4:
            raise
        time.sleep(5)

cur = conn.cursor()

# - Fetch eligible service records from notes + program data -
qry = """
SELECT DISTINCT
    n.PATID,
    n.EPISODE_NUMBER,
    v.program_value,
    d.patient_home_phone
FROM AVCWS.SYSTEM.cw_patient_notes n
JOIN SYSTEM.view_client_episode_history v
    ON n.PATID = v.PATID AND n.EPISODE_NUMBER = v.EPISODE_NUMBER
JOIN SYSTEM.client_curr_demographics d
    ON n.PATID = d.PATID
WHERE n.date_of_service >= ?
  AND n.date_of_service < ?
  AND n.draft_final_code = 'F'
  AND n.service_charge_code IS NOT NULL
  AND LTRIM(RTRIM(n.service_charge_code)) <> ''
  AND NOT (UPPER(v.v_patient_name) LIKE 'TEST%' OR
           UPPER(v.v_patient_name) LIKE 'TEST %' OR
           UPPER(v.v_patient_name) LIKE 'TEST,%' OR
           UPPER(v.v_patient_name) LIKE '% TEST%' OR
           UPPER(v.v_patient_name) LIKE '% TEST,%')
"""
cur.execute(qry, start_date_str, end_date_str)
rows = cur.fetchall()

cols = ['PATID', 'EPISODE_NUMBER', 'program_value', 'patient_home_phone']
df = pd.DataFrame.from_records(rows, columns=cols)
print(f"\nTotal eligible records: {len(df)}")

# - Historical exclusion: Load prior (PATID, program_value) audit pairs -
hist_dir = Path('../hcbs_history')
prev_ids = set()
for f in hist_dir.glob('hcbs_randoms_sample_history_*.txt'):
    df_hist = pd.read_csv(f, header=None, names=['PATID', 'program_value'])
    prev_ids.update(df_hist.apply(lambda x: (str(x['PATID']), x['program_value']), axis=1))

# - Determine per-program quotas and sample -
orig_counts = df['program_value'].value_counts().to_dict()
rng = np.random.default_rng()

def sample_program(group):
    prog = group.name
    quota = max(1, math.floor(orig_counts.get(prog, 0) * 0.05))
    n_draw = min(quota, len(group))
    weights = np.where(group['patient_home_phone'].notnull(), 2, 1)
    return group.sample(n=n_draw, weights=weights, random_state=None)

initial_sample = (df.groupby('program_value', group_keys=False)
                    .apply(sample_program)
                    .reset_index(drop=True))

# - Remove previously sampled (PATID, program) pairs -
final_sample = initial_sample[
    ~initial_sample.apply(lambda x: (str(x['PATID']), x['program_value']) in prev_ids, axis=1)
]

# - Top-up under-sampled programs -
def current_count(prog):
    return (final_sample['program_value'] == prog).sum()

for prog, pop_size in orig_counts.items():
    quota = max(1, math.floor(pop_size * 0.05))
    short = quota - current_count(prog)
    if short <= 0:
        continue

    final_ids = set(final_sample.apply(lambda x: (str(x['PATID']), x['program_value']), axis=1))
    pool = df[(df['program_value'] == prog) & 
              ~df.apply(lambda x: (str(x['PATID']), x['program_value']) in prev_ids or
                                  (str(x['PATID']), x['program_value']) in final_ids,
                        axis=1)]

    if pool.empty:
        continue

    weights = np.where(pool['patient_home_phone'].notnull(), 2, 1)
    topups = pool.sample(n=min(short, len(pool)), weights=weights, random_state=None)
    final_sample = pd.concat([final_sample, topups], ignore_index=True)

# - Output sample -
print("\nFinal sample sizes (by program):")
for prog, cnt in final_sample['program_value'].value_counts().items():
    print(f"  {prog:<35} {cnt}")

timestamp = date.today().strftime('%Y%m%d') + time.strftime('%H%M%S')
hist_dir.mkdir(exist_ok=True)
hist_file = hist_dir / f"hcbs_randoms_sample_history_{timestamp}.txt"
final_sample[['PATID', 'program_value']].to_csv(hist_file, index=False, header=False)
final_sample[['PATID', 'program_value']].to_pickle('temp_data.pkl')
print("Random sample saved to", hist_file)

cur.close()
conn.close()
print("All done.")
