1. Data Gathering
•	First, we pull in all relevant information, including each client’s active episodes, last service date, and any notes.
•	This gives us a complete snapshot of who is currently served and when they were last seen.
2. NOMS (National Outcome Measures)
•	Next, we check which clients have a NOMS on file by comparing against separate logs.
•	If one record for a client shows “Yes,” then all records for that client are marked “Yes.”
o	This ensures that if a client is involved in NOMS, it’s reflected consistently across all of their records.
3. Discharge Logic
1.	Standard 90-Day Rule
o	Any episode inactive for 90 days or more is initially flagged as “Recommended for Discharge.”
2.	Multi-Program CCBHC Rule
o	For CCBHC programs only, if the client is not discharge-eligible in any one of those CCBHC programs, then they’re not eligible in all of their CCBHC programs.
o	This way, clients who are still active in at least one CCBHC program aren’t discharged from any of their CCBHC programs prematurely.
3.	Non-CCBHC Programs
o	Non-CCBHC episodes follow the standard 90-day rule with no extra grouping logic.
4. Final Outputs
•	All Clients
o	Contains everyone, from both CCBHC and non-CCBHC programs, along with whether they’re recommended for discharge based on inactivity.
o	For CCBHC clients specifically, the multi-program rule applies.
•	CCBHC Program Client Discharge
o	Focuses only on the CCBHC programs and lists who is truly ready for discharge after applying both the 90-day rule and the multi-program check.
