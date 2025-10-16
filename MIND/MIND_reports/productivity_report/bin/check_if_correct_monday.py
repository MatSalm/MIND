import datetime
import subprocess

def main():
    today = datetime.date.today()
    first_day_of_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_month - datetime.timedelta(days=1)
    fifteenth_day_of_month = today.replace(day=15)

    # Find the Monday after the last day of the previous month
    monday_after_last_day_of_last_month = last_day_of_last_month + datetime.timedelta(days=(7 - last_day_of_last_month.weekday()) % 7)
    if monday_after_last_day_of_last_month <= last_day_of_last_month:
        monday_after_last_day_of_last_month += datetime.timedelta(days=7)

    # Find the Monday after the 15th of the current month
    monday_after_fifteenth_day_of_month = fifteenth_day_of_month + datetime.timedelta(days=(7 - fifteenth_day_of_month.weekday()) % 7)
    if monday_after_fifteenth_day_of_month <= fifteenth_day_of_month:
        monday_after_fifteenth_day_of_month += datetime.timedelta(days=7)

    # Debug statements to print the calculated dates
    print(f"Today: {today}")
    print(f"Monday after the last day of the last month: {monday_after_last_day_of_last_month}")
    print(f"Monday after the 15th of the current month: {monday_after_fifteenth_day_of_month}")

    if today == monday_after_last_day_of_last_month:
        print("Today is the Monday after the last day of the month.")
        # Execute another script
        subprocess.run(["C:\\MIND\\MIND_reports\\productivity_report\\bin\\run_productivity_report.cmd"])
    elif today == monday_after_fifteenth_day_of_month:
        print("Today is the Monday after the 15th of the month.")
        # Execute another script
        subprocess.run(["C:\\MIND\\MIND_reports\\productivity_report\\bin\\run_productivity_report.cmd"])
    else:
        print("Today is not the Monday after the EOM or the 15th.")

if __name__ == "__main__":
    main()
