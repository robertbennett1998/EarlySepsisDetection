import pandas as pd
import os

working_directory = os.getcwd()
chart_events_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events.csv")
flattened_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events_flattened.csv")

unique_stays = 0
ce_actual = pd.read_csv(chart_events_csv_path)
unique_stays = (ce_actual["icustay_id"].nunique())
ce_actual = None

ce_flattened = pd.read_csv(flattened_csv_path)
flattened_unique_stays = (ce_flattened["icustay_id"].nunique())
ce_flattened = None
if unique_stays == flattened_unique_stays:
    print("Equal", unique_stays)
else:
    print("Not Equal - Probably doesn't matter", unique_stays, "!=", flattened_unique_stays)

