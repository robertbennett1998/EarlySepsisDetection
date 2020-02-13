import pandas as pd
import os
import data_extraction as de
from tqdm import tqdm
import time

class Checkpoint:
    def __init__(self, name):
        self._name = name
        self._start_time = time.time()

    def name(self):
        return self._name

    def elapsed(self):
        return (self._stop_time - self._start_time)

    def stop(self):
        self._stop_time = time.time()

class Profiler:
    def __init__(self):
        self._checkpoints = list()

    def add_checkpoint(self, c):
        self._checkpoints.append(c)

    def print(self):
        for checkpoint in self._checkpoints:
            print(checkpoint.name(), checkpoint.elapsed())

        print("----SUMMARY----")

        overview = dict()
        for checkpoint in self._checkpoints:
            overview[checkpoint.name()] = 0

        for checkpoint in self._checkpoints:
            overview[checkpoint.name()] = overview[checkpoint.name()] + checkpoint.elapsed()
            #overview[checkpoint.name()][1] += checkpoint.elapsed()

        for item in overview.items():
            print(item[0], item[1])

chart_event_hr_data_source_1 = 211
chart_event_hr_data_source_2 = 220045
chart_event_body_temp_data_source_1 = 676
chart_event_body_temp_data_source_2 = 223762
chart_event_respiratory_rate_data_source_1 = 618
chart_event_respiratory_rate_data_source_2 = 220210
chart_event_arterial_paco2_data_source_1 = 778
chart_event_arterial_paco2_data_source_2 = 227038

def get_column_name(id):
    if id == chart_event_hr_data_source_1:
        return "heart_rate"
    elif id == chart_event_hr_data_source_2:
        return "heart_rate"
    elif id == chart_event_body_temp_data_source_1:
        return "body_temp"
    elif id == chart_event_body_temp_data_source_2:
        return "body_temp"
    elif id == chart_event_respiratory_rate_data_source_1:
        return "respiratory_rate"
    elif id == chart_event_respiratory_rate_data_source_2:
        return "respiratory_rate"
    elif id == chart_event_arterial_paco2_data_source_1:
        return "paco2"
    elif id == chart_event_arterial_paco2_data_source_1:
        return "paco2"

    return "unknown"

working_directory = os.getcwd()
lab_events_csv_path = os.path.join(working_directory, "sepsis_patients_lab_events.csv")
chart_events_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events.csv")

if not os.path.exists(lab_events_csv_path):
    de.ExtractSepsisLabEvents(lab_events_csv_path)

if not os.path.exists(chart_events_csv_path):
    de.ExtractSepsisChartEvents(chart_events_csv_path)

# le_distinct_subject_entries = list() # 3d tuple with id, start pos, end pos

# le_icu_stay_data = pd.read_csv(lab_events_csv_path, parse_dates=["charttime"]).groupby(["subject_id", "icustay_id"])
# for icustay_id, data in le_icu_stay_data:
#     dt_values = list()
#     prev_datetime = None
#     for datetime in data["charttime"]:
#         if prev_datetime is None:
#             dt_values.append(datetime - datetime)
#         else:
#             dt_values.append(datetime - prev_datetime)
        
#         prev_datetime = datetime

#     data.insert(len(data.columns), "delta_time", dt_values)
#     print(data)
#     break

ce_icu_stay_data = pd.read_csv(chart_events_csv_path, parse_dates=["charttime"], dtype={'icustay_id': int, 'subject_id': int, 'itemid': int }).groupby(["subject_id", "icustay_id"])
flattened_ce_data = pd.DataFrame()

p = Profiler()

i = 0

main_loop_checkpoint = Checkpoint("main_loop_checkpoint")
p.add_checkpoint(main_loop_checkpoint)
for icustay_id, data in ce_icu_stay_data:
    single_stay_data = None

    single_stay_loop = Checkpoint("single_stay_loop")
    p.add_checkpoint(single_stay_loop)
    group = Checkpoint("group")
    p.add_checkpoint(group)
    for chartevent_id, chartevent_data in data.groupby("itemid"):
        group.stop()
        single_event_data = pd.DataFrame()

        #append the time from t0 in the 
        dt_values = list()
        for datetime in chartevent_data["charttime"]:
            dt_values.append((datetime - chartevent_data["charttime"].iloc[0]).floor("H").total_seconds() / (60**2)) #hours from first reading... #TODO: Should look at getting the admission time
            
            prev_datetime = datetime

        chartevent_data.insert(len(data.columns), "hours_from_first_reading", dt_values)

        # for id, stay, t, dt in zip(chartevent_data["subject_id"], chartevent_data["icustay_id"], chartevent_data["charttime"], chartevent_data["hours_from_first_reading"]):
        #     print(id, "-", stay, "-", t, "-", dt)

        mins = list()
        maxs = list()
        hours = list()
        means = list()
        subject_id = list()
        icustay_id = list()

        value_calculation_loop = Checkpoint("value_calculation_loop")
        p.add_checkpoint(value_calculation_loop)
        for hour, hour_values in chartevent_data.groupby("hours_from_first_reading"):
            # print(hour_values["value"])
            # print(hour_values["value"].mean())
            hours.append(hour)
            mins.append(hour_values["value"].min())
            maxs.append(hour_values["value"].max())
            means.append(hour_values["value"].mean())
            subject_id.append(hour_values["subject_id"].values[0])
            icustay_id.append(hour_values["icustay_id"].values[0])
        value_calculation_loop.stop()

        insert_rows = Checkpoint("insert_rows")
        p.add_checkpoint(insert_rows)
        column_name = get_column_name(chartevent_id)
        single_event_data.insert(0, "subject_id", subject_id)
        single_event_data.insert(1, "icustay_id", icustay_id)
        single_event_data.insert(2, "hour_from_zero", hours)
        single_event_data.insert(3, column_name + "_min", mins)
        single_event_data.insert(4, column_name + "_mean", means)
        single_event_data.insert(5, column_name + "_max", maxs)
        insert_rows.stop()

        merge = Checkpoint("merge")
        p.add_checkpoint(merge)
        if single_stay_data is None:
            single_stay_data = single_event_data.copy()
        else:
            single_stay_data = single_stay_data.merge(single_event_data, how="outer", on=["subject_id", "icustay_id", "hour_from_zero"])
        merge.stop()

    flattened_ce_data = pd.concat([flattened_ce_data, single_stay_data])
    single_stay_loop.stop()
    i += 1
    if i >= 100:
        break

    

main_loop_checkpoint.stop()

p.print()

flattened_ce_data.to_csv(os.path.join(os.getcwd(), "flattened.csv"))
print(flattened_ce_data)