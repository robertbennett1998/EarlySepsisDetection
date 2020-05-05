import os
import shutil

import pandas as pd
import numpy as np

working_directory = os.getcwd()
chart_events_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events.csv")
lab_events_csv_path = os.path.join(working_directory, "sepsis_patients_lab_events.csv")
sepsis_patients_chart_events_flattened_path = os.path.join(working_directory, "sepsis_patients_chart_events_flattened.csv")
sepsis_patients_lab_events_flattened_path = os.path.join(working_directory, "sepsis_patients_lab_events_flattened.csv")
sepsis_patient_flattened_data_csv_path = os.path.join(working_directory, "sepsis_patient_flattened_data.csv")
sepsis_patients_with_sirs_cond_path = os.path.join(working_directory, "sepsis_patients_with_sirs_cond.csv")
sepsis_patients_with_no_sirs_cond_path = os.path.join(working_directory, "sepsis_patients_with_no_sirs_cond.csv")
old_sepsis_patients_csv_path = os.path.join(working_directory, "sepsis_patients.csv")
sepsis_patients_csv_path = os.path.join(working_directory, "../../data/early_sepsis/sepsis_patients.csv")
delete_intimidiate_files = False

if delete_intimidiate_files:
    if os.path.exists(chart_events_csv_path):
        os.remove(chart_events_csv_path)

    if os.path.exists(lab_events_csv_path):
        os.remove(lab_events_csv_path)

    if os.path.exists(sepsis_patients_chart_events_flattened_path):
        os.remove(sepsis_patients_chart_events_flattened_path)

    if os.path.exists(sepsis_patients_lab_events_flattened_path):
        os.remove(sepsis_patients_lab_events_flattened_path)

    if os.path.exists(sepsis_patient_flattened_data_csv_path):
        os.remove(sepsis_patient_flattened_data_csv_path)

    if os.path.exists(sepsis_patients_with_sirs_cond_path):
        os.remove(sepsis_patients_with_sirs_cond_path)

    if os.path.exists(sepsis_patients_with_no_sirs_cond_path):
        os.remove(sepsis_patients_with_no_sirs_cond_path)

if os.path.exists(old_sepsis_patients_csv_path):
    os.rename(old_sepsis_patients_csv_path, sepsis_patients_csv_path)