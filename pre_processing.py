import pandas as pd
import os
import data_extraction as de

if not os.path.exists(os.path.join(os.getcwd(), "sepsis_patients_lab_events.csv")):
    de.ExtractSepsisLabEvents(os.path.join(os.getcwd(), "sepsis_patients_lab_events.csv"))

if not os.path.exists(os.path.join(os.getcwd(), "sepsis_patients_chart_events.csv")):
    de.ExtractSepsisChartEvents(os.path.join(os.getcwd(), "sepsis_patients_chart_events.csv"))