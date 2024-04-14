# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 15:20:21 2024

@author: WalshP
"""

import pandas as pd
import numpy as np

# Provide name for tuning tracker output
new_tracker_name = (
    "Production BTL Tuning Tracker - With Calculations - Python - NEW.xlsx"
)

# Define file paths
tracker_fp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/Initial Data/Tuning Tracker - BTL/"
tracker_ufp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/New Data Files/"
tracker_file_name = "SAM Tuning - Proposed BTL High Priority Thresholds.xlsx"
delta_file_name = "UAT Alerts - Parsed and Deduped.xlsx"
sample_file_name = "UAT Alerts - Sampled and Decisioned.xlsx"

# Load in tuning tracker
tracker = pd.read_excel(tracker_fp + tracker_file_name, sheet_name=0)

# Filter the tracker DataFrame to include only records where 'Is Tunable' equals "Yes"
tracker = tracker[tracker["Is Tunable"] == "Yes"]

tracker.sort_values(by=["Rule ID", "Population Group"], inplace=True)

# Load in delta population data
delta_data = pd.read_excel(tracker_fp + delta_file_name, sheet_name=1, dtype=str)

# Load in sampled data
sample_data = pd.read_excel(tracker_fp + sample_file_name, sheet_name=0, dtype=str)
# Convert 'Alert Date' to datetime
sample_data["Alert Date"] = pd.to_datetime(sample_data["Alert Date"])

# Find the earliest and latest dates
earliest_date = sample_data["Alert Date"].min()
latest_date = sample_data["Alert Date"].max()

# Adjust to the first day of the earliest month and the last day of the latest month
first_day_earliest_month = earliest_date.replace(day=1)
last_day_latest_month = latest_date.replace(day=1) + pd.offsets.MonthEnd()

# Create the 'Date Range' column in the 'tracker' DataFrame
date_range_value = f"{first_day_earliest_month.strftime('%m/%d/%Y')} - {last_day_latest_month.strftime('%m/%d/%Y')}"
tracker["Date Range"] = date_range_value

# Create empty columns
columns_to_add = [
    "Num Alerts Extracted",
    "Num Alerts Sampled",
    "Interesting Alerts",
    "Not Interesting Alerts",
    "Data Quality Alerts",
    "Effectiveness",
    "Prop Interesting Alerts",
    "Prop Not Interesting Alerts",
    "Prop Effectiveness",
    "Net Effectiveness",
    "Min Val",
    "Max Val",
]
for column in columns_to_add:
    tracker[column] = np.nan

# Populate current result info
for index, row in tracker.iterrows():
    rule = row["Rule ID"]
    population_group = row["Population Group"]
    parameter_type = row["Parameter Type"]
    operator = row["Operator"]
    threshold = float(row["Recommended Threshold"])

    # Filters for delta and sample data
    delta_data_size = delta_data[
        (delta_data["Rule ID"] == rule)
        & (delta_data["Population Group"] == population_group)
    ]
    sample_data_size = sample_data[
        (sample_data["Rule ID"] == rule)
        & (sample_data["Population Group"] == population_group)
    ]
    sample_data_interesting = sample_data_size[
        sample_data_size["Tuning Decision"] == "Interesting"
    ]
    sample_data_notinteresting = sample_data_size[
        sample_data_size["Tuning Decision"] == "Not Interesting"
    ]
    sample_data_dataquality = sample_data_size[
        sample_data_size["Tuning Decision"] == "Data Quality"
    ]

    # Assigning values to tracker
    tracker.at[index, "Num Alerts Extracted"] = len(delta_data_size)
    tracker.at[index, "Num Alerts Sampled"] = len(sample_data_size)
    tracker.at[index, "Interesting Alerts"] = len(sample_data_interesting)
    tracker.at[index, "Not Interesting Alerts"] = len(sample_data_notinteresting)
    tracker.at[index, "Data Quality Alerts"] = len(sample_data_dataquality)

    # Calculate proposed result info
    alerts = sample_data_size
    alerts_interesting = alerts[alerts["Tuning Decision"] == "Interesting"]
    alerts_notinteresting = alerts[alerts["Tuning Decision"] == "Not Interesting"]

    # Define a function to apply the operator and threshold
    def apply_operator(data, op, threshold, param):
        values = data[param].astype(float)
        if op == ">=":
            return np.sum(values >= threshold)
        elif op == ">":
            return np.sum(values > threshold)
        elif op == "<=":
            return np.sum(values <= threshold)
        elif op == "<":
            return np.sum(values < threshold)

    prop_interesting = apply_operator(
        alerts_interesting, operator, threshold, parameter_type
    )
    prop_notinteresting = apply_operator(
        alerts_notinteresting, operator, threshold, parameter_type
    )

    tracker.at[index, "Prop Interesting Alerts"] = prop_interesting
    tracker.at[index, "Prop Not Interesting Alerts"] = prop_notinteresting

    # Determine min/max values seen
    sample_data_values = sample_data_size[parameter_type].astype(float)
    if sample_data_values.empty:
        tracker.at[index, "Min Val"] = np.nan
        tracker.at[index, "Max Val"] = np.nan
    else:
        tracker.at[index, "Min Val"] = sample_data_values.min()
        tracker.at[index, "Max Val"] = sample_data_values.max()

# Calculate net effectiveness
net_effectiveness_list = []

# Iterate over unique rules
for rule in tracker["Rule ID"].unique():
    tracker_rule = tracker[tracker["Rule ID"] == rule]

    # Iterate over unique population groups within the rule
    for group in tracker_rule["Population Group"].unique():
        tracker_rule_pop = tracker_rule[tracker_rule["Population Group"] == group]
        net_alerts = sample_data[
            (sample_data["Rule ID"] == rule)
            & (sample_data["Population Group"] == group)
        ]

        # Apply the thresholds for each row in the population group
        for _, rule_pop_row in tracker_rule_pop.iterrows():
            param = rule_pop_row["Parameter Type"]
            oper = rule_pop_row["Operator"]
            rec = float(rule_pop_row["Recommended Threshold"])

            # Apply the operator and threshold
            if oper == ">=":
                temp = net_alerts[net_alerts[param].astype(float) >= rec]
            elif oper == ">":
                temp = net_alerts[net_alerts[param].astype(float) > rec]
            elif oper == "<=":
                temp = net_alerts[net_alerts[param].astype(float) <= rec]
            elif oper == "<":
                temp = net_alerts[net_alerts[param].astype(float) < rec]

            # Update net_alerts to only include alerts that meet all criteria
            net_alerts = net_alerts[net_alerts.index.isin(temp.index)]

        # Calculate effectiveness for the group
        net_interesting = len(
            net_alerts[net_alerts["Tuning Decision"] == "Interesting"]
        )
        net_notinteresting = len(
            net_alerts[net_alerts["Tuning Decision"] == "Not Interesting"]
        )

        # Calculate net fields
        net_effectiveness = (
            round(100 * (net_interesting) / (net_interesting + net_notinteresting), 2)
            if (net_interesting + net_notinteresting) > 0
            else 0
        )

        # if (net_interesting + net_notinteresting) > 0:
        # net_effectiveness = round(100 * net_interesting / (net_interesting + net_notinteresting), 2)
        # else:
        # net_effectiveness = 0

        # Append the effectiveness to the list, repeated for the number of rows in the population group
        net_effectiveness_list.extend([net_effectiveness] * len(tracker_rule_pop))

# Assign the net effectiveness list to the 'Net Effectiveness' column in the tracker
tracker["Net Effectiveness"] = net_effectiveness_list

# Calculate other effectiveness metrics
tracker["Effectiveness"] = round(
    100
    * tracker["Interesting Alerts"]
    / (tracker["Interesting Alerts"] + tracker["Not Interesting Alerts"]),
    2,
)
tracker["Prop Effectiveness"] = round(
    100
    * tracker["Prop Interesting Alerts"]
    / (tracker["Prop Interesting Alerts"] + tracker["Prop Not Interesting Alerts"]),
    2,
)

# Assuming 'df' is your DataFrame
tracker.fillna(0, inplace=True)

# Export tracker to Excel
tracker.to_excel(tracker_ufp + new_tracker_name, index=False)
