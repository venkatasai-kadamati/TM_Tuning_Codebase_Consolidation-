# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 09:15:46 2024

@author: WalshP
"""

import pandas as pd
import numpy as np

# Provide name for tuning tracker output
new_tracker_name = "latest_output_atl.xlsx"

# Load in tuning tracker
tracker_fp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/Initial Data/Tuning Tracker - ATL/"
tracker_ufp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/New Data/"
tracker_file_name = "Proposed ATL High Priority Thresholds.xlsx"
tracker = pd.read_excel(f"{tracker_fp}{tracker_file_name}", sheet_name=0)
tracker.sort_values(by="Rule ID", inplace=True)

# Filter to include only records where 'Is Tunable' equals "Yes"
tracker = tracker[tracker["Is Tunable"] == "Yes"].sort_values(by="Rule ID")

# Load in deduped data
dedupe_fp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/Initial Data/Tuning Tracker - ATL/"
dedupe_file_name = "Actimize Alert Output - Parsed and Deduped.xlsx"
dedupe_data = pd.read_excel(f"{dedupe_fp}{dedupe_file_name}", sheet_name=0, dtype=str)

# Create empty columns
columns_to_add = [
    "Num Alerts Extracted",
    "SARs Filed",
    "Interesting Alerts",
    "Not Interesting Alerts",
    "Data Quality Alerts",
    "Effectiveness",
    "SAR Yield",
    "Prop SARs Filed",
    "Prop Interesting Alerts",
    "Prop Not Interesting Alerts",
    "Prop Effectiveness",
    "Prop SAR Yield",
    "Not Interesting Alert Reduction",
    "Net Effectiveness",
    "Net SAR Yield",
    "Net Not Interesting Alert Reduction",
    "Min Val",
    "Max Val",
    "Alert Count",
    "Proposed Alert Count",
    "Proposed Alert Reduction",
]
for column in columns_to_add:
    tracker[column] = np.nan

# Populate current result info
for index, row in tracker.iterrows():
    rule = row["Rule ID"].upper()
    population_group = row["Population Group"]
    parameter_type = "Occurrence_Parameter"
    # parameter_type = row["Parameter Type"]
    operator = row["Operator"]
    threshold = float(row["Recommended Threshold"])

    # Filter dedupe_data based on conditions
    dedupe_data_filtered = dedupe_data[
        (dedupe_data["Rule ID"].str.upper() == rule)
        & (dedupe_data["Population Group"] == population_group)
    ]

    # Calculate the number of alerts for different categories
    tracker.at[index, "Num Alerts Extracted"] = len(dedupe_data_filtered)
    tracker.at[index, "SARs Filed"] = len(
        dedupe_data_filtered[dedupe_data_filtered["Tuning Decision"] == "SAR Filed"]
    )
    tracker.at[index, "Interesting Alerts"] = len(
        dedupe_data_filtered[dedupe_data_filtered["Tuning Decision"] == "Interesting"]
    )
    tracker.at[index, "Not Interesting Alerts"] = len(
        dedupe_data_filtered[
            dedupe_data_filtered["Tuning Decision"] == "Not Interesting"
        ]
    )
    tracker.at[index, "Data Quality Alerts"] = len(
        dedupe_data_filtered[dedupe_data_filtered["Tuning Decision"] == "Data Quality"]
    )

    # Calculate proposed result info
    alerts = dedupe_data_filtered.copy()
    alerts[parameter_type] = pd.to_numeric(alerts[parameter_type], errors="coerce")

    # Apply thresholding logic to different categories
    prop_SAR = alerts[
        (alerts["Tuning Decision"] == "SAR Filed")
        & (alerts[parameter_type] >= threshold)
    ].shape[0]
    prop_interesting = alerts[
        (alerts["Tuning Decision"] == "Interesting")
        & (alerts[parameter_type] >= threshold)
    ].shape[0]
    prop_notinteresting = alerts[
        (alerts["Tuning Decision"] == "Not Interesting")
        & (alerts[parameter_type] >= threshold)
    ].shape[0]

    tracker.at[index, "Prop SARs Filed"] = prop_SAR
    tracker.at[index, "Prop Interesting Alerts"] = prop_interesting
    tracker.at[index, "Prop Not Interesting Alerts"] = prop_notinteresting

    # Determine min/max values seen
    values = alerts[parameter_type].dropna()
    tracker.at[index, "Min Val"] = values.min() if not values.empty else np.nan
    tracker.at[index, "Max Val"] = values.max() if not values.empty else np.nan

# Calculate net effectiveness and SAR yield
net_alerts_final = pd.DataFrame()

for rule in tracker["Rule ID"].unique():
    tracker_rule = tracker[tracker["Rule ID"].str.upper() == rule.upper()]
    for group in tracker_rule["Population Group"].unique():
        tracker_rule_group = tracker_rule[tracker_rule["Population Group"] == group]
        net_alerts = dedupe_data[
            (dedupe_data["Rule ID"] == rule)
            & (dedupe_data["Population Group"] == group)
        ]
        net_alerts_count = net_alerts["Alert ID"].nunique()

        for index, row in tracker_rule_group.iterrows():
            param = "Occurrence_Parameter"
            oper = row["Operator"]
            rec = float(row["Recommended Threshold"])

            # net_alerts[param] = pd.to_numeric(net_alerts[param])
            net_alerts.loc[:, param] = pd.to_numeric(net_alerts[param])
            if oper == ">=":
                condition = net_alerts[param] >= rec
            elif oper == ">":
                condition = net_alerts[param] > rec
            elif oper == "<=":
                condition = net_alerts[param] <= rec
            elif oper == "<":
                condition = net_alerts[param] < rec
            else:
                raise ValueError(f"Unknown operator: {oper}")

            net_alerts = net_alerts[condition]

        net_SAR = len(net_alerts[net_alerts["Tuning Decision"] == "SAR Filed"])
        net_interesting = len(
            net_alerts[net_alerts["Tuning Decision"] == "Interesting"]
        )
        net_notinteresting = len(
            net_alerts[net_alerts["Tuning Decision"] == "Not Interesting"]
        )
        net_alerts_filtered_count = net_alerts["Alert ID"].nunique()

        net_alerts_final = pd.concat([net_alerts_final, net_alerts])

        # Calculate net fields
        net_effectiveness = (
            round(
                100
                * (net_interesting + net_SAR)
                / (net_SAR + net_interesting + net_notinteresting),
                2,
            )
            if (net_SAR + net_interesting + net_notinteresting) > 0
            else 0
        )
        net_SAR_yield = (
            round(100 * net_SAR / (net_SAR + net_interesting + net_notinteresting), 2)
            if (net_SAR + net_interesting + net_notinteresting) > 0
            else 0
        )
        initial_not_interesting = tracker_rule_group["Not Interesting Alerts"].iloc[0]
        net_notinterestingreduction = (
            round(
                100
                * (initial_not_interesting - net_notinteresting)
                / initial_not_interesting,
                2,
            )
            if initial_not_interesting > 0
            else 0
        )

        # Assign net fields to tracker
        tracker.loc[tracker_rule_group.index, "Net Effectiveness"] = net_effectiveness
        tracker.loc[tracker_rule_group.index, "Net SAR Yield"] = net_SAR_yield
        tracker.loc[tracker_rule_group.index, "Net Not Interesting Alert Reduction"] = (
            net_notinterestingreduction
        )
        tracker.loc[tracker_rule_group.index, "Alert Count"] = net_alerts_count
        tracker.loc[tracker_rule_group.index, "Proposed Alert Count"] = (
            net_alerts_filtered_count
        )

# Calculate final fields
tracker["Effectiveness"] = round(
    100
    * (tracker["Interesting Alerts"] + tracker["SARs Filed"])
    / tracker["Num Alerts Extracted"],
    2,
)
tracker["SAR Yield"] = round(
    100 * tracker["SARs Filed"] / tracker["Num Alerts Extracted"], 2
)
tracker["Prop Effectiveness"] = round(
    100
    * (tracker["Prop Interesting Alerts"] + tracker["Prop SARs Filed"])
    / (
        tracker["Prop SARs Filed"]
        + tracker["Prop Interesting Alerts"]
        + tracker["Prop Not Interesting Alerts"]
    ),
    2,
)
tracker["Prop SAR Yield"] = round(
    100
    * tracker["Prop SARs Filed"]
    / (
        tracker["Prop SARs Filed"]
        + tracker["Prop Interesting Alerts"]
        + tracker["Prop Not Interesting Alerts"]
    ),
    2,
)
tracker["Not Interesting Alert Reduction"] = round(
    100
    * (tracker["Not Interesting Alerts"] - tracker["Prop Not Interesting Alerts"])
    / tracker["Not Interesting Alerts"],
    2,
)
tracker["Proposed Alert Reduction"] = round(
    100
    * (tracker["Alert Count"] - tracker["Proposed Alert Count"])
    / tracker["Alert Count"],
    2,
)

# Assuming 'df' is your DataFrame
tracker.fillna(0, inplace=True)

# Export tracker to Excel
tracker.to_excel(f"{tracker_ufp}{new_tracker_name}", index=False)

# Print the number of unique Alert IDs in the final dataset
print(net_alerts_final["Alert ID"].nunique())
