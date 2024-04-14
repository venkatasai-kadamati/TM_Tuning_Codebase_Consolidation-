import pandas as pd
import numpy as np


def read_excel(file_path, sheet_name=0, dtype=str):
    return pd.read_excel(file_path, sheet_name=sheet_name, dtype=dtype)


def filter_tracker(tracker, is_tunable):
    return tracker[tracker["Is Tunable"] == is_tunable].sort_values(
        by=["Rule ID", "Population Group"]
    )


def create_empty_columns(tracker, columns_to_add):
    for column in columns_to_add:
        tracker[column] = np.nan
    return tracker


def calculate_date_range(data, date_column):
    earliest_date = data[date_column].min()
    latest_date = data[date_column].max()
    first_day_earliest_month = earliest_date.replace(day=1)
    last_day_latest_month = latest_date.replace(day=1) + pd.offsets.MonthEnd()
    return f"{first_day_earliest_month.strftime('%m/%d/%Y')} - {last_day_latest_month.strftime('%m/%d/%Y')}"


def populate_current_result_info_btl(tracker, sample_data):
    for index, row in tracker.iterrows():
        rule = row["Rule ID"]
        population_group = row["Population Group"]
        parameter_type = row["Parameter Type"]
        operator = row["Operator"]
        threshold = float(row["Recommended Threshold"])

        sample_data_filtered = sample_data[
            (sample_data["Rule ID"] == rule)
            & (sample_data["Population Group"] == population_group)
        ]

        tracker.at[index, "Num Alerts Sampled"] = len(sample_data_filtered)
        tracker.at[index, "Interesting Alerts"] = len(
            sample_data_filtered[
                sample_data_filtered["Tuning Decision"] == "Interesting"
            ]
        )
        tracker.at[index, "Not Interesting Alerts"] = len(
            sample_data_filtered[
                sample_data_filtered["Tuning Decision"] == "Not Interesting"
            ]
        )
        tracker.at[index, "Data Quality Alerts"] = len(
            sample_data_filtered[
                sample_data_filtered["Tuning Decision"] == "Data Quality"
            ]
        )

        alerts = sample_data_filtered.copy()
        alerts[parameter_type] = pd.to_numeric(alerts[parameter_type], errors="coerce")

        prop_interesting = alerts[
            (alerts["Tuning Decision"] == "Interesting")
            & (alerts[parameter_type] >= threshold)
        ].shape[0]
        prop_notinteresting = alerts[
            (alerts["Tuning Decision"] == "Not Interesting")
            & (alerts[parameter_type] >= threshold)
        ].shape[0]

        tracker.at[index, "Prop Interesting Alerts"] = prop_interesting
        tracker.at[index, "Prop Not Interesting Alerts"] = prop_notinteresting

        values = alerts[parameter_type].dropna()
        tracker.at[index, "Min Val"] = values.min() if not values.empty else np.nan
        tracker.at[index, "Max Val"] = values.max() if not values.empty else np.nan


def populate_current_result_info_atl(tracker, dedupe_data):
    for index, row in tracker.iterrows():
        rule = row["Rule ID"].upper()
        population_group = row["Population Group"]
        parameter_type = row["Parameter Type"]
        operator = row["Operator"]
        threshold = float(row["Recommended Threshold"])

        dedupe_data_filtered = dedupe_data[
            (dedupe_data["Rule ID"].str.upper() == rule)
            & (dedupe_data["Population Group"] == population_group)
        ]

        tracker.at[index, "Num Alerts Extracted"] = len(dedupe_data_filtered)
        tracker.at[index, "SARs Filed"] = len(
            dedupe_data_filtered[dedupe_data_filtered["Tuning Decision"] == "SAR Filed"]
        )
        tracker.at[index, "Interesting Alerts"] = len(
            dedupe_data_filtered[
                dedupe_data_filtered["Tuning Decision"] == "Interesting"
            ]
        )
        tracker.at[index, "Not Interesting Alerts"] = len(
            dedupe_data_filtered[
                dedupe_data_filtered["Tuning Decision"] == "Not Interesting"
            ]
        )
        tracker.at[index, "Data Quality Alerts"] = len(
            dedupe_data_filtered[
                dedupe_data_filtered["Tuning Decision"] == "Data Quality"
            ]
        )

        alerts = dedupe_data_filtered.copy()
        alerts[parameter_type] = pd.to_numeric(alerts[parameter_type], errors="coerce")

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

        values = alerts[parameter_type].dropna()
        tracker.at[index, "Min Val"] = values.min() if not values.empty else np.nan
        tracker.at[index, "Max Val"] = values.max() if not values.empty else np.nan


def calculate_net_effectiveness_btl(tracker, sample_data):
    net_effectiveness_list = []

    for rule in tracker["Rule ID"].unique():
        tracker_rule = tracker[tracker["Rule ID"] == rule]

        for group in tracker_rule["Population Group"].unique():
            tracker_rule_pop = tracker_rule[tracker_rule["Population Group"] == group]
            net_alerts = sample_data[
                (sample_data["Rule ID"] == rule)
                & (sample_data["Population Group"] == group)
            ]

            for _, rule_pop_row in tracker_rule_pop.iterrows():
                param = rule_pop_row["Parameter Type"]
                oper = rule_pop_row["Operator"]
                rec = float(rule_pop_row["Recommended Threshold"])

                if oper == ">=":
                    temp = net_alerts[net_alerts[param].astype(float) >= rec]
                elif oper == ">":
                    temp = net_alerts[net_alerts[param].astype(float) > rec]
                elif oper == "<=":
                    temp = net_alerts[net_alerts[param].astype(float) <= rec]
                elif oper == "<":
                    temp = net_alerts[net_alerts[param].astype(float) < rec]

                net_alerts = net_alerts[net_alerts.index.isin(temp.index)]

            net_interesting = len(
                net_alerts[net_alerts["Tuning Decision"] == "Interesting"]
            )
            net_notinteresting = len(
                net_alerts[net_alerts["Tuning Decision"] == "Not Interesting"]
            )

            net_effectiveness = (
                round(
                    100 * (net_interesting) / (net_interesting + net_notinteresting), 2
                )
                if (net_interesting + net_notinteresting) > 0
                else 0
            )

            net_effectiveness_list.extend([net_effectiveness] * len(tracker_rule_pop))

    tracker["Net Effectiveness"] = net_effectiveness_list


def calculate_net_effectiveness_atl(tracker, dedupe_data):
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
                param = row["Parameter Type"]
                oper = row["Operator"]
                rec = float(row["Recommended Threshold"])

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
                round(
                    100 * net_SAR / (net_SAR + net_interesting + net_notinteresting), 2
                )
                if (net_SAR + net_interesting + net_notinteresting) > 0
                else 0
            )
            initial_not_interesting = tracker_rule_group["Not Interesting Alerts"].iloc[
                0
            ]
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

            tracker.loc[tracker_rule_group.index, "Net Effectiveness"] = (
                net_effectiveness
            )
            tracker.loc[tracker_rule_group.index, "Net SAR Yield"] = net_SAR_yield
            tracker.loc[
                tracker_rule_group.index, "Net Not Interesting Alert Reduction"
            ] = net_notinterestingreduction
            tracker.loc[tracker_rule_group.index, "Alert Count"] = net_alerts_count
            tracker.loc[tracker_rule_group.index, "Proposed Alert Count"] = (
                net_alerts_filtered_count
            )

    return tracker, net_alerts_final


def calculate_final_fields_btl(tracker):
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


def calculate_final_fields_atl(tracker):
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


# Define file paths for BTL
btl_tracker_fp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/Initial Data/Tuning Tracker - BTL/"
btl_tracker_ufp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/New Data Files/"
btl_tracker_file_name = "SAM Tuning - Proposed BTL High Priority Thresholds.xlsx"
btl_sample_file_name = "UAT Alerts - Sampled and Decisioned.xlsx"

# Define file paths for ATL
atl_tracker_fp = "C:\Users\Kadamativ\OneDrive - Crowe LLP\Office_Workspace\TM Tuning Project\code_integration_tm_scripts\Initial Data\Tuning Tracker - ATL"
atl_tracker_ufp = "C:\Users\Kadamativ\OneDrive - Crowe LLP\Office_Workspace\TM Tuning Project\code_integration_tm_scripts\New Data Files/"
atl_tracker_file_name = "Proposed ATL High Priority Thresholds.xlsx"
atl_dedupe_file_name = "Actimize Alert Output - Parsed and Deduped.xlsx"

# Load in tuning tracker for BTL
btl_tracker = read_excel(btl_tracker_fp + btl_tracker_file_name, sheet_name=0)
btl_tracker = filter_tracker(btl_tracker, "Yes")

# Load in sampled data for BTL
btl_sample_data = read_excel(
    btl_tracker_fp + btl_sample_file_name, sheet_name=0, dtype=str
)
# Convert 'Alert Date' to datetime
btl_sample_data["Alert Date"] = pd.to_datetime(btl_sample_data["Alert Date"])

# Find the earliest and latest dates for BTL
btl_earliest_date = btl_sample_data["Alert Date"].min()
btl_latest_date = btl_sample_data["Alert Date"].max()

# Adjust to the first day of the earliest month and the last day of the latest month for BTL
btl_first_day_earliest_month = btl_earliest_date.replace(day=1)
btl_last_day_latest_month = btl_latest_date.replace(day=1) + pd.offsets.MonthEnd()

# Create the 'Date Range' column in the 'tracker' DataFrame for BTL
btl_date_range_value = calculate_date_range(btl_sample_data, "Alert Date")
btl_tracker["Date Range"] = btl_date_range_value

# Create empty columns for BTL
columns_to_add_btl = [
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
btl_tracker = create_empty_columns(btl_tracker, columns_to_add_btl)

# Populate current result info for BTL
populate_current_result_info_btl(btl_tracker, btl_sample_data)

# Calculate net effectiveness for BTL
calculate_net_effectiveness_btl(btl_tracker, btl_sample_data)

# Calculate final fields for BTL
calculate_final_fields_btl(btl_tracker)

# Assuming 'df' is your DataFrame
btl_tracker.fillna(0, inplace=True)

# Export tracker to Excel for BTL
btl_tracker.to_excel(
    btl_tracker_ufp
    + "Production BTL Tuning Tracker - With Calculations - Python - NEW.xlsx",
    index=False,
)

# Load in tuning tracker for ATL
atl_tracker = read_excel(atl_tracker_fp + atl_tracker_file_name, sheet_name=0)
atl_tracker = filter_tracker(atl_tracker, "Yes")

# Load in deduped data for ATL
atl_dedupe_data = read_excel(
    atl_tracker_fp + atl_dedupe_file_name, sheet_name=0, dtype=str
)

# Create empty columns for ATL
columns_to_add_atl = [
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
atl_tracker = create_empty_columns(atl_tracker, columns_to_add_atl)

# Populate current result info for ATL
populate_current_result_info_atl(atl_tracker, atl_dedupe_data)

# Calculate net effectiveness and SAR yield for ATL
atl_tracker, atl_net_alerts_final = calculate_net_effectiveness_atl(
    atl_tracker, atl_dedupe_data
)

# Calculate final fields for ATL
calculate_final_fields_atl(atl_tracker)

# Assuming 'df' is your DataFrame
atl_tracker.fillna(0, inplace=True)

# Export tracker to Excel for ATL
atl_tracker.to_excel(
    atl_tracker_ufp + "Actimize Tuning Tracker - ATL Calculations - Python - NEW.xlsx",
    index=False,
)

# Print the number of unique Alert IDs in the final dataset for ATL
print(atl_net_alerts_final["Alert ID"].nunique())
