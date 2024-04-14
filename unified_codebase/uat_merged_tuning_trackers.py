import pandas as pd
import numpy as np


def load_tuning_tracker(file_path: str, file_name: str) -> pd.DataFrame:
    """Load and filter the tuning tracker Excel file."""
    tracker = pd.read_excel(f"{file_path}{file_name}", sheet_name=0)
    tracker = tracker[tracker["Is Tunable"] == "Yes"].sort_values(
        by=["Rule ID", "Population Group"]
    )
    return tracker


def load_data_file(
    file_path: str, file_name: str, sheet_name: int = 0, dtype: type = str
) -> pd.DataFrame:
    """Load data from an Excel file."""
    data = pd.read_excel(f"{file_path}{file_name}", sheet_name=sheet_name, dtype=dtype)
    return data


def create_empty_columns(tracker: pd.DataFrame, columns_to_add: list) -> pd.DataFrame:
    """Create empty columns in the tracker DataFrame."""
    for column in columns_to_add:
        tracker[column] = np.nan
    return tracker


def calculate_metrics(
    tracker: pd.DataFrame, delta_data: pd.DataFrame, sample_data: pd.DataFrame
) -> pd.DataFrame:
    """Calculate metrics for each row in the tracker DataFrame."""
    for index, row in tracker.iterrows():
        rule = row["Rule ID"]
        population_group = row["Population Group"]
        parameter_type = row["Parameter Type"]
        operator = row["Operator"]
        threshold = float(row["Recommended Threshold"])

        delta_data_filtered = delta_data[
            (delta_data["Rule ID"] == rule)
            & (delta_data["Population Group"] == population_group)
        ]
        sample_data_filtered = sample_data[
            (sample_data["Rule ID"] == rule)
            & (sample_data["Population Group"] == population_group)
        ]

        tracker.at[index, "Num Alerts Extracted"] = len(delta_data_filtered)
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
        # alerts[parameter_type] = pd.to_numeric(alerts[parameter_type], errors="coerce")
        # ! test change v1
        if parameter_type in alerts.columns:
            alerts[parameter_type] = pd.to_numeric(
                alerts[parameter_type], errors="coerce"
            )
        else:
            # Handle the case when the column is not present
            print(
                f"Column '{parameter_type}' not found in alerts DataFrame. Skipping conversion."
            )

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

    return tracker


def calculate_net_metrics(tracker: pd.DataFrame, sample_data: pd.DataFrame) -> tuple:
    """Calculate net effectiveness and other metrics."""
    net_alerts_final = pd.DataFrame()

    for rule in tracker["Rule ID"].unique():
        tracker_rule = tracker[tracker["Rule ID"] == rule]
        for group in tracker_rule["Population Group"].unique():
            tracker_rule_group = tracker_rule[tracker_rule["Population Group"] == group]
            net_alerts = sample_data[
                (sample_data["Rule ID"] == rule)
                & (sample_data["Population Group"] == group)
            ]

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

            net_interesting = len(
                net_alerts[net_alerts["Tuning Decision"] == "Interesting"]
            )
            net_notinteresting = len(
                net_alerts[net_alerts["Tuning Decision"] == "Not Interesting"]
            )

            net_alerts_final = pd.concat([net_alerts_final, net_alerts])

            net_effectiveness = (
                round(
                    100 * (net_interesting) / (net_interesting + net_notinteresting), 2
                )
                if (net_interesting + net_notinteresting) > 0
                else 0
            )

            tracker.loc[tracker_rule_group.index, "Net Effectiveness"] = (
                net_effectiveness
            )

    return tracker, net_alerts_final


def calculate_final_fields(tracker: pd.DataFrame) -> pd.DataFrame:
    """Calculate final fields in the tracker DataFrame."""
    tracker["Effectiveness"] = round(
        100
        * (tracker["Interesting Alerts"])
        / (tracker["Interesting Alerts"] + tracker["Not Interesting Alerts"]),
        2,
    )
    tracker["Prop Effectiveness"] = round(
        100
        * tracker["Prop Interesting Alerts"]
        / (tracker["Prop Interesting Alerts"] + tracker["Prop Not Interesting Alerts"]),
        2,
    )
    return tracker


def process_btl_tuning_tracker(
    tracker_file_path: str,
    tracker_file_name: str,
    delta_file_path: str,
    delta_file_name: str,
    sample_file_path: str,
    sample_file_name: str,
    output_file_path: str,
    output_file_name: str,
):
    """Process the BTL tuning tracker and generate the output file."""
    tracker = load_tuning_tracker(tracker_file_path, tracker_file_name)
    delta_data = load_data_file(delta_file_path, delta_file_name, sheet_name=1)
    sample_data = load_data_file(sample_file_path, sample_file_name)

    sample_data["Alert Date"] = pd.to_datetime(sample_data["Alert Date"])
    earliest_date = sample_data["Alert Date"].min()
    latest_date = sample_data["Alert Date"].max()
    first_day_earliest_month = earliest_date.replace(day=1)
    last_day_latest_month = latest_date.replace(day=1) + pd.offsets.MonthEnd()
    date_range_value = f"{first_day_earliest_month.strftime('%m/%d/%Y')} - {last_day_latest_month.strftime('%m/%d/%Y')}"
    tracker["Date Range"] = date_range_value

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
    tracker = create_empty_columns(tracker, columns_to_add)

    tracker = calculate_metrics(tracker, delta_data, sample_data)
    tracker, _ = calculate_net_metrics(tracker, sample_data)
    tracker = calculate_final_fields(tracker)

    tracker.fillna(0, inplace=True)
    tracker.to_excel(f"{output_file_path}{output_file_name}", index=False)


def calculate_atl_metrics(
    tracker: pd.DataFrame, dedupe_data: pd.DataFrame
) -> pd.DataFrame:
    """Calculate metrics for each row in the ATL tracker DataFrame."""
    for index, row in tracker.iterrows():
        rule = row["Rule ID"].upper()
        population_group = row["Population Group"]
        parameter_type = row["Parameter Type"]
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

        if parameter_type in alerts.columns:
            alerts[parameter_type] = pd.to_numeric(
                alerts[parameter_type], errors="coerce"
            )
        else:
            print(
                f"Column '{parameter_type}' not found in alerts DataFrame. Skipping conversion."
            )
            continue

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

    return tracker


def calculate_atl_net_metrics(
    tracker: pd.DataFrame, dedupe_data: pd.DataFrame
) -> tuple:
    """Calculate net effectiveness, SAR yield, and other metrics for ATL."""
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


def calculate_atl_final_fields(tracker: pd.DataFrame) -> pd.DataFrame:
    """Calculate final fields in the ATL tracker DataFrame."""
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
    return tracker


def process_atl_tuning_tracker(
    tracker_file_path: str,
    tracker_file_name: str,
    dedupe_file_path: str,
    dedupe_file_name: str,
    output_file_path: str,
    output_file_name: str,
):
    """Process the ATL tuning tracker and generate the output file."""
    tracker = load_tuning_tracker(tracker_file_path, tracker_file_name)
    dedupe_data = load_data_file(dedupe_file_path, dedupe_file_name)
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
    tracker = create_empty_columns(tracker, columns_to_add)

    tracker = calculate_atl_metrics(tracker, dedupe_data)
    tracker, net_alerts_final = calculate_atl_net_metrics(tracker, dedupe_data)
    tracker = calculate_atl_final_fields(tracker)

    tracker.fillna(0, inplace=True)
    tracker.to_excel(f"{output_file_path}{output_file_name}", index=False)

    print(
        f"Number of unique Alert IDs in the final dataset: {net_alerts_final['Alert ID'].nunique()}"
    )


if __name__ == "__main__":
    # Usage example for BTL tuning
    btl_tracker_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/Initial Data/Tuning Tracker - BTL/"
    btl_tracker_file_name = "SAM Tuning - Proposed BTL High Priority Thresholds.xlsx"

    btl_delta_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/Initial Data/Tuning Tracker - BTL/"
    btl_delta_file_name = "UAT Alerts - Parsed and Deduped.xlsx"

    btl_sample_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/Initial Data/Tuning Tracker - BTL/"
    btl_sample_file_name = "UAT Alerts - Sampled and Decisioned.xlsx"

    btl_output_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/New Data Files/"
    btl_output_file_name = "Production BTL Tuning Tracker - With Calculations.xlsx"

    process_btl_tuning_tracker(
        btl_tracker_file_path,
        btl_tracker_file_name,
        btl_delta_file_path,
        btl_delta_file_name,
        btl_sample_file_path,
        btl_sample_file_name,
        btl_output_file_path,
        btl_output_file_name,
    )

    # Usage example for ATL tuning
    atl_tracker_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/Initial Data/Tuning Tracker - ATL/"
    atl_tracker_file_name = "Proposed ATL High Priority Thresholds.xlsx"

    atl_dedupe_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/Initial Data/Tuning Tracker - ATL/"
    atl_dedupe_file_name = "Actimize Alert Output - Parsed and Deduped.xlsx"

    atl_output_file_path = "C:/Users/Kadamativ/OneDrive - Crowe LLP/Office_Workspace/TM Tuning Project/code_integration_tm_scripts/New Data Files/"
    atl_output_file_name = "Actimize Tuning Tracker - ATL Calculations.xlsx"

    process_atl_tuning_tracker(
        atl_tracker_file_path,
        atl_tracker_file_name,
        atl_dedupe_file_path,
        atl_dedupe_file_name,
        atl_output_file_path,
        atl_output_file_name,
    )
