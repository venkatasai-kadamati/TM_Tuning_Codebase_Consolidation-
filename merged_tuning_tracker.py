import numpy as np
import pandas as pd


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


def populate_current_result_info_btl(tracker, delta_data, sample_data):
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

    return tracker


def populate_current_result_info_atl(tracker, dedupe_data):
    for index, row in tracker.iterrows():
        rule = row["Rule ID"].upper()
        population_group = row["Population Group"]
        # parameter_type = row["Parameter Type"]
        # !!!! change log v1: the above line is changed to the following line TODO: check if this is correct
        parameter_type = "Occurrence_Parameter"
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

    return tracker


def calculate_net_effectiveness_btl(tracker, sample_data):
    for rule in tracker["Rule ID"].unique():
        tracker_rule = tracker[tracker["Rule ID"] == rule]
        for group in tracker_rule["Population Group"].unique():
            tracker_rule_group = tracker_rule[tracker_rule["Population Group"] == group]
            net_alerts = sample_data[
                (sample_data["Rule ID"] == rule)
                & (sample_data["Population Group"] == group)
            ]

            for index, row in tracker_rule_group.iterrows():
                # param = row["Parameter Type"]
                # !!!! change log v1: the above line is changed to the following line TODO: check if this is correct
                param = "Occurrence_Parameter"

                oper = row["Operator"]
                rec = float(row["Recommended Threshold"])

                if oper == ">=":
                    condition = net_alerts[param].astype(float) >= rec
                elif oper == ">":
                    condition = net_alerts[param].astype(float) > rec
                elif oper == "<=":
                    condition = net_alerts[param].astype(float) <= rec
                elif oper == "<":
                    condition = net_alerts[param].astype(float) < rec
                else:
                    raise ValueError(f"Unknown operator: {oper}")

                net_alerts = net_alerts[condition]

            net_interesting = len(
                net_alerts[net_alerts["Tuning Decision"] == "Interesting"]
            )
            net_notinteresting = len(
                net_alerts[net_alerts["Tuning Decision"] == "Not Interesting"]
            )

            net_effectiveness = (
                round(100 * net_interesting / (net_interesting + net_notinteresting), 2)
                if (net_interesting + net_notinteresting) > 0
                else 0
            )

            tracker.loc[tracker_rule_group.index, "Net Effectiveness"] = (
                net_effectiveness
            )

    return tracker


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
                # param = row["Parameter Type"]
                # !!!! change log v1: the above line is changed to the following line TODO: check if this is correct
                param = "Occurrence_Parameter"

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
    return tracker


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
    return tracker


def process_btl_tuning_tracker(
    tracker_file_path,
    tracker_file_name,
    delta_file_path,
    delta_file_name,
    sample_file_path,
    sample_file_name,
    output_file_path,
    output_file_name,
):
    tracker = read_excel(tracker_file_path + tracker_file_name, sheet_name=0)
    tracker = filter_tracker(tracker, "Yes")

    delta_data = read_excel(delta_file_path + delta_file_name, sheet_name=1)
    sample_data = read_excel(sample_file_path + sample_file_name, sheet_name=0)
    sample_data["Alert Date"] = pd.to_datetime(sample_data["Alert Date"])

    tracker["Date Range"] = calculate_date_range(sample_data, "Alert Date")

    columns_to_add_btl = [
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
    tracker = create_empty_columns(tracker, columns_to_add_btl)

    tracker = populate_current_result_info_btl(tracker, delta_data, sample_data)
    tracker = calculate_net_effectiveness_btl(tracker, sample_data)
    tracker = calculate_final_fields_btl(tracker)

    tracker.fillna(0, inplace=True)
    tracker.to_excel(output_file_path + output_file_name, index=False)


def process_atl_tuning_tracker(
    tracker_file_path,
    tracker_file_name,
    dedupe_file_path,
    dedupe_file_name,
    output_file_path,
    output_file_name,
):
    tracker = read_excel(tracker_file_path + tracker_file_name, sheet_name=0)
    tracker = filter_tracker(tracker, "Yes")

    dedupe_data = read_excel(dedupe_file_path + dedupe_file_name, sheet_name=0)

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
    tracker = create_empty_columns(tracker, columns_to_add_atl)

    tracker = populate_current_result_info_atl(tracker, dedupe_data)
    tracker, net_alerts_final = calculate_net_effectiveness_atl(tracker, dedupe_data)
    tracker = calculate_final_fields_atl(tracker)

    tracker.fillna(0, inplace=True)
    tracker.to_excel(output_file_path + output_file_name, index=False)

    print(net_alerts_final["Alert ID"].nunique())


if __name__ == "__main__":
    # Define file paths for BTL
    btl_tracker_fp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/Initial Data/Tuning Tracker - BTL/"

    btl_tracker_ufp = (
        "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/New Data/"
    )

    btl_tracker_file_name = "SAM Tuning - Proposed BTL High Priority Thresholds.xlsx"
    btl_delta_file_name = "UAT Alerts - Parsed and Deduped.xlsx"
    btl_sample_file_name = "UAT Alerts - Sampled and Decisioned.xlsx"
    btl_output_file_name = "Production BTL Tuning Tracker - With Calculations.xlsx"

    # Process BTL tuning tracker
    process_btl_tuning_tracker(
        btl_tracker_fp,
        btl_tracker_file_name,
        btl_tracker_fp,
        btl_delta_file_name,
        btl_tracker_fp,
        btl_sample_file_name,
        btl_tracker_ufp,
        btl_output_file_name,
    )

    # Define file paths for ATL
    atl_tracker_fp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/Initial Data/Tuning Tracker - ATL/"

    atl_tracker_ufp = (
        "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/New Data/"
    )

    atl_tracker_file_name = "Proposed ATL High Priority Thresholds.xlsx"
    atl_dedupe_file_name = "Actimize Alert Output - Parsed and Deduped.xlsx"
    atl_output_file_name = "Actimize Tuning Tracker - ATL Calculations.xlsx"

    # Process ATL tuning tracker
    process_atl_tuning_tracker(
        atl_tracker_fp,
        atl_tracker_file_name,
        atl_tracker_fp,
        atl_dedupe_file_name,
        atl_tracker_ufp,
        atl_output_file_name,
    )
