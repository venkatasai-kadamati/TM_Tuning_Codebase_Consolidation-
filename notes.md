<!-- atl loading filepaths and filenames -->

# Provide name for tuning tracker output

new_tracker_name = "Actimize Tuning Tracker - ATL Calculations - Python - NEW6.xlsx"

# Load in tuning tracker

tracker_fp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/Initial Data/Tuning Tracker - ATL/"
tracker_ufp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/New Data Files/"
tracker_file_name = "Proposed ATL High Priority Thresholds.xlsx"
tracker = pd.read_excel(f"{tracker_fp}{tracker_file_name}", sheet_name=0)
tracker.sort_values(by="Rule ID", inplace=True)

# Filter to include only records where 'Is Tunable' equals "Yes"

tracker = tracker[tracker["Is Tunable"] == "Yes"].sort_values(by="Rule ID")

# Load in deduped data

dedupe_fp = "C:/Users/WalshP/OneDrive - Crowe LLP/Desktop/Internal Initiatives/AT Innovation Challenge/TM/Initial Data/Tuning Tracker - ATL/"
dedupe_file_name = "Actimize Alert Output - Parsed and Deduped.xlsx"
dedupe_data = pd.read_excel(f"{dedupe_fp}{dedupe_file_name}", sheet_name=0, dtype=str)
