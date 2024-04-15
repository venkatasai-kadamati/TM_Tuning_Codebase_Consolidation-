# TM_Tuning_Codebase_Consolidation-

# Provide name for tuning tracker output

new_tracker_name = "latest_output_atl.xlsx"

# Load in tuning tracker

tracker_fp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/Initial Data/Tuning Tracker - ATL/"
tracker_ufp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/New Data/"
tracker_file_name = "Proposed ATL High Priority Thresholds.xlsx"
tracker = pd.read_excel(f"{tracker_fp}{tracker_file_name}", sheet_name=0)
tracker.sort_values(by="Rule ID", inplace=True)

# Debug: Print the shape of the tracker after loading and sorting

print("Shape of tracker DataFrame after loading and sorting:", tracker.shape)

# Filter to include only records where 'Is Tunable' equals "Yes"

tracker = tracker[tracker["Is Tunable"] == "Yes"].sort_values(by="Rule ID")

# Debug: Print the shape of the tracker after filtering

print("Shape of tracker DataFrame after filtering 'Is Tunable':", tracker.shape)

# Load in deduped data

dedupe_fp = "C:/Users/kadam/Documents/TM_Tuning_Codebase_Consolidation/Initial Data/Tuning Tracker - ATL/"
dedupe_file_name = "Actimize Alert Output - Parsed and Deduped.xlsx"
dedupe_data = pd.read_excel(f"{dedupe_fp}{dedupe_file_name}", sheet_name=0, dtype=str)
print("Column names in 'dedupe_data':", dedupe_data.columns.tolist())
