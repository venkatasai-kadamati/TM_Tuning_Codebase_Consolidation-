import pandas as pd

fp = 'C:/Users/SprongJ/OneDrive - Crowe LLP/Documents/Non-Charge/Innovation Challenge/' #update file path 
updated_fp = 'C:/Users/SprongJ/OneDrive - Crowe LLP/Documents/Non-Charge/Innovation Challenge/' #update file path 

data = pd.read_excel(fp + 'Actimize_Alerts_2022-05-31_to_2023-05-31 FINAL.xlsx', sheet_name = 'Parsed Data') #Update name of file, confirm sheet

rules = data['Rule ID'].value_counts() #confirm the name of the rule id column.

DeDuped = pd.DataFrame()

for x in rules.index:
  split_x = x.split('-')
  if split_x[1] == 'HBC' or split_x[2] == 'HBC':
    dataManip = data[data['Rule ID']==x]
    dataManip = dataManip.sort_values(by=['Data Date','Transaction Date'], ascending=[False,False])
    dataManip = dataManip.drop_duplicates(subset=['Alert ID', 'Rule ID','Account Number']) #confirm column names
  elif split_x[5] == 'M01' or split_x[5] == 'M03':
    dataManip = data[data['Rule ID']==x]
    dataManip = dataManip.sort_values(by=['Data Date','Transaction Date'], ascending=[False,False])
    dataManip = dataManip.drop_duplicates(subset=['Alert ID', 'Rule ID','Account Number']) #confirm column names
  else:
    dataManip = data[data['Rule ID']==x]
    dataManip = dataManip.sort_values(by='Data Date', ascending=True)
    dataManip = dataManip.drop_duplicates(subset=['Alert ID', 'Rule ID','Account Number', 'Transaction Date', 'Value_Parameter']) #confirm column names
  DeDuped = pd.concat([DeDuped, dataManip.iloc[:, :50]], ignore_index=True)
  
DeDuped.to_csv(updated_fp + 'Python Data Compare.csv', index=False)

split_x[1]