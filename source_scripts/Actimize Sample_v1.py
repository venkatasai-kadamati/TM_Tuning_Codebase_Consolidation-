import pandas as pd
import numpy as np
from scipy.stats import stats

# Global Variables
I = 0.95 # Confidence Level
z = 1.96 # z-value
q = 0.5  # probability of success
c = 0.05 # confidence interval
minSamp = 100 # min samples size
sampSize = z*z*q*(1-q)/c/c # base sample size

# Parameters to stratify from
params = ['STDEV_Parameter', 'Ratio_Parameter', 'Value_Parameter', 'Occurrence_Parameter', 'Volume_Parameter']

pop_groups_exist = True

# Alert File
fp = 'C:/Users/SprongJ/OneDrive - Crowe LLP/Documents/Non-Charge/Innovation Challenge/TM Tuning/Sampling/' # update file path 

dataToSample = pd.read_excel(fp + 'Python Data Deduped.xlsx', sheet_name = 'Python Data Deduped', na_values = '') # Update name of file, confirm sheet

# Get list of rule id's in population
ruleIDs = dataToSample['Rule ID'].value_counts()

FullSample = pd.DataFrame()
test = pd.DataFrame()
sampleSize = {}

for x in ruleIDs.index: 

  dataToSample_Rule = dataToSample[dataToSample['Rule ID'] == x]
  
  # UPDATE THIS TO BE CORRECT INDEX OF PARAMETERS FOR EACH RULE.  
  if x in ['AML-EBB-IFT-ALL-A-D30-EOP','AML-EBO-IFT-ALL-P-D05-EOP','AML-EBA-IFT-ALL-A-D07-ERL','AML-EBA-IFT-ALL-P-D01-ERL','AML-EBB-IFT-ALL-P-D05-EOP']:
     paramUse = [params[i] for i in [2, 4]] # select which ones
  elif x in ['AML-HBC-CCE-INN-A-M01-HBN','AML-HBC-CCE-INN-A-M01-HBS']:
     paramUse = [params[i] for i in [0, 4]]
  elif x in ['AML-FTF-AWR-CSH-A-D05-FTR','AML-FTF-CSH-AWR-A-D07-FTR','AML-FTF-CSH-CSH-A-D07-FTR']:
     paramUse = [params[i] for i in [1, 2]]
  else:
     paramUse = [params[i] for i in [2, 3]]

  if pop_groups_exist:
     popGroups = dataToSample_Rule['Population Group'].value_counts()

     for pop in popGroups.index:
    
        if popGroups[pop] <= minSamp:
          dataToSample_Rule_Pop = dataToSample_Rule[dataToSample_Rule['Population Group'] == pop]
          dataToSample_Rule_Pop['StrataVal'] = 'NA'
    
          if FullSample.empty:
            FullSample = dataToSample_Rule_Pop 
          else:
            FullSample = pd.concat([FullSample, dataToSample_Rule_Pop])
    
          sampleSize[x + ':  ' + pop] = len(FullSample[(FullSample['Rule ID'] == x) & (FullSample['Population Group'] == pop)])
        else:
          # filter file to specific rule, popgroup
          sampleFrame = dataToSample_Rule[dataToSample_Rule['Population Group'] == pop]
          p = popGroups[pop] # get's population of popgroup for rule
          newSampSize = max(np.ceil(sampSize/(1+(sampSize-1)/p)), minSamp) # gets the new estimated sample size
    
    
          # Calculates Strata
          sampleFrame['StrataVal'] = '' 
          for i in paramUse: 
              strata_values = np.minimum(9, 0.1 * np.floor(sampleFrame[i].apply(lambda y : stats.percentileofscore(sampleFrame[i], y, kind = 'strict'))))
              sampleFrame['StrataVal'] += strata_values.astype(int).astype(str)
          
          # creates lookup table for strata indexing
          res = sampleFrame['StrataVal'].value_counts()
    
          # creates sample
          for a in res.index: 
    
            # filters to specific stratum
            strataPop = sampleFrame[sampleFrame['StrataVal'] == a]
    
            # finds sample size of stratum
            if(res[a] < 6):
                strataSampSize = res[a]
            elif(res[a]*newSampSize/p < 6):
                strataSampSize = 5
            else:
                strataSampSize = np.ceil(res[a]*newSampSize/p)
    
            # pulls the sample
            strataSamp = strataPop.sample(n=int(strataSampSize), replace=False)
    
            # either creates new sample or appends to it
            FullSample = pd.concat([FullSample, strataSamp])
            
            new_row = {'rule': x, 'pop' : pop, 'popsampsize': newSampSize}
            test = test.append(new_row, ignore_index=True)
    
          sampleSize[pop] = len(FullSample[FullSample['Population Group'] == pop])
    
        print(f'RuleID Sampled:  {x}')
        print(f'Pop Group Sampled: {pop}')
        print(f'Sample Size:  {len(FullSample[(FullSample["Rule ID"] == x) & (FullSample["Population Group"] == pop)])}')
        
  else:
     if ruleIDs[x] <= minSamp:
       dataToSample_Rule['StrataVal'] = 'NA'
 
       if FullSample.empty:
         FullSample = dataToSample_Rule
       else:
         FullSample = pd.concat([FullSample, dataToSample_Rule])
 
       sampleSize[x] = len(FullSample[FullSample['Rule ID'] == x])
     else:
       # filter file to specific rule, popgroup
       sampleFrame = dataToSample_Rule
       size = ruleIDs[x] # get's population of rule
       newSampSize = max(np.ceil(sampSize/(1+(sampSize-1)/size)), minSamp) # gets the new estimated sample size
 
 
       # Calculates Strata
       sampleFrame['StrataVal'] = '' 
       for i in paramUse: 
           strata_values = np.minimum(9, 0.1 * np.floor(sampleFrame[i].apply(lambda y : stats.percentileofscore(sampleFrame[i], y, kind = 'strict'))))
           sampleFrame['StrataVal'] += strata_values.astype(int).astype(str)
       
       # creates lookup table for strata indexing
       res = sampleFrame['StrataVal'].value_counts()
 
       # creates sample
       for a in res.index: 
 
         # filters to specific stratum
         strata = sampleFrame[sampleFrame['StrataVal'] == a]
 
         # finds sample size of stratum
         if(res[a] < 6):
             strataSampSize = res[a]
         elif(res[a]*newSampSize/size < 6):
             strataSampSize = 5
         else:
             strataSampSize = np.ceil(res[a]*newSampSize/size)
 
         # pulls the sample
         strataSamp = strata.sample(n=int(strataSampSize), replace=False)
 
         # either creates new sample or appends to it
         FullSample = pd.concat([FullSample, strataSamp])
         
         new_row = {'rule': x, 'sampsize': newSampSize}
         test = test.append(new_row, ignore_index=True)
 
       sampleSize[x] = len(FullSample[FullSample['Rule ID'] == x])
 
     print(f'RuleID Sampled:  {x}')
     print(f'Sample Size:  {len(FullSample[(FullSample["Rule ID"] == x)])}') 

  print(f'{len(FullSample["Rule ID"].unique())} rules have been sampled for a total of {len(FullSample)} alerts')
  FullSample.to_csv(fp + 'Python Sample.csv', index=False)
  test.to_csv(fp+'Python QC.csv', index=False)
  #sampleFrame.to_csv(fp+'Python test sampleFrame.csv',index=False)

