import os
import pandas as pd
import numpy as np
from IPython.display import clear_output

clean_files = sorted(['CleanTriggerFiles/' + f for f in os.listdir('CleanTriggerFiles') if f.endswith('.csv')])
dirty_files = sorted(['DirtyTriggerFiles/' + f for f in os.listdir('DirtyTriggerFiles') if f.endswith('.csv')])

Ls_clean = []
Ls_dirty = []
for file in clean_files:
    clear_output(wait = True)
    print(file)
    triggers = pd.read_csv(file)
    if 'P_Clean' in triggers.columns and 'P_Dirty' in triggers.columns:
        Ls_clean += list(np.log((triggers['P_Dirty']/triggers['P_Clean'])).values)
for file in dirty_files:
    clear_output(wait = True)
    print(file)
    triggers = pd.read_csv(file)
    if 'P_Clean' in triggers.columns and 'P_Dirty' in triggers.columns:
        Ls_dirty += list(np.log((triggers['P_Dirty']/triggers['P_Clean'])).values)
Ls = np.array(Ls_clean + Ls_dirty)
Ls_clean = np.array(Ls_clean)
Ls_dirty = np.array(Ls_dirty)

percentile_99 = np.percentile(Ls_clean[abs(Ls_clean) < np.inf], 99)

print(f"Estimated 99th percentile: {percentile_99}")

for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file)
    if 'P_Clean' in triggers.columns and 'P_Dirty' in triggers.columns:
        triggers['L_Dirty'] = np.log((triggers['P_Dirty']/triggers['P_Clean']))
    triggers_glitch = triggers[triggers['L_Dirty'] >= percentile_99]
    triggers_other = triggers[triggers['L_Dirty'] < percentile_99]
    print("Total Triggers: ", len(triggers), " | Glitch Triggers: ",len(triggers_glitch), ' | Other Triggers: ',len(triggers_other))
    triggers_glitch.to_csv(file.replace('DirtyTriggerFiles/', 'GlitchTriggerFiles/'))
    triggers_temp = pd.read_csv(file.replace('DirtyTriggerFiles/', 'OtherTriggerFiles/'))
    triggers_temp = pd.concat([triggers_temp, triggers_other])
    triggers_temp.to_csv(file.replace('DirtyTriggerFiles/', 'OtherTriggerFiles/'))

    

