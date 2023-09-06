import sqlite3
import numpy as np
import pandas as pd
import warnings
from os import listdir, system, environ
from IPython.display import clear_output
import os

# Ignoring some pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
environ["GSTLAL_FIR_WHITEN"] = '0'

omic_file = 'omicron.csv'

glitches_file = '/home/andre.guimaraes/public_html/gstlal/offline_analysis/background_investigation_gstlal_02/O3glitches.csv'

trigger_file_dir = 'TriggerFiles'
trigger_files = sorted([trigger_file_dir + "/" + f for f in os.listdir(trigger_file_dir) if f.endswith('.csv')])

print("Reading Glitches File")
glitches = pd.read_csv(glitches_file)
omics = pd.read_csv(omic_file)
omics = omics[omics['snr'] > 6]
glitches['tstart'] = glitches['GPStime'] - glitches['duration']
glitches['tend'] = glitches['GPStime'] + glitches['duration']

for trigger_file in trigger_files:
    clear_output(wait = True)
    
    print("Reading Trigger Files: " + trigger_file)
    triggers = pd.read_csv(trigger_file)
    triggers['GPStime'] = triggers['end_time'] + 1e-9*triggers['end_time_ns']
    triggers['start_time'] = triggers['GPStime'] - triggers['template_duration']
    
    print("Creating Temporary Arrays")
    glitchIDs = ['None']*len(triggers)
    omicIDs = [-1]*len(triggers)
    glitch_time_mask = (glitches['tend'] >= min(triggers['GPStime'])) & (glitches['tstart'] <= max(triggers['GPStime']))
    omic_time_mask = (omics['tend'] >= min(triggers['GPStime'])) & (omics['tstart'] <= max(triggers['GPStime']))
    
    glitches_temp = glitches[glitch_time_mask]
    omics_temp = omics[omic_time_mask]

    ifo_mask = {'H1':triggers['ifo'] == 'H1', 'L1':triggers['ifo'] == 'L1'}
    count = 0
    for i, glitch in glitches_temp.iterrows():
        ifo = glitch['ifo']
        if count % 1000 == 0:
            print(str(count) + " / " + str(len(glitches_temp)))
        triggers_in_glitch_mask = (triggers['start_time'] <= glitch["tend"]) & (triggers['GPStime'] >= glitch["tstart"])
        triggers_in_glitch = triggers[triggers_in_glitch_mask & ifo_mask[ifo]]
        indexes = triggers_in_glitch.index
        for idx in indexes:
            glitchIDs[idx] = glitch['id']
        count += 1
    count = 0
    
    for i, omic in omics_temp.iterrows():
        ifo = omic['ifo']
        if count % 1000 == 0:
            print(str(count) + " / " + str(len(omics_temp)))
        triggers_in_omic_mask = (triggers['start_time'] <= omic["tend"]) & (triggers['GPStime'] >= omic["tstart"])
        triggers_in_omic = triggers[triggers_in_omic_mask & ifo_mask[ifo]]
        indexes = triggers_in_omic.index
        for idx in indexes:
            omicIDs[idx] = omic['id']
        count += 1
    
    print("Assigning Glitch and Omicron IDs")
    triggers['glitch_id'] = glitchIDs
    triggers['omic_id'] = omicIDs
    
    print("Saving CSVs")
    
    glitches['glitch_id'] = glitches['id']
    mask_glitch = (triggers['glitch_id'] != 'None')
    mask_other = (triggers['glitch_id'] == 'None') & (triggers['omic_id'] == 1)
    mask_clean = (triggers['glitch_id'] == 'None') & (triggers['omic_id'] == -1)
    
    triggers_dirty = triggers[mask_glitch].copy()
    triggers_other = triggers[mask_other].copy()
    triggers_clean = triggers[mask_clean].copy()
    
    triggers_dirty = triggers_dirty.merge(glitches[['glitch_id','confidence']], on='glitch_id', how='left')
    triggers_other = pd.concat([triggers_other, triggers_dirty[triggers_dirty['confidence'] < 0.9]])
    triggers_dirty = triggers_dirty[triggers_dirty['confidence'] >= 0.9]
    
    print("Original Number of Triggers: " + str(len(triggers)))
    print("Number of Clean Triggers Found: " + str(len(triggers_clean)))
    print("Number of Dirty Triggers Found: " + str(len(triggers_dirty)))
    print("Number of Other Triggers Found: " + str(len(triggers_other)))
    
    triggers_clean.to_csv("CleanTriggerFiles/" + trigger_file.split('/')[-1])
    triggers_dirty.to_csv("DirtyTriggerFiles/" + trigger_file.split('/')[-1])
    triggers_other.to_csv("OtherTriggerFiles/" + trigger_file.split('/')[-1])
