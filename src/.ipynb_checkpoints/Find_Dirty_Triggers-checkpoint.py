import numpy as np
import pandas as pd
import warnings
from os import listdir, system, environ
import os
import yaml
import datetime

def log(text):
    if log_q:
        date = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        print(date + " : " + text)
        with open(log_dir + tag + '.log', 'a') as f:
            f.write(date + " : " + text + "\n")
# Ignoring some pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
environ["GSTLAL_FIR_WHITEN"] = '0'

print("Loading Config File")
with open('config.yml', 'r') as file:
    try:
        config = yaml.safe_load(file)
    except yaml.YAMLError as exc:
        print(exc)

log_q = config['workflow']['other']['log']    

print("Loading Glitches Configurations")
gspy_file = config['glitches']['gspy-file']
omic_file = config['glitches']['gspy-file']

print("Loading Triggers Configurations")
pipeline = config['triggers']['pipeline']
raw_trigger_dir = config['triggers']['raw-trigger-dir']
clean_trigger_dir = config['triggers']['clean-trigger-dir']
dirty_trigger_dir = config['triggers']['dirty-trigger-dir']
other_trigger_dir = config['triggers']['other-trigger-dir']

print("Loading plots Configurations")
plots_dir = config['plots']['plots-dir']

print("Loading workflow Configurations")
omicron_snr_cutoff = config['workflow']['trigger-finding']['omicron-snr-cutoff']
percentile_cutoff = config['workflow']['cutoff']['percentile-cutoff']
machine_learning_cutoff = config['workflow']['cutoff']['machine-learning-cutoff']
machine_learning_cat = config['workflow']['categorization']['machine-learning-cat']
temp_dir = config['workflow']['other']['temp-dir']
log_dir = config['workflow']['other']['log-dir']
tag = config['workflow']['other']['tag']

print("Fixing File Paths by adding a '/' in the end if necessary")

if not raw_trigger_dir.endswith('/'):
    raw_trigger_dir += '/'
if not clean_trigger_dir.endswith('/'):
    clean_trigger_dir += '/'
if not dirty_trigger_dir.endswith('/'):
    dirty_trigger_dir += '/'
if not other_trigger_dir.endswith('/'):
    other_trigger_dir += '/'
if not plots_dir.endswith('/'):
    plots_dir += '/'
if not temp_dir.endswith('/'):
    temp_dir += '/'
if not log_dir.endswith('/'):
    log_dir += '/'

if not os.path.exists(clean_trigger_dir):
    os.makedirs(clean_trigger_dir)
if not os.path.exists(dirty_trigger_dir):
    os.makedirs(dirty_trigger_dir)
if not os.path.exists(other_trigger_dir):
    os.makedirs(other_trigger_dir)
if not os.path.exists(plots_dir):
    os.makedirs(plots_dir)
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log("Starting Up")

log("Sorting out Trigger Files")
trigger_files = sorted([raw_trigger_dir + f for f in os.listdir(raw_trigger_dir) if f.endswith('.csv')])

log("Reading Gspy File")
glitches = pd.read_csv(gspy_file, index_col = 0)

log("Reading Omic File")
omics = pd.read_csv(omic_file, index_col = 0)

log("Applying cut on Omic triggers")
if type(omicron_snr_cutoff) == type(None):
    omicron_snr_cutoff = min(glitches['snr'])
omics = omics[omics['snr'] > omicron_snr_cutoff]

log("Defining glitches 'tstart' and 'tend'")
glitches['tstart'] = glitches['GPStime'] - glitches['duration']/2
glitches['tend'] = glitches['GPStime'] + glitches['duration']/2

log("Defining omics 'tstart' and 'tend'")
omics['tstart'] = omics['GPStime'] - omics['duration']/2
omics['tend'] = omics['GPStime'] + omics['duration']/2

log("Looping Through triggers")
for trigger_file in trigger_files:
    log("Reading Trigger Files: " + trigger_file)
    triggers = pd.read_csv(trigger_file, index_col = 0)
    log("Defining triggers GPStime and start_time")
    triggers['GPStime'] = triggers['end_time'] + 1e-9*triggers['end_time_ns']
    triggers['start_time'] = triggers['GPStime'] - triggers['template_duration']
    
    log("Creating Temporary Arrays for glitchIDs and omicIDs")
    glitchIDs = ['None']*len(triggers)
    omicIDs = [-1]*len(triggers)
    
    log("Creating mask for glitches and omicron so we only looks at ones within the chunk")
    glitch_time_mask = (glitches['tend'] >= min(triggers['start_time'])) & (glitches['tstart'] <= max(triggers['GPStime']))
    omic_time_mask = (omics['tend'] >= min(triggers['GPStime'])) & (omics['tstart'] <= max(triggers['GPStime']))
    
    log("Applying masks to glitches and omic into temporary variables")
    glitches_temp = glitches[glitch_time_mask]
    omics_temp = omics[omic_time_mask]

    log("Creating masks for IFOs")
    ifo_mask = {'H1':triggers['ifo'] == 'H1', 'L1':triggers['ifo'] == 'L1'}
    
    log("Looping through GravitySpy glitches")
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
        
    log("Looping through Omicron triggers")
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
    
    log("Assigning Glitch and Omicron IDs")
    triggers['glitch_id'] = glitchIDs
    triggers['omic_id'] = omicIDs
    
    log("Creating masks for different categories")
    glitches['glitch_id'] = glitches['id']
    mask_glitch = (triggers['glitch_id'] != 'None')
    mask_other = (triggers['glitch_id'] == 'None') & (triggers['omic_id'] == 1)
    mask_clean = (triggers['glitch_id'] == 'None') & (triggers['omic_id'] == -1)
    
    log("Creating masked dataframes")
    triggers_dirty = triggers[mask_glitch].copy()
    triggers_other = triggers[mask_other].copy()
    triggers_clean = triggers[mask_clean].copy()

    log("Assigning gspy confidence to all the dirty triggers")
    triggers_dirty = triggers_dirty.merge(glitches[['glitch_id','confidence']], on='glitch_id', how='left')
    log("Throwing low-confidence triggers into other triggers")
    triggers_other = pd.concat([triggers_other, triggers_dirty[triggers_dirty['confidence'] < 0.9]])
    log("Redefinint dirty triggers with high confidence only")
    triggers_dirty = triggers_dirty[triggers_dirty['confidence'] >= 0.9]
    
    log("Original Number of Triggers: " + str(len(triggers)))
    log("Number of Clean Triggers Found: " + str(len(triggers_clean)))
    log("Number of Dirty Triggers Found: " + str(len(triggers_dirty)))
    log("Number of Other Triggers Found: " + str(len(triggers_other)))
    
    log("Saving the CSV for eacht rigger")
    triggers_clean.to_csv(trigger_file.replace(raw_trigger_dir, clean_trigger_dir))
    triggers_dirty.to_csv(trigger_file.replace(raw_trigger_dir, dirty_trigger_dir))
    triggers_other.to_csv(trigger_file.replace(raw_trigger_dir, other_trigger_dir))

log("Done!")
