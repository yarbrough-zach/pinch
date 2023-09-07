import numpy as np
import pandas as pd
import warnings
from os import listdir, system, environ
import os
import pickle as pkl
from scipy.stats import gaussian_kde
import yaml
import datetime
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

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
glitch_trigger_dir = config['triggers']['glitch-trigger-dir']

print("Loading plots Configurations")
plots_dir = config['plots']['plots-dir']

print("Loading workflow Configurations")
omicron_snr_cutoff = config['workflow']['trigger-finding']['omicron-snr-cutoff']
nBins = config['workflow']['cutoff']['number-bins']
percentile_cutoff = config['workflow']['cutoff']['percentile-cutoff']
machine_learning_cutoff = config['workflow']['cutoff']['machine-learning-cutoff']
machine_learning_samples = config['workflow']['cutoff']['machine-learning-samples']
cutoff_params = config['workflow']['cutoff']['cutoff-params']
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
if not glitch_trigger_dir.endswith('/'):
    glitch_trigger_dir += '/'
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
if not os.path.exists(glitch_trigger_dir):
    os.makedirs(glitch_trigger_dir)
if not os.path.exists(plots_dir):
    os.makedirs(plots_dir)
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log("Starting Up Apply_Cutoff.py")

log("Listing clean, dirty, and other trigger files")
raw_files = sorted([raw_trigger_dir + f for f in os.listdir(raw_trigger_dir) if f.endswith('.csv')])
clean_files = sorted([clean_trigger_dir + f for f in os.listdir(clean_trigger_dir) if f.endswith(".csv")])
dirty_files = sorted([dirty_trigger_dir + f for f in os.listdir(dirty_trigger_dir) if f.endswith(".csv")])
other_files = sorted([other_trigger_dir + f for f in os.listdir(other_trigger_dir) if f.endswith(".csv")])

log('Loading the VSV for all the clean files')
VSV_clean = None
for file in clean_files:
    print(file)
    triggers = pd.read_csv(file, index_col = 0)
    if type(VSV_clean) == type(None):
        VSV_clean = triggers['VSV'].value
    else:
        VSV_clean = np.append(VSV_clean, triggers['VSV'].value, axis = 0)
log("Calculating threshold based on percentile cutoff")
threshold = np.percentile(VSV_clean, percentile_cutoff)

log("Applying the cutoff to all dirty files.")
for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file, index_col = 0)
    clean_temp = triggers[triggers['VSV'] <= threshold]
    glitch_temp = triggers[triggers['VSV'] > threshold]
    
    glitch_temp.to_csv(file.replace(dirty_trigger_dir, glitch_trigger_dir))
    
    clean_triggers = pd.read_csv(file.replace(dirty_trigger_dir, clean_trigger_dir), index_col = 0)
    clean_triggers['phase'] = np.repeat(0, len(clean_triggers))
    clean_temp['phase'] = np.repeat(1, len(clean_temp))
    
    clean_triggers = pd.concat([clean_triggers, clean_temp])
    clean_triggers.to_csv(file.replace(dirty_trigger_dir, clean_trigger_dir))

log('Done with Apply_Cutoff.py')