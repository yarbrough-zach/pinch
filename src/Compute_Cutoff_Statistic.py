import numpy as np
import pandas as pd
import warnings
from os import listdir, system, environ
import os
import pickle as pkl
from scipy.stats import gaussian_kde
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
nBinsX = config['workflow']['cutoff']['number-binsX']
nBinsY = config['workflow']['cutoff']['number-binsY']
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

if machine_learning_cutoff:
    pass
else:
    log("Reading Glitch Files")
    glitches = pd.read_csv(gspy_file)
    glitches['glitch_id'] = glitches['id']
    
    log("Listing Raw Trigger Files for finding min and max")
    # First we need to find the max and min of each bin space.
    files = sorted([f for f in os.listdir(raw_trigger_dir) if f.endswith(".csv")])
    
    log("Initializing Mins and Maxs as infs")
    maxX = -np.inf
    minX = np.inf
    maxY = -np.inf
    minY = np.inf
    log("Looping through triggers to find mins and max")
    for file in sorted(files):
        print(file)
        triggers = pd.read_csv(raw_trigger_dir + file)
        triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
        maxX = max([maxX, max(triggers['snr'])])
        minX = min([minX, min(triggers['snr'])])
        maxY = max([maxY, max(triggers['chisqBysnrsq'])])
        minY = min([minY, min(triggers['chisqBysnrsq'])])
        print(minX, maxX, minY, maxY, file)
    
    log("Definint bins")
    binsX = np.exp(np.linspace(np.log(0.9*minX), np.log(1.1*maxX), num = nBinsX, endpoint = True))
    binsY = np.exp(np.linspace(np.log(0.9*minY), np.log(1.1*maxY), num = nBinsY, endpoint = True))

    log("Initializing Clean and Dirty Histograms")
    X, Y = np.meshgrid(binsX[:-1], binsY[:-1])
    hists = {'H1':{}, 'L1':{}}
    hists['H1']['Clean'] = np.zeros((nBinsX-1, nBinsY-1))
    hists['L1']['Clean'] = np.zeros((nBinsX-1, nBinsY-1))
    hists['H1']['Dirty'] = np.zeros((nBinsX-1, nBinsY-1))
    hists['L1']['Dirty'] = np.zeros((nBinsX-1, nBinsY-1))
    
    log("Listing clean, dirty, and other trigger files")
    clean_files = sorted([clean_trigger_dir + f for f in os.listdir(clean_trigger_dir) if f.endswith(".csv")])
    dirty_files = sorted([dirty_trigger_dir + f for f in os.listdir(dirty_trigger_dir) if f.endswith(".csv")])
    other_files = sorted([other_trigger_dir + f for f in os.listdir(other_trigger_dir) if f.endswith(".csv")])
    
    log("Populating Clean Histograms")
    for file in clean_files:
        print(file)
        triggers = pd.read_csv(file)
        triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
        triggersH1 = triggers[triggers['ifo'] == 'H1']
        triggersL1 = triggers[triggers['ifo'] == 'L1']

        histH1, xedges, yedges = np.histogram2d(triggersH1['snr'].values, triggersH1['chisqBysnrsq'].values, bins=[binsX, binsY])
        histL1, xedges, yedges = np.histogram2d(triggersL1['snr'].values, triggersL1['chisqBysnrsq'].values, bins=[binsX, binsY])

        hists['H1']['Clean'] += histH1
        hists['L1']['Clean'] += histL1
    
    log("Populating Dirty Histograms")
    for file in dirty_files:
        print(file)
        triggers = pd.read_csv(file)
        triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
        triggersH1 = triggers[triggers['ifo'] == 'H1']
        triggersL1 = triggers[triggers['ifo'] == 'L1']

        histH1, xedges, yedges = np.histogram2d(triggersH1['snr'].values, triggersH1['chisqBysnrsq'].values, bins=[binsX, binsY])
        histL1, xedges, yedges = np.histogram2d(triggersL1['snr'].values, triggersL1['chisqBysnrsq'].values, bins=[binsX, binsY])

        hists['H1']['Dirty'] += histH1
        hists['L1']['Dirty'] += histL1
    
    log("Saving Histograms")
    with open(temp_dir + tag + "-histograms.pkl", 'wb') as f:
        pkl.dump((binsX, binsY, hists), f)
        
    log("Defining mid-bins for kde evaluation")
    xs = np.log(np.array([0.5*(binsX[i] + binsX[i+1]) for i in range(binsX.shape[0]-1)]))
    ys = np.log(np.array([0.5*(binsY[i] + binsY[i+1]) for i in range(binsY.shape[0]-1)]))
    
    log("Creating KDEs")
    X, Y = np.meshgrid(xs, ys)
    kdes = {'H1':{}, 'L1':{}}
    for ifo in ['H1', 'L1']:
        for label in ['Clean', 'Dirty']:
            kdes[ifo][label] = gaussian_kde([X.ravel(), Y.ravel()], weights = hists[ifo][label].T.ravel())
    
    allFiles = clean_files + dirty_files + other_files
    log("Looping Thorugh All Files")
    for file in allFiles:
        log("Reading file: " + file)
        triggers = pd.read_csv(file)
        
        Ps = {label:np.zeros(len(triggers)) for label in ['Clean', 'Dirty']}

        triggers['log_snr'] = np.log(triggers['snr'])
        triggers['log_chisqBysnrsq'] = np.log(triggers['chisq']/triggers['snr']**2)

        for ifo in ['H1', 'L1']:
            triggersTemp = triggers[(triggers['ifo'] == ifo)]
            
            Ps['Clean'][triggersTemp.index.values] = kdes[ifo]['Clean']([triggersTemp['log_snr'].values, triggersTemp['log_chisqBysnrsq'].values])
            Ps['Dirty'][triggersTemp.index.values] = kdes[ifo]['Clean']([triggersTemp['log_snr'].values, triggersTemp['log_chisqBysnrsq'].values])
            
        triggers['P_Clean'] = Ps['Clean']
        triggers['P_Dirty'] = Ps['Dirty']
        
        triggers['VSV'] = np.log(triggers['P_Dirty']/triggers['P_Clean'])
        
        triggers.to_csv(file)

