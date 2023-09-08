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

def formula(triggers, param):
    if param == 'log_mtotal':
        return np.log(triggers['mass1'] + triggers['mass2'])
    elif param == 'log_snr':
        return np.log(triggers['snr'])
    elif param == 'chisqBysnrsq':
        return triggers['chisq']/triggers['snr']**2
    elif param == 'log_chisqBysnrsq':
        return np.log(triggers['chisq']/triggers['snr']**2)
    elif param == 'log_bankchisqBysnrsq':
        return np.log(triggers['bank_chisq']/triggers['snr']**2)
    elif param == 'log_q':
        return np.log(triggers['mass1']/triggers['mass2'])
    elif param == 'log_bank_chisq':
        return np.log(triggers['bank_chisq'])
    elif param == 'log_sigmasq':
        return np.log(triggers['sigmasq'])
    elif param == 'log_template_duration':
        return np.log(triggers['template_duration'])
    elif param == 'log_mass1':
        return np.log(triggers['mass1'])
    elif param == 'log_mass2':
        return np.log(triggers['mass2'])
    
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
omic_file = config['glitches']['omic-file']

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

log("Starting Up Compute_Cutoff_Statistic.py")

log("Listing clean, dirty, and other trigger files")
raw_files = sorted([raw_trigger_dir + f for f in os.listdir(raw_trigger_dir) if f.endswith('.csv')])[:1]
clean_files = sorted([clean_trigger_dir + f for f in os.listdir(clean_trigger_dir) if f.endswith(".csv")])[:1]
dirty_files = sorted([dirty_trigger_dir + f for f in os.listdir(dirty_trigger_dir) if f.endswith(".csv")])[:1]
other_files = sorted([other_trigger_dir + f for f in os.listdir(other_trigger_dir) if f.endswith(".csv")])[:1]
allFiles = clean_files + dirty_files + other_files

if machine_learning_cutoff:
    log("Using Machine Learning for cutoff")
    
    log("Creating Clean Set")
    nClean = int(machine_learning_samples/len(clean_files))
    set_clean = None
    for file in clean_files:
        print(file)
        triggers = pd.read_csv(file, index_col = 0)
        for param in cutoff_params:
            if param not in triggers.columns:
                triggers[param] = formula(triggers, param)
        if type(set_clean) == type(None):
            set_clean = triggers[cutoff_params].sample(nClean).values
        else:
            set_clean = np.append(set_clean, triggers[cutoff_params].sample(nClean).values, axis = 0)
    
    log('Scaling data and fitting scaler')
    scaler = StandardScaler()
    scaled_clean = scaler.fit_transform(set_clean)
    log("Training SVM")
    clf = OneClassSVM(kernel="rbf", nu=0.1).fit(scaled_clean)
    log("Computing Cutoff Statistic for All files")
    for file in allFiles:
        print(file)
        triggers = pd.read_csv(file, index_col = 0)
        for param in cutoff_params:
            if param not in triggers.columns:
                triggers[param] = formula(triggers, param)
        set_clean = triggers[cutoff_params].values
        scaled_clean = scaler.transform(set_clean)
        triggers['VSV'] = -clf.decision_function(scaled_clean)
        triggers.to_csv(file)    
    
else:
    log("No Machine Learning used for cutoff")
    
    log("Initializing Mins and Maxs as infs")
    mins = [np.inf for _ in range(len(cutoff_params))]
    maxs = [-np.inf for _ in range(len(cutoff_params))]
    log("Looping through triggers to find mins and max")
    for file in sorted(raw_files):
        print(file)
        triggers = pd.read_csv(file, index_col = 0)
        for i, param in enumerate(cutoff_params):
            if param not in triggers.columns:
                triggers[param] = formula(triggers, param)
            maxs[i] = max([maxs[i], max(triggers[param])])
            mins[i] = min([mins[i], min(triggers[param])])
    
    log("Defining histogram bins")
    bins = np.array([np.linspace(0.9*MIN, 1.1*MAX, num = nBins, endpoint = True) for MIN, MAX in zip(mins, maxs)])
    bins_reduced = np.array([b[:-1] for b in bins])
    log("Initializing Clean and Dirty Histograms")
    Xs = np.array(np.meshgrid(*bins_reduced))
    hists = {'H1':{}, 'L1':{}}
    hists['H1']['Clean'] = np.zeros([nBins-1]*len(cutoff_params))
    hists['L1']['Clean'] = np.zeros([nBins-1]*len(cutoff_params))
    hists['H1']['Dirty'] = np.zeros([nBins-1]*len(cutoff_params))
    hists['L1']['Dirty'] = np.zeros([nBins-1]*len(cutoff_params))
    
    log("Populating Clean Histograms")
    for file in clean_files:
        print(file)
        triggers = pd.read_csv(file, index_col = 0)
        for param in cutoff_params:
            if param not in triggers.columns:
                triggers[param] = formula(triggers, param)
        triggersH1 = triggers[triggers['ifo'] == 'H1']
        triggersL1 = triggers[triggers['ifo'] == 'L1']
        
        histH1, edges = np.histogramdd(triggersH1[cutoff_params].values, bins=bins)
        histL1, edges = np.histogramdd(triggersL1[cutoff_params].values, bins=bins)
        
        hists['H1']['Clean'] += histH1
        hists['L1']['Clean'] += histL1
    
    log("Populating Dirty Histograms")
    for file in dirty_files:
        triggers = pd.read_csv(file, index_col = 0)
        for param in cutoff_params:
            if param not in triggers.columns:
                triggers[param] = formula(triggers, param)
        triggersH1 = triggers[triggers['ifo'] == 'H1']
        triggersL1 = triggers[triggers['ifo'] == 'L1']
        histH1, edges = np.histogramdd(triggersH1[cutoff_params].values, bins=bins)
        histL1, edges = np.histogramdd(triggersL1[cutoff_params].values, bins=bins)

        hists['H1']['Dirty'] += histH1
        hists['L1']['Dirty'] += histL1
    
    log("Saving Histograms")
    with open(temp_dir + tag + "-histograms.pkl", 'wb') as f:
        pkl.dump((edges, hists), f)
        
    log("Defining mid-bins for kde evaluation")
    xs = np.array([np.array([0.5*(b[i] + b[i+1]) for i in range(b.shape[0]-1)]) for b in bins])
    
    log("Creating KDEs")
    X = np.meshgrid(*xs)
    ravels = [x.ravel() for x in X]
    kdes = {'H1':{}, 'L1':{}}
    for ifo in ['H1', 'L1']:
        for label in ['Clean', 'Dirty']:
            kdes[ifo][label] = gaussian_kde(ravels, weights = hists[ifo][label].T.ravel())
    
    log("Looping Thorugh All Files and computing the VSV")
    for file in allFiles:
        log("Reading file: " + file)
        triggers = pd.read_csv(file, index_col = 0)
        for param in cutoff_params:
            if param not in triggers.columns:
                triggers[param] = formula(triggers, param)
                
        Ps = {label:np.zeros(len(triggers)) for label in ['Clean', 'Dirty']}

        for ifo in ['H1', 'L1']:
            triggersTemp = triggers[(triggers['ifo'] == ifo)]
            
            Ps['Clean'][triggersTemp.index.values] = kdes[ifo]['Clean'](triggersTemp[cutoff_params].values.T)
            Ps['Dirty'][triggersTemp.index.values] = kdes[ifo]['Dirty'](triggersTemp[cutoff_params].values.T)
            
        triggers['P_Clean'] = Ps['Clean']
        triggers['P_Dirty'] = Ps['Dirty']
        
        triggers['VSV'] = np.log(triggers['P_Dirty']/triggers['P_Clean'])
        
        triggers.to_csv(file)

log("Done with Compute_Cutoff_Statistic.py")
