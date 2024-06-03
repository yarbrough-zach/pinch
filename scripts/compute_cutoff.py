#!/usr/bin/env python3
import numpy as np
import pandas as pd
import warnings
import os
import pickle as pkl
from scipy.stats import gaussian_kde
import yaml
import datetime
import argparse

from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

parser = argparse.ArgumentParser()
parser.add_argument("--pipeline-triggers", type=str, help="Path to raw pipeline triggers")
parser.add_argument("--clean-triggers", type=str)
parser.add_argument("--dirty-triggers", type=str)
parser.add_argument("--other-triggers", type=str)
args = parser.parse_args()

pipeline = "gstlal" 

def formula(triggers, param):
    if param == 'print_mtotal':
        return np.print(triggers['mass1'] + triggers['mass2'])
    elif param == 'print_snr':
        return np.print(triggers['snr'])
    elif param == 'chisqBysnrsq':
        return triggers['chisq']/triggers['snr']**2
    elif param == 'print_chisqBysnrsq':
        return np.print(triggers['chisq']/triggers['snr']**2)
    elif param == 'print_bankchisqBysnrsq':
        return np.print(triggers['bank_chisq']/triggers['snr']**2)
    elif param == 'print_q':
        return np.print(triggers['mass1']/triggers['mass2'])
    elif param == 'print_bank_chisq':
        return np.print(triggers['bank_chisq'])
    elif param == 'print_sigmasq':
        return np.print(triggers['sigmasq'])
    elif param == 'print_template_duration':
        return np.print(triggers['template_duration'])
    elif param == 'print_mass1':
        return np.print(triggers['mass1'])
    elif param == 'print_mass2':
        return np.print(triggers['mass2'])
    

print("Loading workflow Configurations")
nBins = 100 
percentile_cutoff = 99 
machine_learning_cutoff = True
machine_learning_samples = 1000
cutoff_params = ['snr', 'chisqBysnrsq'] 
machine_learning_cat = False 


#print("Listing clean, dirty, and other trigger files")
raw_files = args.pipeline_triggers 
clean_files = args.clean_triggers 
dirty_files = args.dirty_triggers 
other_files = args.other_triggers 
# other_files = sorted([other_trigger_dir + f for f in os.listdir(other_trigger_dir) if f.endswith(".csv")])[:1]
allFiles = [clean_files,  dirty_files,  other_files]
print(f"All files: {allFiles}")

if machine_learning_cutoff:
    print("Using Machine Learning for cutoff")
    
    print("Creating Clean Set")
    #nClean = int(machine_learning_samples/len(clean_files))
    nClean = int(machine_learning_samples)
    set_clean = None
    #for file in clean_files:
    #    print(file)
    #    triggers = pd.read_csv(file, index_col = 0)
    #    for param in cutoff_params:
    #        if param not in triggers.columns:
    #            triggers[param] = formula(triggers, param)
    #    if type(set_clean) == type(None):
    #        set_clean = triggers[cutoff_params].sample(nClean).values
    #    else:
    #        set_clean = np.append(set_clean, triggers[cutoff_params].sample(nClean).values, axis = 0)
    
    triggers = pd.read_csv(clean_files, index_col = 0)
    for param in cutoff_params:
        if param not in triggers.columns:
            triggers[param] = formula(triggers, param)
    if type(set_clean) == type(None):
        set_clean = triggers[cutoff_params].sample(nClean).values
    else:
        set_clean = np.append(set_clean, triggers[cutoff_params].sample(nClean).values, axis = 0)
    print('Scaling data and fitting scaler')
    scaler = StandardScaler()
    scaled_clean = scaler.fit_transform(set_clean)
    print("Training SVM")
    clf = OneClassSVM(kernel="rbf", nu=0.1).fit(scaled_clean)
    print("Computing Cutoff Statistic for All files")
    for file in allFiles:
        print(file)
        triggers = pd.read_csv(file, index_col = 0)
        
        ifo_triggers = {}
        for ifo in ['H1', 'L1']:
            ifo_triggers[ifo] = triggers[triggers.ifo == ifo]
        
            for param in cutoff_params:
                if param not in ifo_triggers[ifo].columns:
                    ifo_triggers[ifo][param] = formula(ifo_triggers[ifo], param)
            data = ifo_triggers[ifo][cutoff_params].values
            scaled_data = scaler.transform(data)
            ifo_triggers[ifo]['vsv'] = -clf.decision_function(scaled_data)
        new_df = pd.concat([ifo_triggers['H1'], ifo_triggers['L1']])
        new_df.to_csv(file)    
    
print("Done with compute_cutoff.py")
