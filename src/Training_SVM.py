from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
import pickle as pkl

def bestbandwidth(data):
    return 1.06*np.std(data)*len(data)**(-1/5)

dirty_files = ['DirtyTriggerFiles/' + f for f in os.listdir('DirtyTriggerFiles') if f.endswith('.csv')]
clean_files = ['CleanTriggerFiles/' + f for f in os.listdir('CleanTriggerFiles') if f.endswith('.csv')]

set_dirty = None
set_clean = None

params = ['log_snr','log_chisqBysnrsq']#,'log_mtotal', 'log_bank_chisq', 'log_q', 'log_mass1', 'log_mass2', 'log_sigmasq', 'log_template_duration']
def formula(triggers, param):
    if param == 'log_mtotal':
        return np.log(triggers['mass1'] + triggers['mass2'])
    elif param == 'log_snr':
        return np.log(triggers['snr'])
    elif param == 'chisqBysnrsq':
        return triggers['chisq']/triggers['snr']**2
    elif param == 'log_chisqBysnrsq':
        return np.log(triggers['chisq']/triggers['snr']**2)
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

labels_clean = []
print("Creating Clean Set")
for file in clean_files:
    print(file)
    triggers = pd.read_csv(file)
    for param in params:
        if param not in triggers.columns:
            triggers[param] = formula(triggers, param)
    if type(set_clean) == type(None):
        set_clean = triggers[params].values
    else:
        set_clean = np.append(set_clean, triggers[params].values, axis = 0)
    labels_clean += [file + '-' + str(i) for i in range(len(triggers))]

labels_dirty = []
print("Creating Dirty Set")
for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file)
    for param in params:
        if param not in triggers.columns:
            triggers[param] = formula(triggers, param)
    if type(set_dirty) == type(None):
        set_dirty = triggers[params].values
    else:
        set_dirty = np.append(set_dirty, triggers[params].values, axis = 0)
    labels_dirty += [file + '-' + str(i) for i in range(len(triggers))]
    
scaler = StandardScaler()
scaled_clean = scaler.fit_transform(set_clean)
scaled_dirty = scaler.transform(set_dirty)

count = 0
distances_clean = np.repeat(0., len(set_clean))
distances_dirty = np.repeat(0., len(set_dirty))

step = int(1e4)
    
# Train a OneClassSVM
print("Training SVM")
sample_idx = np.random.choice(np.array(range(len(scaled_clean))), size = 100000, replace = False)
clf = OneClassSVM(kernel="rbf", nu=0.1).fit(scaled_clean[sample_idx])
print("Evaluating Distances, Clean")
distances_clean = clf.decision_function(scaled_clean)
print("Evaluating Distances, Dirty")
distances_dirty = clf.decision_function(scaled_dirty)

print("Saving")
with open('distances_clean.pkl', 'wb') as f:
    pkl.dump(np.vstack([labels_clean, distances_clean]).T, f)
with open('distances_dirty.pkl', 'wb') as f:
    pkl.dump(np.vstack([labels_dirty, distances_dirty]).T, f)