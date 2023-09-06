import pickle as pkl
import os
import numpy as np
from scipy.stats import gaussian_kde
import matplotlib.pyplot as plt
import pandas as pd
import random

dirty_files = sorted(['DirtyTriggerFiles/' + f for f in os.listdir('DirtyTriggerFiles') if f.endswith('.csv')])
clean_files = sorted(['CleanTriggerFiles/' + f for f in os.listdir('CleanTriggerFiles') if f.endswith('.csv')])
other_files = sorted(['OtherTriggerFiles/' + f for f in os.listdir('OtherTriggerFiles') if f.endswith('.csv')])

with open('histograms.pkl', 'rb') as f:
    xBins, yBins, hists = pkl.load(f)
    
glitches = pd.read_csv('../../background_investigation_gstlal_02/O3glitches.csv')
glitches['glitch_id'] = glitches['id']
xs = np.log(np.array([0.5*(xBins[i] + xBins[i+1]) for i in range(xBins.shape[0]-1)]))
ys = np.log(np.array([0.5*(yBins[i] + yBins[i+1]) for i in range(yBins.shape[0]-1)]))

X, Y = np.meshgrid(xs, ys)

kdes = {'H1':{}, 'L1':{}}
for ifo in ['H1', 'L1']:
    for label in hists[ifo].keys():
        kdes[ifo][label] = gaussian_kde([X.ravel(), Y.ravel()], weights = hists[ifo][label].T.ravel())
        
allFiles = dirty_files + clean_files + other_files
random.shuffle(allFiles)

for file in allFiles:
    print(file)
    triggers = pd.read_csv(file)
    
    Ps = {label:np.zeros(len(triggers)) for label in hists['H1'].keys()}
    
    glitches['glitch_id'] = glitches['id']
    if 'DirtyTriggerFiles' in file:
        triggers = triggers.merge(glitches[['glitch_id','label']], on='glitch_id', how='left')
    elif 'CleanTriggerFiles' in file:
        triggers['label'] = ['Clean']*len(triggers)
    elif 'OtherTriggerFiles' in file:
        triggers['label'] = ['Other']*len(triggers)
    
    triggers['log_snr'] = np.log(triggers['snr'])
    triggers['log_chisqBysnrsq'] = np.log(triggers['chisq']/triggers['snr']**2)
    
    allLabels = ['Clean', 'Dirty']
    
    for label in allLabels:
        print(label)
        for ifo in ['H1', 'L1']:
            triggersTemp = triggers[(triggers['ifo'] == ifo)]
            Ps[label][triggersTemp.index.values] = kdes[ifo][label]([triggersTemp['log_snr'].values, triggersTemp['log_chisqBysnrsq'].values])
        triggers['P_' + label] = Ps[label]
    
    triggers.to_csv(file)
