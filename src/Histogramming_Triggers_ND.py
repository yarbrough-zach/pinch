import sqlite3
import numpy as np
import pandas as pd
import warnings
from os import listdir, system, environ
from IPython.display import clear_output
import os
import pickle as pkl
from scipy.stats import gaussian_kde

glitches_file = '/home/andre.guimaraes/public_html/gstlal/offline_analysis/background_investigation_gstlal_02/O3glitches.csv'

nBinsX = 200
nBinsY = 200

glitches = glitches = pd.read_csv(glitches_file)

# First we need to find the max and min of each bin space.
files = [f for f in os.listdir("TriggerFiles") if f.endswith(".csv")]
maxX = -np.inf
minX = np.inf
maxY = -np.inf
minY = np.inf
print("Reading Maxs and Mins before constructing bins.")
for file in sorted(files):
    clear_output(wait = True)
    triggers = pd.read_csv("TriggerFiles/" + file)
    triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
    maxX = max([maxX, max(triggers['snr'])])
    minX = min([minX, min(triggers['snr'])])
    maxY= max([maxY, max(triggers['chisqBysnrsq'])])
    minY = min([minY, min(triggers['chisqBysnrsq'])])
    print(minX, maxX, minY, maxY, file)
    
    
binsX = np.exp(np.linspace(np.log(0.9*minX), np.log(1.1*maxX), num = nBinsX, endpoint = True))
binsY = np.exp(np.linspace(np.log(0.9*minY), np.log(1.1*maxY), num = nBinsY, endpoint = True))

X, Y = np.meshgrid(binsX[:-1], binsY[:-1])

glitch_labels = np.unique(glitches['label'])

hists = {'H1':{g:np.zeros((nBinsX-1, nBinsY-1)) for g in glitch_labels},
         'L1':{g:np.zeros((nBinsX-1, nBinsY-1)) for g in glitch_labels}}

hists['H1']['None'] = np.zeros((nBinsX-1, nBinsY-1))
hists['L1']['None'] = np.zeros((nBinsX-1, nBinsY-1))


clean_files = sorted(["CleanTriggerFiles/" + f for f in os.listdir("CleanTriggerFiles") if f.endswith(".csv")])
dirty_files = sorted(["GlitchedTriggerFiles/" + f for f in os.listdir("GlitchedTriggerFiles") if f.endswith(".csv")])

# Populating Clean Histograms
print("Populating Clean Histograms")
for file in clean_files:
    print(file)
    triggers = pd.read_csv(file)
    triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
    triggersH1 = triggers[triggers['ifo'] == 'H1']
    triggersL1 = triggers[triggers['ifo'] == 'L1']
    
    histH1, xedges, yedges = np.histogram2d(triggersH1['snr'].values, triggersH1['chisqBysnrsq'].values, bins=[binsX, binsY])
    histL1, xedges, yedges = np.histogram2d(triggersH1['snr'].values, triggersH1['chisqBysnrsq'].values, bins=[binsX, binsY])
   
    hists['H1']['None'] += histH1
    hists['L1']['None'] += histL1

# Populating Dirty Histograms
print("Populating Dirty Histograms")
for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file)
    triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
    triggersH1 = triggers[triggers['ifo'] == 'H1']
    triggersL1 = triggers[triggers['ifo'] == 'L1']
    for label in glitch_labels:
        triggers_label_H1 = triggersH1[triggersH1['GlitchType'] == label]
        triggers_label_L1 = triggersL1[triggersL1['GlitchType'] == label]
        
        histH1, xedges, yedges = np.histogram2d(triggers_label_H1['snr'].values, triggers_label_H1['chisqBysnrsq'].values, bins=[binsX, binsY])
        histL1, xedges, yedges = np.histogram2d(triggers_label_L1['snr'].values, triggers_label_L1['chisqBysnrsq'].values, bins=[binsX, binsY])
        hists['H1'][label] += histH1
        hists['L1'][label] += histL1
        
print("Saving Histograms")
with open("histograms.pkl", 'wb') as f:
    pkl.dump((binsX, binsY, hists), f)


kde_bg_L1 = gaussian_kde((np.log(X.ravel()), np.log(Y.ravel())), weights = hists['L1']['None'].ravel())
kdes_L1 = {l:gaussian_kde((np.log(X.ravel()), np.log(Y.ravel())), weights = hists['L1'][l].ravel()) for l in glitch_labels}

kde_bg_H1 = gaussian_kde((np.log(X.ravel()), np.log(Y.ravel())), weights = hists['H1']['None'].ravel())
kdes_H1 = {l:gaussian_kde((np.log(X.ravel()), np.log(Y.ravel())), weights = hists['H1'][l].ravel()) for l in glitch_labels}

for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file)
    triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
    glitch_L = np.repeat(0., len(triggers))
    count = 0
    for idx, trigger in triggers.iterrows():
        x = np.log(trigger['snr'])
        y = np.log(trigger['chisqBysnrsq'])
        label = trigger['GlitchType']
        ifo = trigger['ifo']
        if ifo == 'H1':
            glitch_L[count] = kdes_H1[label]([x, y])[0]/kde_bg_H1([x, y])[0]
        elif ifo == 'L1':
            glitch_L[count] = kdes_L1[label]([x, y])[0]/kde_bg_L1([x, y])[0]
        else:
            glitch_L[count] = np.nan
        count += 1
    triggers['Glitch_L'] = glitch_L
    triggers.to_csv(file)
