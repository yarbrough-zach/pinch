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

glitches = pd.read_csv(glitches_file)
glitches['glitch_id'] = glitches['id']
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

hists['H1']['Clean'] = np.zeros((nBinsX-1, nBinsY-1))
hists['L1']['Clean'] = np.zeros((nBinsX-1, nBinsY-1))

clean_files = sorted(["CleanTriggerFiles/" + f for f in os.listdir("CleanTriggerFiles") if f.endswith(".csv")])
dirty_files = sorted(["DirtyTriggerFiles/" + f for f in os.listdir("DirtyTriggerFiles") if f.endswith(".csv")])

# Populating Clean Histograms
print("Populating Clean Histograms")
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

# Populating Dirty Histograms
print("Populating Dirty Histograms")

hists['H1']['Dirty'] = np.zeros((nBinsX-1, nBinsY-1))
hists['L1']['Dirty'] = np.zeros((nBinsX-1, nBinsY-1))

for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file)
    triggers['chisqBysnrsq'] = triggers['chisq']/triggers['snr']**2
    triggers = triggers.merge(glitches[['glitch_id','label']], on='glitch_id', how='left')
    triggersH1 = triggers[triggers['ifo'] == 'H1']
    triggersL1 = triggers[triggers['ifo'] == 'L1']
    
    for label in glitch_labels:
        triggers_label_H1 = triggersH1[triggersH1['label'] == label]
        triggers_label_L1 = triggersL1[triggersL1['label'] == label]
        
        histH1, xedges, yedges = np.histogram2d(triggers_label_H1['snr'].values, triggers_label_H1['chisqBysnrsq'].values, bins=[binsX, binsY])
        histL1, xedges, yedges = np.histogram2d(triggers_label_L1['snr'].values, triggers_label_L1['chisqBysnrsq'].values, bins=[binsX, binsY])
        hists['H1'][label] += histH1
        hists['L1'][label] += histL1
        hists['H1']['Dirty'] += histH1
        hists['L1']['Dirty'] += histL1
        
print("Saving Histograms")
with open("histograms.pkl", 'wb') as f:
    pkl.dump((binsX, binsY, hists), f)
