import pickle as pkl
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd

with open('distances_clean.pkl', 'rb') as f:
    distances_clean = pkl.load(f)
with open('distances_dirty.pkl', 'rb') as f:
    distances_dirty = pkl.load(f)

labels_clean, distances_clean = distances_clean.T
labels_dirty, distances_dirty = distances_dirty.T

distances_clean = distances_clean.astype(float)
distances_dirty = distances_dirty.astype(float)

percentile_99 = np.percentile(distances_clean, 1)

dirty_files = sorted(['DirtyTriggerFiles/' + f for f in os.listdir('DirtyTriggerFiles/') if f.endswith('.csv')])

distances_glitch = distances_dirty[distances_dirty <= percentile_99]
labels_glitch = labels_dirty[distances_dirty <= percentile_99]

files_glitch = np.unique([label.split('-')[0] for label in labels_glitch])
idx = {file:[] for file in files_glitch}
for label in labels_glitch:
    file = label.split('-')[0]
    idx[file] += [int(label.split('-')[1])]
for key in idx.keys():
    idx[key] = np.array(idx[key])
    
for file in dirty_files:
    print(file)
    triggers = pd.read_csv(file)
    triggers = triggers.loc[idx[file]]
    triggers.to_csv(file.replace('DirtyTriggerFiles/', 'GlitchTriggerFiles_ML/'))