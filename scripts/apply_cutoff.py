#!/usr/bin/env python3
import numpy as np
import pandas as pd
import warnings
import os
import yaml
import datetime
import argparse
import glob

parser = argparse.ArgumentParser()
parser.add_argument("--clean-triggers", type=str)
parser.add_argument("--dirty-triggers", type=str)
parser.add_argument("--other-triggers", type=str)
parser.add_argument("--glitch-triggers", type=str)
parser.add_argument("--chunk")
args = parser.parse_args()

print("Loading workflow Configurations")
percentile_cutoff = 99

print("Starting Up Apply_Cutoff.py")

print("Listing clean, dirty, and other trigger files")

print('Loading the VSV for all the clean files')

VSV_clean = None
clean_triggers = pd.read_csv(args.clean_triggers)
if not VSV_clean:
    VSV_clean = clean_triggers['VSV'].values
else:
    VSV_clean = np.append(VSV_clean, clean_triggers['VSV'].values, axis = 0)

print("Calculating threshold based on percentile cutoff")
threshold = np.percentile(VSV_clean, percentile_cutoff)

print("Applying the cutoff to all dirty files.")
dirty_triggers = pd.read_csv(args.dirty_triggers, index_col = 0)
clean_temp = dirty_triggers[dirty_triggers['VSV'] <= threshold]
glitch_temp = dirty_triggers[dirty_triggers['VSV'] > threshold]
 
glitch_temp.to_csv(os.path.join(args.glitch_triggers, f"glitched_chunk{args.chunk}.csv"))

#clean_triggers['phase'] = np.repeat(0, len(clean_triggers))
#clean_temp['phase'] = np.repeat(1, len(clean_temp))
    
#clean_triggers = pd.concat([clean_triggers, clean_temp])
print(os.path.join('/'.join(args.clean_triggers.split('/')[:10]), 'glitched'))
#clean_triggers.to_csv(os.path.join('/'.join(args.clean_triggers.split('/')[:11]), f'glitched/glitched_chunk{args.chunk}.csv'))

print('Done with apply_cutoff.py')
