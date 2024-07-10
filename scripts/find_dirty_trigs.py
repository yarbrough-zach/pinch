#!/usr/bin/env python3
import numpy as np
import pandas as pd
import os
import yaml
import argparse
import time

from get_gspy_events import GravitySpyEvents

parser = argparse.ArgumentParser()
parser.add_argument('--pipeline', type=str)
parser.add_argument('--pipeline-triggers', type=str)
parser.add_argument('--nrows', type=int)
parser.add_argument('--gspy-triggers', type=str)
parser.add_argument('--omicron-triggers-H1', type=str)
parser.add_argument('--omicron-triggers-L1', type=str)
parser.add_argument('--clean-output', type=str)
parser.add_argument('--dirty-output', type=str)
parser.add_argument('--other-output', type=str)
parser.add_argument('--query', action='store_true')
parser.add_argument('--chunk-definition-file', type=str)
parser.add_argument('--chunk', type=str)
parser.add_argument('--tag', type=str)
args = parser.parse_args()

#trigger_files = sorted([raw_trigger_dir + f for f in os.listdir(raw_trigger_dir) if f.endswith('.csv')])[:1]

if args.query and args.chunk_definition_file and args.chunk:

    chunk_dict = {}
    with open(args.chunk_definition_file, "r") as file:
        lines = file.readlines()

        for line in lines:
            elements = line.split()
            chunk_dict[elements[0]] = [elements[1], elements[2]]

    del chunk_dict['#']

    start = chunk_dict[args.chunk][0]
    end = chunk_dict[args.chunk][1]
    wait_time = np.random.uniform(30, 300)
    time.sleep(wait_time)
    gspy = GravitySpyEvents(t_start = start, t_end = end)
    glitches = gspy.fetch_gravity_spy_events()
    glitches = glitches.to_pandas()
    print(f"Len final glitch df: {len(glitches)}")

elif args.gspy_triggers and not args.query:
    glitches = pd.read_csv(args.gspy_triggers, index_col = 0)

else:
    raise ValueError('Improper combination of arguments passed for gspy glitches, please query with chunk definition file or provide gspy file.')


H1_omics = pd.read_csv(args.omicron_triggers_H1, index_col = 0)
L1_omics = pd.read_csv(args.omicron_triggers_L1, index_col = 0)

omics = pd.concat([H1_omics, L1_omics])

omicron_snr_cutoff = 5.5

if type(omicron_snr_cutoff) == type(None):
    omicron_snr_cutoff = min(glitches['snr'])
    print(omicron_snr_cutoff)
print("Omicron Size Original: ", len(omics))
omics = omics[omics['snr'] >= omicron_snr_cutoff]
print("Omicron Size SNR Reduced: ", len(omics))

glitches['tstart'] = (glitches['peak_time'] + 1e-9*glitches['peak_time_ns']) - glitches['duration']/2
glitches['tend'] = (glitches['peak_time'] + 1e-9*glitches['peak_time_ns']) + glitches['duration']/2

omics['tend'] = (omics['peak_time'] + 1e-9*omics['peak_time_ns']) - omics['duration']/2
omics['tstart'] = (omics['peak_time'] + 1e-9*omics['peak_time_ns']) + omics['duration']/2

omics['GPStime'] = omics['peak_time'] + 1e-9*omics['peak_time_ns']

print(f"Trigger file: {args.pipeline_triggers}")

if args.pipeline == 'gstlal':
    triggers = pd.read_csv(f"{args.pipeline_triggers}", index_col = 0)

elif args.pipeline == 'pycbc':

    if args.nrows:
        triggers = pd.read_csv(f"{args.pipeline_triggers}", nrows=args.nrows) #nrows = 1000000
    else:
        triggers = pd.read_csv(f"{args.pipeline_triggers}")

print(triggers.columns)


if args.pipeline == 'gstlal':
    triggers['GPStime'] = triggers['end_time'] + 1e-9*triggers['end_time_ns']
    triggers['start_time'] = triggers['GPStime'] - triggers['template_duration']

elif args.pipeline == 'pycbc':
    triggers['GPStime'] = triggers.end_time
    triggers['ifo'] = 'L1'

glitchIDs = ['None']*len(triggers)
omicIDs = [-1]*len(triggers)

#print(f"Triggers: {min(triggers.start_time)}, {max(triggers.GPStime)}")
#print(f"Glitches: {min(glitches.tstart)}, {max(glitches.tend)}")

if args.pipeline ==  'gstlal':
    glitch_time_mask = (glitches['tstart'] >= min(triggers['start_time'])) & (glitches['tend'] <= max(triggers['GPStime']))
    omic_time_mask = (omics['tend'] >= min(triggers['GPStime'])) & (omics['tstart'] <= max(triggers['GPStime']))
elif args.pipeline == 'pycbc':
    glitch_time_mask = (glitches['tstart'] >= min(triggers['GPStime'])) & (glitches['tend'] <= max(triggers['GPStime']))
    omic_time_mask = (omics['tend'] >= min(triggers['GPStime'])) & (omics['tstart'] <= max(triggers['GPStime']))

glitches_temp = glitches[glitch_time_mask]
print(f"len glitches_temp: {len(glitches_temp)}")
omics_temp = omics[omic_time_mask]
print("Omicron Size Time Constrained: ", len(omics_temp))  
ifo_mask = {'H1':triggers['ifo'] == 'H1', 'L1':triggers['ifo'] == 'L1'}

count = 0
for i, glitch in glitches_temp.iterrows():
    
    if count % 1000 == 0:
        print(str(count) + " / " + str(len(glitches_temp)))

    ifo = glitch['ifo']

    if args.pipeline == 'gstlal':
        triggers_in_glitch_mask = (triggers['start_time'] <= glitch["tend"]) & (triggers['GPStime'] >= glitch["tstart"])
        triggers_in_glitch = triggers[triggers_in_glitch_mask & ifo_mask[ifo]]
    
    elif args.pipeline == 'pycbc':
        triggers_in_glitch_mask = (triggers['GPStime'] <= glitch["tend"]) & (triggers['GPStime'] >= glitch["tstart"])
        triggers_in_glitch = triggers[triggers_in_glitch_mask & ifo_mask[ifo]]
    
    indexes = triggers_in_glitch.index
    for idx in indexes:
        glitchIDs[idx] = glitch['gravityspy_id']
    count += 1
    
count = 0
for i, omic in omics_temp.iterrows():
    ifo = omic['ifo']
    if count % 1000 == 0:
        print(str(count) + " / " + str(len(omics_temp)))

    if args.pipeline == 'gstlal':
        triggers_in_omic_mask = (triggers['start_time'] <= omic["tend"]) & (triggers['GPStime'] >= omic["tstart"])
        triggers_in_omic = triggers[triggers_in_omic_mask & ifo_mask[ifo]]
    elif args.pipeline == 'pycbc':
        triggers_in_omic_mask = (triggers['GPStime'] <= omic["tend"]) & (triggers['GPStime'] >= omic["tstart"])
        triggers_in_omic = triggers[triggers_in_omic_mask & ifo_mask[ifo]]
    
    if count % 1000 == 0:
        print(f"len triggers_in_omic: {len(triggers_in_omic)}")
    indexes = triggers_in_omic.index
    for idx in indexes:
        omicIDs[idx] = omic['event_id']
    count += 1

triggers['glitch_id'] = glitchIDs
triggers['omic_id'] = omicIDs

glitches['glitch_id'] = glitches['gravityspy_id']
mask_glitch = (triggers['glitch_id'] != 'None')
print(f"Len of true mask glitch: {len(mask_glitch)}")
mask_other = (triggers['glitch_id'] == 'None') & (triggers['omic_id'] == 1)
mask_clean = (triggers['glitch_id'] == 'None') & (triggers['omic_id'] == -1)

print(f"Len triggers with mask glitch: {len(triggers[mask_glitch])}")
triggers_dirty = triggers[mask_glitch].copy()
print(f"len triggers_dirty: {len(triggers_dirty)}")
triggers_other = triggers[mask_other].copy()
triggers_clean = triggers[mask_clean].copy()

triggers_dirty = triggers_dirty.merge(glitches[['glitch_id','ml_confidence', 'ml_label']], on='glitch_id', how='left')
print(f"len triggers_dirty after merge: {len(triggers_dirty)}")
triggers_other = pd.concat([triggers_other, triggers_dirty[triggers_dirty['ml_confidence'] < 0.9]])
#triggers_dirty = triggers_dirty[triggers_dirty['confidence'] >= 0.9]
print(f"len triggers_Dirty after confidence cut: {len(triggers_dirty)}")

triggers_clean.to_csv(f"{args.clean_output}/clean_chunk{args.chunk}_{args.tag}.csv")
triggers_dirty.to_csv(f"{args.dirty_output}/dirty_chunk{args.chunk}_{args.tag}.csv")
triggers_other.to_csv(f"{args.other_output}/other_chunk{args.chunk}_{args.tag}.csv")

print("Done!")
