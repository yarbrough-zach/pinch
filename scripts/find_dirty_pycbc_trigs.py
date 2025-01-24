#!/usr/bin/env python3
import numpy as np
import pandas as pd

import h5py
import os
import yaml
import argparse
import time

from get_gspy_events import GravitySpyEvents
from chunk_parse import ChunkParse
from omicron_finder import OmicronFinder

parser = argparse.ArgumentParser()
parser.add_argument('--path-to-pipeline-triggers', type=str, help='path on current cluster to pycbc h5 trigger files')
parser.add_argument('--start', type=int, help='alternative method if chunk not provided')
parser.add_argument('--end', type=int, help='alternative method if chunk not provided')
parser.add_argument('--gspy-triggers', type=str)
parser.add_argument('--omicron-triggers-H1', type=str)
parser.add_argument('--omicron-triggers-L1', type=str)
parser.add_argument('--clean-output', type=str, help='where you would like csvs of clean triggers to be stored')
parser.add_argument('--dirty-output', type=str, help='where you would like csvs of dirty triggers to be stored')
parser.add_argument('--other-output', type=str, help='where you would like csvs of other triggers to be stored')
parser.add_argument('--query', action='store_true', help='bool, whether or not to query gravity spy database for given times')
parser.add_argument('--wait', action='store_true', help='bool, adds random timeout to help against gravity spy database restrictions')
parser.add_argument('--ml-confidence', type=float, help='optional confidence cut for gravity spy query')
parser.add_argument('--chunk-definition-file', type=str, help='path to chunk definition txt file')
parser.add_argument('--chunk', type=str, help='chunk you wish to analyze')
parser.add_argument('--tag', type=str, help='optional tag to add to filenames')
args = parser.parse_args()


if not os.path.isdir(args.clean_output):
    raise ValueError(f"{args.clean_output} does not exist")

elif not os.path.isdir(args.dirty_output):
    raise ValueError(f"{args.dirty_output} does not exist")

elif not os.path.isdir(args.other_output):
    raise ValueError(f"{args.other_output} does not exist")

else:
    print(f"All output dirs exist, continuing...")

# read chunk definition file if provided, parse chunks
if args.chunk_definition_file and args.chunk:
    chunkparse = ChunkParse()
    start, end = chunkparse.parse_chunk_file(args.chunk, args.chunk_definition_file)
    print(start, end)

elif args.start and args.end:
    start = args.start
    end = args.end

else:
    raise ValueError('Invalid combination of time inputs provided. '
        'Please provide either chunk number and chunk defiunition file ' 
        'or start and end gps times')

# gravity spy
if args.query:
 
    if args.wait:
        wait_time = np.random.uniform(30, 300)
        print('querying, please wait for random sleep time', wait_time)
    
        time.sleep(wait_time)

    if args.ml_confidence:
        gspy = GravitySpyEvents(t_start = start, t_end = end, confidence = args.ml_confidence)

    else:
        gspy = GravitySpyEvents(t_start = start, t_end = end, confidence=0.9)
    
    glitches = gspy.fetch_gravity_spy_events()
    glitches = glitches.to_pandas()
    print(f"Len final glitch df: {len(glitches)}")

elif args.gspy_triggers and not args.query:
    glitches = pd.read_csv(args.gspy_triggers, index_col = 0)
    glitches = glitches[glitches.ml_confidence >= 0.9]

else:
    raise ValueError('Improper combination of arguments passed for gspy glitches, please query with chunk definition file or provide gspy file.')

# omicron triggers
# should we allow them to be passed as arguments still?

# H1_omics = pd.read_csv(args.omicron_triggers_H1, index_col = 0)
# L1_omics = pd.read_csv(args.omicron_triggers_L1, index_col = 0)

omic = OmicronFinder()
ifo = omic.det_site()
omics = omic.fetch_and_save_omicron(start, end)

if omics.empty:
    raise RuntimeError("No omicron triggers returned from OmicronFinder")

else:
    print(f"{len(omics)} omicron triggers found...")

omicron_snr_cutoff = 5.5

print("Omicron Size Original: ", len(omics))

omics = omics[omics['snr'] >= omicron_snr_cutoff]
print("Omicron Size SNR Reduced: ", len(omics))


# construct stard and end times for glitches
glitches['peak_time'] = pd.to_numeric(glitches['peak_time'], errors='coerce')
glitches['peak_time_ns'] = pd.to_numeric(glitches['peak_time_ns'], errors='coerce')
glitches['duration'] = pd.to_numeric(glitches['duration'], errors='coerce')

glitches['tstart'] = (glitches['peak_time'] + 1e-9*glitches['peak_time_ns']) - glitches['duration']/2
glitches['tend'] = (glitches['peak_time'] + 1e-9*glitches['peak_time_ns']) + glitches['duration']/2

# construct start and end times for omicron triggers
#omics['tend'] = (omics['peak_time'] + 1e-9*omics['peak_time_ns']) - omics['duration']/2
#omics['tstart'] = (omics['peak_time'] + 1e-9*omics['peak_time_ns']) + omics['duration']/2
#omics['GPStime'] = omics['peak_time'] + 1e-9*omics['peak_time_ns']

#omics['tend'] = (omics['time'] + 1e-9*omics['time_ns']) - omics['duration']/2
#omics['tstart'] = (omics['time'] + 1e-9*omics['time_ns']) + omics['duration']/2
omics['GPStime'] = omics['time']

def extract_pycbc_data(file_path):

    def recursively_extract(group, prefix=""):
        for key, item in group.items():
            path = f"{prefix}/{key}" if prefix else key

            if isinstance(item, h5py.Dataset):
                data[path] = item[()]
            elif isinstance(item, h5py.Group):
                recursively_extract(item, path)

    with h5py.File(file_path, 'r') as h5_file:

        # ifo provided by socket in OmicronFinder
        group = h5_file[ifo]

        data = {}

        recursively_extract(group)
        
        for key in ['gates', 'loudest', 'psd']:
            if key in data.keys():
                del data[key]

        #for key in data.keys():
            #print(key, len(data[key]))
        
        #df =  pd.DataFrame(dict([(k, pd.Series(v)) for k, v in data.items()]))
   
    return data

def find_dirty_trigs(triggers, glitches, omics):
    print('Finding dirty trigs...')
    
    triggers['GPStime'] = triggers.end_time

    glitchIDs = ['None']*len(triggers)
    omicIDs = [-1]*len(triggers)

    #print(f"Triggers: {min(triggers.start_time)}, {max(triggers.GPStime)}")
    #print(f"Glitches: {min(glitches.tstart)}, {max(glitches.tend)}")

    glitch_time_mask = (glitches['tstart'] >= min(triggers['GPStime'])) & (glitches['tend'] <= max(triggers['GPStime']))
    omic_time_mask = (omics['tend'] >= min(triggers['GPStime'])) & (omics['tstart'] <= max(triggers['GPStime']))

    glitches_temp = glitches[glitch_time_mask]
    print(f"len glitches_temp: {len(glitches_temp)}")
    omics_temp = omics[omic_time_mask]
    print("Omicron Size Time Constrained: ", len(omics_temp))  
    #ifo_mask = {'H1':triggers['ifo'] == 'H1', 'L1':triggers['ifo'] == 'L1'}

    count = 0
    for i, glitch in glitches_temp.iterrows():
        
        if count % 1000 == 0:
            print(str(count) + " / " + str(len(glitches_temp)))

        ifo = glitch['ifo']

        if ifo == 'V1':
            continue

        triggers_in_glitch_mask = (triggers['GPStime'] <= glitch["tend"]) & (triggers['GPStime'] >= glitch["tstart"])
        #triggers_in_glitch = triggers[triggers_in_glitch_mask & ifo_mask[ifo]]
        triggers_in_glitch = triggers[triggers_in_glitch_mask]

        indexes = triggers_in_glitch.index
        for idx in indexes:
            glitchIDs[idx] = glitch['gravityspy_id']
        count += 1
        
    count = 0
    for i, omic in omics_temp.iterrows():
        if count % 1000 == 0:
            print(str(count) + " / " + str(len(omics_temp)))

        triggers_in_omic_mask = (triggers['GPStime'] <= omic["tend"]) & (triggers['GPStime'] >= omic["tstart"])
        #triggers_in_omic = triggers[triggers_in_omic_mask & ifo_mask[ifo]]
        triggers_in_omic = triggers[triggers_in_omic_mask]
        
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

    #triggers_clean.to_csv(f"{args.clean_output}/clean_chunk{args.chunk}_{args.tag}.csv")
    #triggers_dirty.to_csv(f"{args.dirty_output}/dirty_chunk{args.chunk}_{args.tag}.csv")
    #triggers_other.to_csv(f"{args.other_output}/other_chunk{args.chunk}_{args.tag}.csv")

    return triggers_clean, triggers_dirty, triggers_other

def save_and_reset(df, kind, count, path):
    save_file = f"{kind}_pycbc_triggers_part_{count}.csv"
    df.to_csv(f"{path}/{save_file}", index=False)

    if kind == 'clean':
        return pd.DataFrame(), clean_counter + 1
    
    elif kind == 'dirty':
        return pd.DataFrame(), dirty_counter + 1

    elif kind == 'other':
        return pd.DataFrame(), other_counter + 1

max_rows = 1_000_000

pycbc_files = [
    os.path.join(args.path_to_pipeline_triggers, file)
    for file in os.listdir(args.path_to_pipeline_triggers)
    if file.endswith('.hdf') and (file.startswith('H1L1') or file.startswith('H1L1V1'))]

print(f"Found {len(pycbc_files)} trigger files...")

if not len(pycbc_files):
    raise RuntimeError("No pycbc trigger files found")

clean_counter = 0
dirty_counter = 0
other_counter = 0

clean = pd.DataFrame() 
dirty = pd.DataFrame()
other = pd.DataFrame()

for i, file in enumerate(pycbc_files):
    print(file)

    if (not len(clean) and not len(other) and not len(dirty)):

        df = pd.DataFrame(extract_pycbc_data(file))

        if df.empty:
            print('empty df')
            continue
        
        df = df[df['snr'] >= 4]

        clean, dirty, other = find_dirty_trigs(df, glitches, omics)
        print(len(clean), len(dirty), len(other))

    else:
        current_df = pd.DataFrame(extract_pycbc_data(file))

        if current_df.empty:
            continue

        current_df = current_df[current_df['snr'] >= 4]
        
        current_clean, current_dirty, current_other = find_dirty_trigs(current_df, glitches, omics)

        clean = pd.concat([clean, current_clean], ignore_index=True)
        dirty = pd.concat([dirty, current_dirty], ignore_index=True)
        other = pd.concat([other, current_other], ignore_index=True)

    if len(clean) > max_rows:
        clean, clean_counter = save_and_reset(clean, "clean", clean_counter, args.clean_output)

    if len(dirty) > max_rows:
        dirty, dirty_counter = save_and_reset(dirty, "dirty", dirty_counter, args.dirty_output)

    if len(other) > max_rows:
        other, other_counter = save_and_reset(other, "other", other_counter, args.other_output)

if not clean.empty:
    clean, clean_counter = save_and_reset(clean, "clean", clean_counter, args.clean_output)

if not dirty.empty:
    dirty, dirty_counter = save_and_reset(dirty, "dirty", dirty_counter, args.dirty_output)

if not other.empty:
    other, other_counter = save_and_reset(other, "other", other_counter, args.other_output)

print("Done!")
