#! /usr/bin/env python3

import pandas as pd
import argparse

from gwtrigfind import find_trigger_files
from gwpy.table import (Table, EventTable)
from gwpy.table.filters import in_segmentlist
from gwpy.segments import DataQualityFlag
from gwpy.time import tconvert
from gwpy.timeseries import TimeSeries
from gwpy.segments import Segment, SegmentList

from urllib.parse import urlparse
from gwpy.table.io.pycbc import filter_empty_files

import h5py

hoft_channel = 'L1:GDS-CALIB_STRAIN'
idq_channel = 'L1:IDQ-LOGLIKE_OVL_16_4096'
whistle_flag = 'L1:DCH-WHISTLES:1'

parser = argparse.ArgumentParser()
parser.add_argument('--start', type=int)
parser.add_argument('--end', type=int)
parser.add_argument('--chunk', type=str)
parser.add_argument('--output-path', type=str)
args = parser.parse_args()

#start = 1373711624
#end = 1374936316

pycbc_files = find_trigger_files(hoft_channel, 'pycbc_live', args.start, args.end)

print(pycbc_files)

pycbc_files = [urlparse(url).path for url in pycbc_files]

print('Filtering empty files')
pycbc_files = filter_empty_files(pycbc_files, ifo='L1')
print('non empty files', pycbc_files)

#columns = ['end_time', 'snr', 'chisq','template_duration', 'mass1','mass2','spin1z','spin2z']

columns = ['approximant',
'chisq',
'chisq_dof',
'coa_phase',
'end_time',
'f_lower',
'gates',
'loudest',
'mass1',
'mass2',
'psd',
'sg_chisq',
'sigmasq',
'snr',
'spin1z',
'spin2z',
'template_duration',
'template_hash',
'template_id']

pycbc_df = pd.DataFrame(columns=columns)

for i, file in enumerate(pycbc_files):
    print(file)
    if i % 10 == 0:
        print(i)
    pycbc_events = EventTable.read(file, format='hdf5.pycbc_live', ifo='L1', columns=columns)
    pycbc_events = pycbc_events.to_pandas()
    pycbc_df = pd.concat([pycbc_df, pycbc_events], ignore_index=True)
    
    if i % 1000 == 0 and not pycbc_df.empty:
        print(i)
        pycbc_df.to_csv(f'{args.output_path}/chunk{args.chunk}_pycbc_{i}.csv')
        del pycbc_df
        pycbc_df = pd.DataFrame(columns=columns)

pycbc_df.to_csv('{args.output_path}/chunk{args.chunk}_pycbc_final.csv')

