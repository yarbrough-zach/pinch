#!/usr/bin/env python3

import os, glob, argparse

import pandas as pd
import numpy as np

from ligo.lw import ligolw
from ligo.lw import array as ligolw_array
from ligo.lw import param as ligolw_param
from ligo.lw import lsctables
from ligo.lw import utils as ligolw_utils

class LIGOLWContentHandler(ligolw.LIGOLWContentHandler):
    pass

ligolw_array.use_in(LIGOLWContentHandler)
ligolw_param.use_in(LIGOLWContentHandler)
lsctables.use_in(LIGOLWContentHandler)

parser = argparse.ArgumentParser()
parser.add_argument('--input-path', type=str)
parser.add_argument('--output-path', type=str)
parser.add_argument('--start', type=int)
parser.add_argument('--end', type=int)
parser.add_argument('--chunk', type=int)
parser.add_argument('--dir', type=str)
parser.add_argument('--low', type=int)
parser.add_argument('--high', type=int)
parser.add_argument('--gps-target', type=int)
parser.add_argument('--save-likelihoods', action='store_true')
parser.add_argument('--likelihood-output-path', type=str)
args = parser.parse_args()

class GstlalTriggers:
    def __init__(self, start, end, input_path, output_path):
        self.start = start
        self.end = end
        self.input_path = input_path
        self.output_path = output_path
        
        if not os.path.isdir(args.input_path):
            raise ValueError(f"{args.input_path} is not a valid path")
    
    def find_gstlal_edward_trigger_files_per_chunk(self, dir=None, start=None, end=None, input_file_path=None, low=None, high=None, gps_target=None):
        print('find files per chunk') 

        files = []

        if (low and high) and not gps_target:
            range = np.arange(low, high)

            for i in range:
                print(f"{input_file_path}/*_noninj_LLOID-{dir}{i}*.xml.gz")
                query = glob.glob(f"{input_file_path}/*_noninj_LLOID-{dir}{i}*.xml.gz")
                print(query)
                files += query
            
            print("low, high", files)
        
        elif gps_target is not None:
            print('gps target', gps_target)
            print("test", os.path.join(input_file_path, f"*_noninj_LLOID-{dir}{gps_target}*.xml.gz"))
            files += glob.glob(os.path.join(input_file_path, f"*_noninj_LLOID-{dir}{gps_target}*.xml.gz"))
            print("glob", files)
        
        if not files:
            raise ValueError(f"List of trigger files is empty for dir {dir} and target {gps_target}")
        print(files)
        return list(files)
    
    def read_gstlal_xml(self, file):
        svd = file.split('-')[1].split('_')[0][1:]
        xmldoc = ligolw_utils.load_filename(file, contenthandler = LIGOLWContentHandler)
        snglrow = lsctables.SnglInspiralTable.get_table(xmldoc)
        coincrow = lsctables.CoincTable.get_table(xmldoc)
        
        if args.save_likelihoods:
            return svd, coincrow
        else:
            return svd, snglrow

    def read_snglrow(self, dataframe, file, append_index):
        
        svd, snglrow = self.read_gstlal_xml(file)
        #print('len df', len(dataframe))

        for row in snglrow:
            dataframe.loc[append_index, 'snr'] = row.snr
            dataframe.loc[append_index, 'chisq'] = row.chisq
            dataframe.loc[append_index, 'mass1'] = row.mass1
            dataframe.loc[append_index, 'mass2'] = row.mass2
            dataframe.loc[append_index, 'spin1z'] = row.spin1z
            dataframe.loc[append_index, 'spin2z'] = row.spin2z
            dataframe.loc[append_index, 'ifo'] = row.ifo
            dataframe.loc[append_index, 'template_duration'] = row.template_duration
            dataframe.loc[append_index, 'template_id'] = row.template_id
            dataframe.loc[append_index, 'end_time'] = row.end_time
            dataframe.loc[append_index, 'end_time_ns'] = row.chisq
            dataframe.loc[append_index, 'svd'] = svd
            
            append_index += 1
            print('append index', append_index)
            #if append_index  % 1000 == 0:
            #    print('append index', append_index)
                
        return append_index

    def read_coincrow(self, dataframe, file, append_index):
        svd, coincrow = self.read_gstlal_xml(file)
        for row in coincrow:
            dataframe.loc[append_index, 'likelihood'] = row.likelihood
            dataframe.loc[append_index, 'coinc_def_id'] = row.coinc_def_id
            dataframe.loc[append_index, 'coinc_event_id'] = row.coinc_event_id
        

                
if __name__ == "__main__":
#    print("Running...")
    
    columns = ["snr", "chisq", "mass1", "mass2", "spin1z", "spin2z", "ifo", "template_duration", 
           "template_id", "end_time", "end_time_ns"]

    trigger_df = pd.DataFrame(columns=columns, index=range(int(1e6)))
    
    gstlalTriggers = GstlalTriggers(args.start, args.end, args.input_path, args.output_path)
    
    glob_files = gstlalTriggers.find_gstlal_edward_trigger_files_per_chunk(dir=args.dir, input_file_path=args.input_path, low=args.low, high=args.high, gps_target=args.gps_target)
    
    append_index = 0
    for file in glob_files:
        count = gstlalTriggers.read_snglrow(trigger_df, file, append_index)
        append_index = count

    trigger_df = trigger_df.iloc[:append_index]

    trigger_df.to_csv(args.output_path)

    if args.save_likelihoods:
        columns = ['likelihood', 'coinc_def_id', 'coinc_event_id']
        likelihood_df = pd.DataFrame(columns=columns, index=range(int(1e6)))

        append_index = 0
        for file in glob_files:
            count = gstlalTriggers.read_coincrow(likelihood_df, file, append_index)
            append_index = count

        likelihood_df.to_csv(args.likelihood_output_path)
