#!/usr/bin/env python3

import os, glob, argparse

import pandas as pd

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
args = parser.parse_args()

class GstlalTriggers:
    def __init__(self, start, end, input_path, output_path):
        self.start = start
        self.end = end
        self.input_path = input_path
        self.output_path = output_path
        
        if not os.path.isdir(args.input_path):
            raise ValueError(f"{args.input_path} is not a valid path")
    
    def find_gstlal_edward_trigger_files_per_chunk(self, dir, start, end, input_file_path):
        
        files = glob.glob(f"{input_file_path}/{dir}/*LLOID-*.xml.gz")
        print(files)
        return list(files)
    
    def read_gstlal_xml(self, file):
        svd = file.split('-')[1].split('_')[0][1:]
        xmldoc = ligolw_utils.load_filename(file, contenthandler = LIGOLWContentHandler)
        snglrow = lsctables.SnglInspiralTable.get_table(xmldoc)
        return svd, snglrow

    def read_snglrow(self, dataframe, file):
        
        svd, snglrow = self.read_gstlal_xml(file)
        
        for row in snglrow:
            append_index = len(dataframe.index)
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
            
            if len(dataframe) % 10000 == 0:
                print(len(dataframe))

                
if __name__ == "__main__":
#    print("Running...")
    
    columns = ["snr", "chisq", "mass1", "mass2", "spin1z", "spin2z", "ifo", "template_duration", 
           "template_id", "end_time", "end_time_ns"]

    trigger_df = pd.DataFrame(columns=columns)
#    print("Trigger df:", trigger_df)
    
    gstlalTriggers = GstlalTriggers(args.start, args.end, args.input_path, args.output_path)
    
    files = gstlalTriggers.find_gstlal_edward_trigger_files_per_chunk(args.dir, args.start, args.end, args.input_path)
    
    for file in files:
#        print(file)
        gstlalTriggers.read_snglrow(trigger_df, file)

    trigger_df.to_csv(args.output_path)
