#!/usr/bin/env python3

import os
import argparse
import numpy as np

from file_finder import FileFinder

parser = argparse.ArgumentParser()
parser.add_argument('--input-path', type=str)
parser.add_argument('--output-path', type=str)
parser.add_argument('--chunk', type=str)
parser.add_argument('--num-jobs', type=int)
parser.add_argument('--chunk-definition-file', type=str)
parser.add_argument('--dag-output-path', type=str, help='Where to save dag')
args = parser.parse_args()

#input_file_path = '/home/gstlalcbc.online/observing/4/a/runs/trigs.edward_o4a/'
#output_file_path = '/home/zach.yarbrough/TGST/observing/4/a/gstlal_triggers/'

#chunk = 13
#input_start = 1383404656
#input_end = 1384588801

chunk_dict = {}
with open(args.chunk_definition_file, "r") as file:
    lines = file.readlines()

    for line in lines:
        elements = line.split()
        chunk_dict[elements[0]] = [elements[1], elements[2]]

del chunk_dict['#']

chunk_start = chunk_dict[args.chunk][0]
chunk_end = chunk_dict[args.chunk][1]

start = int(str(chunk_start)[:5])
end = int(str(chunk_end)[:5])

dirs = np.arange(start, end+1)

with open(os.path.join(args.dag_output_path, f"fetch_chunk{args.chunk}_preallocate_many_jobs.dag"), 'w') as f:
    job_num = 0    
    for i, dir in enumerate(dirs):
        
        print(dir)

        loop_input_file_path = os.path.join(args.input_path, str(dir))

        for j in range(0, 9):
            loop_output_file_path = os.path.join(args.output_path, f"gstlal_chunk{args.chunk}_{dir}_part{job_num}_triggers.csv")

            tag = format(job_num, '05')

            f.write(f'JOB fetch_gstlal_triggers.{tag} fetch_gstlal_triggers.sub\n')
            f.write(f'VARS fetch_gstlal_triggers.{tag} nodename="fetch_gstlal_triggers_{tag}" input_file_path="{loop_input_file_path}" output_path="{loop_output_file_path}" start="{start}" end="{end}" chunk="{args.chunk}" dir="{dir}" gps_target="{j}"\n')

            job_num += 1

        #job_num += 1
        #
        #loop_output_file_path = os.path.join(args.output_path, f"gstlal_chunk{args.chunk}_{dir}_half2_triggers.csv")
        #
        #tag = format(job_num, '05')

        #f.write(f'JOB fetch_gstlal_triggers.{tag} fetch_gstlal_triggers.sub\n')
        #f.write(f'VARS fetch_gstlal_triggers.{tag} nodename="fetch_gstlal_triggers_{tag}" input_file_path="{loop_input_file_path}" output_path="{loop_output_file_path}" start="{start}" end="{end}" chunk="{args.chunk}" dir="{dir}" low="5" high="9"\n')

        #job_num +=1 
