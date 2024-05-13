#! /usr/bin/env python3

import os, glob, argparse
import numpy as np



class FileFinder:
    def __init__(self, dir, start, end, input_file_path):
        self.dir = dir
        self.start = start
        self.end = end
        self.input_file_path = input_file_path

    def find_gstlal_edward_trigger_files_per_chunk(self, input_file_path, reduced=False):

        files = glob.glob(f"{input_file_path}/*_noninj_LLOID-*.xml.gz")
        print(files)

        if not files:
            raise ValueError(f"List of trigger files is empty")
        
        if not reduced:
            return list(files)

        else:
            
            reduced_files = []
            for file in files:
                reduced_files.append(file.split('/')[-1])
            return reduced_files

    def return_split_files(self):
        files = self.find_gstlal_edward_trigger_files_per_chunk(self.input_file_path, reduced=True)

        split_index = int(len(files)/2)
        file_half1 = files[:split_index]
        file_half2 = files[split_index:]

        if len(file_half1)+len(file_half2) == len(files):
            print('halves agree')
            
            return file_half1, file_half2

        else:
            print('no')

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', type=str)
    parser.add_argument('--output-path', type=str)
    parser.add_argument('--start', type=int)
    parser.add_argument('--end', type=int)
    parser.add_argument('--chunk', type=int)
    parser.add_argument('--dir', type=str)
    args = parser.parse_args()

    file_finder = FileFinder(args.dir, args.start, args.end, args.input_path)
    files = file_finder.find_gstlal_edward_trigger_files_per_chunk(args.dir, args.start, args.end, args.input_path)
    
    half1, half2 = file_finder.return_split_files()
    print('half1', half1)
    print('half2', half2)

    split_index = int(len(files)/2)
    file_half1 = files[:split_index]
    file_half2 = files[split_index:]

    if len(file_half1)+len(file_half2) == len(files):
        print('halves agree')
    else:
        print('no')
