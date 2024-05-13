#!/usr/bin/env python3
import os, argparse
import pandas as pd
from gwpy.table import GravitySpyTable

# export authentication for gspy database
class GravitySpyEvents:
    def __init__(self, t_start=None, t_end=None, ml_label=None):
        self.start = t_start
        self.end = t_end
        self.ml_label = ml_label

    def fetch_gravity_spy_events(self): 
        
        if not self.ml_label:
            glitches = GravitySpyTable.fetch("gravityspy", "glitches_v2d0", 
            selection=f"event_time > {self.start} && event_time < {self.end}")
  
        else:
            glitches = GravitySpyTable.fetch("gravityspy", "glitches_v2d0", 
            selection=f"event_time > {self.start} && event_time < {self.end} && ml_label={self.ml_label} && confidence >=0.9")
            
        if not glitches:
            raise ValueError("No glitches retured for query")

        return glitches

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, help='GPS time of query start')
    parser.add_argument('--end', type=int, help='GPS time of query end')
    parser.add_argument('--chunk-definition-file', type=str, help='Path to file containing chunk definitions')
    parser.add_argument('--chunk', type=str)
    parser.add_argument('--ml-label', type=str, help='Label of glitch class to query, optional')
    parser.add_argument('--output-path', type=str)
    args = parser.parse_args()
    
    if args.chunk_definition_file and args.chunk:
        
        chunk_dict = {}
        with open(args.chunk_definition_file, "r") as file:
            lines = file.readlines()

            for line in lines:
                elements = line.split()
                chunk_dict[elements[0]] = [elements[1], elements[2]]

        del chunk_dict['#']

        start = chunk_dict[args.chunk][0]
        end = chunk_dict[args.chunk][1]

    elif args.start and args.end:
        start = args.start
        end = args.end

    else:
        raise ValueError("Insufficient time arguments provided")
    
    gspy_events = GravitySpyEvents(t_start=start, t_end=end, ml_label=args.ml_label)

    glitches = gspy_events.fetch_gravity_spy_events()

    glitches.to_pandas().to_csv(f"{args.output_path}/chunk{args.chunk}_gspy.csv")
