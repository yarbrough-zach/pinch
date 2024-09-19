#! /usr/bin/env/ python3

import argparse
import pandas as pd

class ChunkParse: 
    def parse_chunk_file(self, chunk, chunk_definition_file):
        chunk = str(chunk)
        
        if chunk_definition_file.endswith('.txt'):
            chunk_dict = {}
            with open(chunk_definition_file, "r") as file:
                lines = file.readlines()

                for line in lines:
                    elements = line.split()
                    chunk_dict[elements[0]] = [elements[1], elements[2]]

            del chunk_dict['#']

            start = chunk_dict[chunk][0]
            end = chunk_dict[chunk][1]

        elif chunk_definition_file.endswith('.csv'):
            print('reading csv')
            df = pd.read_csv(chunk_definition_file, usecols=['chunk', 'start', 'end'], index_col=False)
            #indiv_df = df[df['chunk'] == int(chunk)]
            #start = indiv_df['start']
            #end = indiv_df['end']
            start = df.iloc[int(chunk)-1]['start']
            end = df.iloc[int(chunk)-1]['end']
        return start, end

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--chunk-definition-file', type=str)
    parser.add_argument('--chunk', type=str)
    args = parser.parse_args()
