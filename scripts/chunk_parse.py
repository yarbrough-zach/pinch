#! /usr/bin/env/ python3

import argparse

class ChunkParse: 
    def parse_chunk_file(self, chunk, chunk_definition_file):
        chunk = str(chunk)

        chunk_dict = {}
        with open(chunk_definition_file, "r") as file:
            lines = file.readlines()

            for line in lines:
                elements = line.split()
                chunk_dict[elements[0]] = [elements[1], elements[2]]

        del chunk_dict['#']

        start = chunk_dict[chunk][0]
        end = chunk_dict[chunk][1]
        return start, end

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--chunk-definition-file', type=str)
    parser.add_argument('--chunk', type=str)
    args = parser.parse_args()
