#!/usr/bin/env python3

import argparse
import pandas as pd


class ChunkParse:
    """
    A utility class for parsing chunk time intervals from a chunk definition file.

    This class supports reading chunk metadata from either a `.txt` file or a `.csv` file.
    It returns the start and end times associated with a given chunk number.

    Methods:
        parse_chunk_file(chunk, chunk_definition_file):
            Parses the given chunk from the definition file and returns its start and end times.
    """

    def parse_chunk_file(self, chunk, chunk_definition_file):
        """
        Parse the start and end time for a specified chunk from a definition file.

        Args:
            chunk (str or int): The chunk number to extract.
            chunk_definition_file (str): Path to the chunk definition file. Supported formats are `.txt` and `.csv`.

        Returns:
            tuple: A tuple (start, end), where both are typically strings representing time intervals.

        Raises:
            KeyError: If the chunk is not found in the `.txt` file.
            IndexError: If the chunk index is out of bounds in the `.csv` file.
        """
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
            df = pd.read_csv(
                    chunk_definition_file,
                    usecols=['chunk', 'start', 'end'],
                    index_col=False
                )

            start = df.iloc[int(chunk)-1]['start']
            end = df.iloc[int(chunk)-1]['end']
        return start, end


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--chunk-definition-file', type=str)
    parser.add_argument('--chunk', type=str)
    args = parser.parse_args()

    chunkparse = ChunkParse()
    start, end = chunkparse.parse_chunk_file(
            args.chunk,
            args.chunk_definition_file
        )

    print(start, end)
