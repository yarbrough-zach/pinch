#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd
from collections import defaultdict


class TIO:
    def __init__(
            self,
            input_path=None,
            output_path=None,
    ):
        self.input_path = input_path
        self.output_path = output_path

    @staticmethod
    def determine_input_type(path):
        if os.path.isdir(path):
            return "dir"
        elif os.path.isfile(path):
            return "file"
        else:
            return "unknown"

    @staticmethod
    def determine_ifos(df):
        return np.unique(df['ifo'])

    @staticmethod
    def multiple_ifos(df):
        """
        Determine if a given df has rows for multiple ifos
        """
        ifos = np.unique(df['ifo'])

        if len(ifos) > 1:
            return True

        elif len(ifos) == 1:
            return False

    @staticmethod
    def separate_by_ifo(df):
        """
        Return dict of triggers separated by ifo keyed by ifo
        """
        ifos = np.unique(df['ifo'])

        return {ifo: df[df['ifo'] == ifo] for ifo in ifos}

    @classmethod
    def read(cls, input_path):
        path_type = cls.determine_input_type(input_path)

        if path_type == 'dir':
            data = cls._read_files_in_dir(input_path)
        elif path_type == 'file':
            data = cls._read_file(input_path)
        else:
            raise ValueError('Input path is neither path nor file')

        return data

    @classmethod
    def _read_files_in_dir(cls, path):
        """
        Read in csv files in dir, return dict of dfs for each ifo keyed by ifo
        """
        dfs = defaultdict(list)

        for file in os.listdir(path):
            if file.endswith('.csv'):
                full_path = os.path.join(path, file)
                df = pd.read_csv(full_path)

                for ifo in cls.determine_ifos(df):
                    dfs[ifo].append(df)

        for ifo in dfs.keys():
            dfs[ifo] = pd.concat(dfs[ifo], ignore_index=True)

        return dfs

    @classmethod
    def _read_file(cls, path):
        df = pd.read_csv(path)

        return cls.separate_by_ifo(df)
