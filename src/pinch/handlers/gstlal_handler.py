#!/usr/bin/env python3

import os
from typing import List, Tuple, Union
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class GstlalHandler:
    """
    A handler for reading and conditioning GstLAL pipeline trigger data.

    This class loads one or more GstLAL CSV files, filters for a specified IFO,
    and computes derived start and end times for each trigger.

    Attributes:
        trigger_path (str): Path to the directory containing GstLAL trigger CSV files.
        ifo (str): Interferometer name (e.g., 'H1', 'L1').
        segment (bool): Currently unused. Reserved for segment-specific functionality.
        triggers (pd.DataFrame or None): DataFrame holding loaded and conditioned triggers.

    Methods:
        return_gstlal_file_list(): Return a list of all CSV files in the trigger path.
        read_gstlal_csv(csv_path): Read a single CSV file into the triggers attribute.
        read_all_gstlal_csv(): Load and concatenate all valid CSVs in the trigger directory.
        construct_gstlal_start_end(): Compute `tstart` and `tend` columns for triggers.
        condition_gstlal_triggers(): Read, filter, and compute full timing for triggers.
        return_max_start_end(): Return the minimum tstart and maximum tend for the triggers.
    """
    def __init__(
            self,
            trigger_path: Union[str, Path],
            ifo: str,
            segment: bool = False
        ) -> None:
            self.triggers = None
            self.ifo = ifo
            self.segment = segment
            self.trigger_path = trigger_path

    def return_gstlal_file_list(self) -> List[str]:
        """
        Return a list of all file paths in the trigger directory.

        Returns:
            list[str]: List of full paths to files under `self.trigger_path`.
        """
        return [
            os.path.join(self.trigger_path, f)
            for f in os.listdir(self.trigger_path) if f.endswith('.csv')
        ]

        def read_gstlal_csv(self, csv_path: Union[str, path]) -> None:
        """
        Read a single GstLAL trigger CSV file.

        Args:
            csv_path (str): Path to a GstLAL CSV file.
        """
        self.triggers = pd.read_csv(csv_path)

    def read_all_gstlal_csv(self) -> None:
        """
        Read and concatenate all CSV files in the trigger directory.

        This filters out empty files and only retains triggers
        for the specified interferometer.
        """
        csv_list = self.return_gstlal_file_list()
        dfs = []

        for file in csv_list:
            df = pd.read_csv(file)

            if len(df):
                dfs.append(df)

        self.triggers = pd.concat(dfs, ignore_index=True)
        self.triggers = self.triggers[self.triggers['ifo'] == self.ifo].copy()

    def construct_gstlal_start_end(self) -> None:
        """
        Compute `tstart` and `tend` columns for each trigger.

        `tend` is calculated from `end_time` and `end_time_ns`.
        `tstart` is back-calculated using the `template_duration`.
        """
        self.triggers.loc[:, 'tend'] = (
                self.triggers['end_time'] + 1e-9 * self.triggers['end_time_ns']
            )

        self.triggers.loc[:, 'tstart'] = (
                self.triggers['tend'] - self.triggers['template_duration']
            )

    def condition_gstlal_triggers(self) -> pd.DataFrame:
        """
        Load and condition all GstLAL triggers.

        This includes loading CSVs, filtering by IFO, and calculating timing columns.

        Returns:
            pd.DataFrame: A DataFrame of conditioned GstLAL triggers.

        Raises:
            RuntimeError: If segment-based conditioning is requested (not supported).
        """
        if not self.segment:
            self.read_all_gstlal_csv()
            self.construct_gstlal_start_end()

        else:
            msg = "segment=True not supported yet"
            logger.error(msg)
            raise RuntimeError(msg)

        return self.triggers

    def return_max_start_end(self) -> Tuple[float, float]:
        """
        Return the global start and end bounds for the conditioned triggers.

        Returns:
            tuple[float, float]: (min_tstart, max_tend) for all triggers.
        """
        return min(self.triggers['tstart']), max(self.triggers['tend'])
