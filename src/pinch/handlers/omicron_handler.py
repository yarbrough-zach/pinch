#! /usr/bin/env python3

import argparse
import pandas as pd


class OmicronHandler:
    """
    A handler class for loading, filtering, and conditioning Omicron trigger data.

    This class reads an Omicron trigger CSV file, applies a signal-to-noise ratio (SNR)
    cut, and computes start and end times for each trigger.

    Attributes:
        omics (pd.DataFrame): DataFrame containing Omicron triggers.

    Methods:
        read_omicron_csv(csv_path): Load Omicron triggers from CSV.
        apply_omicron_snr_cut(omicron_snr_cut): Filter triggers by SNR.
        construct_omicron_start_end(): Add `tstart` and `tend` columns.
        condition_omicron(): Apply all processing steps and return the result.
    """
    def __init__(self, csv_path):
        self.omics = self.read_omicron_csv(csv_path)

    def read_omicron_csv(self, csv_path):
        """
        Read Omicron triggers from a CSV file.

        Args:
            csv_path (str): Path to the CSV file.

        Returns:
            pd.DataFrame: DataFrame containing raw Omicron triggers.
        """
        return pd.read_csv(csv_path)

    def apply_omicron_snr_cut(self, omicron_snr_cut=5.5):
        """
        Apply a minimum SNR cut to the Omicron triggers.

        Args:
            omicron_snr_cut (float): Minimum SNR value to retain (default: 5.5).
        """
        # mutate attr in place
        self.omics = self.omics[self.omics['snr'] >= omicron_snr_cut].copy()

    def construct_omicron_start_end(self):
        """
        Construct `tstart` and `tend` columns for the Omicron triggers.

        `tstart` is calculated from `start_time` and `start_time_ns`.
        `tend` is computed as `tstart + duration`.
        """

        if 'tstart' in self.omics.columns:
            print('tstart and tend already present...')

        else:

            self.omics.loc[:, 'tstart'] = (
                    self.omics['start_time'] + 1e-9 * self.omics['start_time_ns']
                )

            self.omics.loc[:, 'tend'] = (
                    self.omics['tstart'] + self.omics['duration']
                )

    def condition_omicron(self):
        """
        Apply SNR cut and compute start/end times for Omicron triggers.

        Returns:
            pd.DataFrame: Filtered and augmented Omicron triggers.
        """
        self.apply_omicron_snr_cut()
        self.construct_omicron_start_end()

        return self.omics


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--omicron-trigger-path', type=str, help='path to csv file containing omicron triggers')
    args = parser.parse_args()

    omic_obj = OmicronHandler(args.omicron_trigger_path)

    conditioned_omicron_triggers = omic_obj.condition_omicron()

    print('len conditioned triggers: ', len(conditioned_omicron_triggers))


if __name__ == '__main__':
    main()
