#! /usr/bin/env python3

import os
import socket
import argparse
import pandas as pd

from gwtrigfind import find_trigger_files
from gwpy.table import EventTable


class OmicronFinder:
    def __init__(self):
        self.host = socket.getfqdn()

    def det_site(self):
        if self.host.split('.')[1] == 'ligo-la':
            ifo = 'L1'

        elif self.host.split('.')[1] == 'ligo-wa':
            ifo = 'H1'

        else:
            raise ValueError("Hostname {self.host} not recognized, "
                    "make sure you are running this script on the LLO or LHO cluster")

        return ifo

    def fetch_and_save_omicron(self, start, end, output_path=None, channel=None, save=False):
        
        ifo = self.det_site()
        
        if not channel:
            channel = f"{ifo}:GDS-CALIB_STRAIN_CLEAN"

        else:
            channel = f"{ifo}{channel}"
            print(f'Channel name provided, searching for {channel}')

        print(f"Searching between {start} and {end} for h5 files...")
        omicron_files = find_trigger_files(channel, 'omicron', start, end, ext='h5')

        if not len(omicron_files):
            print('No h5 files found, trying xml...')
            omicron_files = find_trigger_files(channel, 'omicron', start, end)

        if not len(omicron_files):
            raise RuntimeError("No omicron files found")

        omicron_events = EventTable.read(omicron_files, path='triggers') 

        omicron_events = omicron_events.to_pandas()

        if save and output_path:
            output_path = os.path.join(output_path, f"{ifo}_omicron_{start}_{end}.csv")
            omicron_events.to_csv(output_path)

        return omicron_events


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, help='gps start time for query')
    parser.add_argument('--end', type=str, help='gps end time for query')
    parser.add_argument('--output-path', type=str, help='output path for csv of omicron triggers')
    parser.add_argument('--save', action='store_true', help='Bool, whether or not to save omicron triggers as csv')

    args = parser.parse_args()

    omic = OmicronFinder()

    if args.save and args.output_path:
        fetch_and_save_omicron(args.start, args.end, args.output_path, save=True)

    else:
        omicron_events = omic.fetch_and_save_omicron(args.start, args.end)

        print('Omicron events found:')
        print(omicron_events)
        print('If you want to save these to disk, run again with --output_path and --save')
