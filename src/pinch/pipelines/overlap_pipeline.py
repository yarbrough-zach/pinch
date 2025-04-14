#!/usr/bin/env python3

import argparse
import os

from pinch.handlers.gspy_handler import GravitySpyHandler
from pinch.handlers.omicron_handler import OmicronHandler
from pinch.handlers.gstlal_handler import GstlalHandler

from pinch.overlap_engine import OverlapEngine


class OverlapPipeline:
    """
    Pipeline to process and classify pipeline triggers based on glitch overlaps.

    This class orchestrates loading pipeline, Gravity Spy, and Omicron triggers,
    identifying time overlaps between them, and writing out categorized results.

    Attributes:
        ifo (str): Interferometer (e.g., 'H1', 'L1').
        args (Namespace): Parsed command-line arguments.
        pipeline_df (pd.DataFrame): DataFrame of pipeline triggers.
        gspy_df (pd.DataFrame): DataFrame of Gravity Spy triggers.
        omic_df (pd.DataFrame): DataFrame of Omicron triggers.
        separated_triggers (dict): Dictionary of DataFrames: clean, dirty, other.

    Methods:
        load_pipeline_triggers(): Load and process GstLAL triggers.
        load_gspy_triggers(): Query and prepare Gravity Spy triggers.
        load_omicron_triggers(): Load and condition Omicron triggers.
        run(): Perform full overlap analysis.
        write_output(separated_triggers=None): Write categorized triggers to disk.
    """
    def __init__(self, ifo, args):
        self.ifo = ifo
        self.args = args
        self.pipeline_df = None
        self.gspy_df = None
        self.omic_df = None

    def load_pipeline_triggers(self):
        """
        Load and condition GstLAL pipeline triggers using GstlalHandler.
        """
        gstlal_handler = GstlalHandler(self.args.pipeline_triggers, self.ifo)
        self.pipeline_df = gstlal_handler.condition_gstlal_triggers()

    def load_gspy_triggers(self):
        """
        Query and condition Gravity Spy triggers using the time bounds of pipeline triggers.
        """
        gspy_handler = GravitySpyHandler(self.ifo)

        gspy_handler.start = min(self.pipeline_df['tstart']) - 10
        gspy_handler.end = max(self.pipeline_df['tstart']) + 10

        self.gspy_df = gspy_handler.return_gspy_events()

    def load_omicron_triggers(self):
        """
        Load and condition Omicron triggers from the CSV path using OmicronHandler.
        """
        omic_handler = OmicronHandler(self.args.omicron_path)
        self.omic_df = omic_handler.condition_omicron()

    def run(self):
        """
        Execute the pipeline: load data, perform overlap analysis, and separate triggers.
        """
        self.load_pipeline_triggers()

        if self.args.gspy:
            self.load_gspy_triggers()

        if self.args.omicron:
            self.load_omicron_triggers()

        engine = OverlapEngine(
                self.pipeline_df,
                gspy_triggers=self.gspy_df,
                omicron_triggers=self.omic_df,
            )

        if self.gspy_df is not None:
            engine.find_gspy_overlaps()

        if self.omic_df is not None:
            engine.find_omicron_overlaps()

        engine.separate_triggers()

        self.separated_triggers = engine.return_separated_triggers()

    def write_output(self, separated_triggers=None):
        """
        Write separated trigger DataFrames (clean/dirty/other) to CSV files.

        Args:
            separated_triggers (dict, optional): Optional dictionary of categorized triggers.
        """
        os.makedirs(self.args.output_dir, exist_ok=True)

        if not separated_triggers:
            separated_triggers = self.separated_triggers

        for key, df in separated_triggers.items():
            df.to_csv(
                    f"{self.args.output_dir}/{self.ifo}_{key}.csv",
                    index=False,
                )


def parse_args():
    """
    Parse command-line arguments for the overlap pipeline.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Run glitch overlap engine")

    parser.add_argument('--ifos', required=True, nargs='+', help='IFOs to analyze')
    parser.add_argument('--pipeline-triggers', required=True, help='Path to pipeline trigger CSVs')
    parser.add_argument('--gspy', action='store_true', help='Enable Gravity Spy overlap')
    parser.add_argument('--omicron', action='store_true', help='Enable Omicron overlap')
    parser.add_argument(
            '--omicron-paths',
            type=str,
            help='Comma-separated list of IFO:path_to_omicron_csv pairs; e.g., H1:path/H1.csv,L1:/path/L1.csv')
    parser.add_argument('--output-dir', required=True, help='Path to write output CSVs')

    return parser.parse_args()


def main():
    """
    Entry point for the overlap pipeline CLI.

    Validates inputs, sets up per-IFO processing, and writes output CSVs.
    """
    args = parse_args()
    omicron_path_dict = {}

    if args.omicron and args.omicron_paths:
        try:
            for pair in args.omicron_paths.split(','):
                ifo, path = pair.split(':')
                omicron_path_dict[ifo] = path
        except ValueError:
            raise ValueError('--omicron-paths entry must be in IFO:path format')

    elif args.omicron and not args.omicron_paths:
        raise ValueError('--omicron specified but no omicron paths provided')

    elif args.omicron_paths and not args.omicron:
        raise ValueError('omicron paths provided without specifying --omicron')

    for ifo in args.ifos:
        print(ifo)

        if args.omicron:
            args.omicron_path = omicron_path_dict[ifo]

        pipeline = OverlapPipeline(ifo, args)

        pipeline.run()
        pipeline.write_output()


if __name__ == '__main__':
    main()
