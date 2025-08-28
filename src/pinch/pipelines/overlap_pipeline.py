#!/usr/bin/env python3

import os

from pinch.handlers.gspy_handler import GravitySpyHandler
from pinch.handlers.omicron_handler import OmicronHandler
from pinch.handlers.gstlal_handler import GstlalHandler

from pinch.pipelines.overlap_engine import OverlapEngine


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
    def __init__(
            self,
            ifo,
            pipeline_trigger_path,
            output_dir,
            gspy_enabled=False,
            omicron_enabled=False,
            omicron_path=None,
    ):
        self.ifo = ifo
        self.pipeline_trigger_path = pipeline_trigger_path
        self.output_dir = output_dir
        self.gspy_enabled = gspy_enabled
        self.omicron_enabled = omicron_enabled
        self.omicron_path = omicron_path

        self.pipeline_df = None
        self.gspy_df = None
        self.omic_df = None
        self.separated_triggers = {}

    def load_pipeline_triggers(self):
        """
        Load and condition GstLAL pipeline triggers using GstlalHandler.
        """
        gstlal_handler = GstlalHandler(self.pipeline_trigger_path, self.ifo)
        self.pipeline_df = gstlal_handler.condition_gstlal_triggers()

    def load_gspy_triggers(self):
        """
        Query and condition Gravity Spy triggers using the time bounds of pipeline triggers.
        """
        gspy_handler = GravitySpyHandler.from_omicron_df(
                self.ifo,
                omicron_df=self.omic_df,
                margin=10.0,
            )

        #gspy_handler.start = min(self.pipeline_df['tstart']) - 10
        #gspy_handler.end = max(self.pipeline_df['tstart']) + 10

        self.gspy_df = gspy_handler.return_gspy_events()

    def load_omicron_triggers(self):
        """
        Load and condition Omicron triggers from the CSV path using OmicronHandler.
        """
        start = min(self.pipeline_df['tstart']) - 10
        end = max(self.pipeline_df['tstart']) + 10

        omic_handler = OmicronHandler(self.omicron_path, start=start, end=end)

        self.omic_df = omic_handler.condition_omicron()

    def run(self):
        """
        Execute the pipeline: load data, perform overlap analysis, and separate triggers.
        """
        self.load_pipeline_triggers()

        if self.omicron_enabled:
            self.load_omicron_triggers()

        if self.gspy_enabled:
            self.load_gspy_triggers()

        engine = OverlapEngine(
                self.pipeline_df,
                gspy_triggers=self.gspy_df,
                omicron_triggers=self.omic_df,
            )

        if self.gspy_df is not None:
            engine.find_gspy_overlaps_tree()

        if self.omic_df is not None:
            engine.find_omicron_overlaps_tree()

        engine.separate_triggers()

        self.separated_triggers = engine.return_separated_triggers()

    def write_output(self, separated_triggers=None):
        """
        Write separated trigger DataFrames (clean/dirty/other) to CSV files.

        Args:
            separated_triggers (dict, optional): Optional dictionary of categorized triggers.
        """
        os.makedirs(self.output_dir, exist_ok=True)

        if not separated_triggers:
            separated_triggers = self.separated_triggers

        for key, df in separated_triggers.items():
            df.to_csv(
                    f"{self.output_dir}/{self.ifo}_{key}.csv",
                    index=False,
                )
