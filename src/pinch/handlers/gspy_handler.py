#!/usr/bin/env python3

import os
import argparse

from gwpy.table import GravitySpyTable

from pinch.utils.chunk_parse import ChunkParse

# export authentication for gspy database
class GravitySpyHandler:
    """
    A handler for querying and processing Gravity Spy glitch data.

    This class interfaces with the Gravity Spy database using GWPy's GravitySpyTable
    and provides methods to fetch glitch events, compute derived time intervals,
    and return the results as a pandas DataFrame.

    Attributes:
        ifo (str): Interferometer identifier (e.g., 'H1', 'L1').
        start (int or float): GPS start time of the query.
        end (int or float): GPS end time of the query.
        ml_label (str, optional): Glitch class to filter by (e.g., 'Koi_Fish').
        confidence (float): Minimum confidence threshold for returned events.
        glitches (pd.DataFrame or None): DataFrame of queried glitch data.

    Methods:
        fetch_gravity_spy_events():
            Fetch glitch events from the Gravity Spy database.

        construct_gspy_start_end():
            Add `tstart` and `tend` columns to the DataFrame based on start_time and duration.

        query_and_condition_gspy():
            Fetch glitch data and compute additional timing fields.

        return_gspy_events():
            Query, condition, and return glitch events as a DataFrame.
    """
    def __init__(
        self,
        ifo,
        omicron_df=None,
        t_start=None,
        t_end=None,
        ml_label=None,
        confidence=0.0,
    ):

        self.ifo = ifo
        self.omicron_df = omicron_df
        self.start = t_start
        self.end = t_end
        self.ml_label = ml_label
        self.confidence = confidence
        self.glitches = None

    def fetch_gravity_spy_events(self):
        """
        Query the Gravity Spy database for glitches within the specified time range
        and optional class and confidence criteria.

        Returns:
            pd.DataFrame: A DataFrame of glitch events matching the query.

        Raises:
            ValueError: If no glitch events are returned.
        """
        if not self.ml_label:
            glitches = GravitySpyTable.fetch(
                    "gravityspy",
                    "glitches_v2d0",
                    selection=(
                        f"ifo='{self.ifo}' && "
                        f"event_time > {self.start} && "
                        f"event_time < {self.end} && "
                        f"ml_confidence >={self.confidence}",
                    ),
                )

        else:
            glitches = GravitySpyTable.fetch(
                    "gravityspy",
                    "glitches_v2d0",
                    selection=(
                        f"ifo='{self.ifo}' && "
                        f"event_time > {self.start} && "
                        f"event_time < {self.end} && "
                        f"ml_label={self.ml_label} && "
                        f"ml_confidence >={self.confidence}",
                    ),
                )

        if not glitches:
            raise ValueError("No glitches retured for query")

        self.glitches = glitches.to_pandas()

        return self.glitches

    def construct_gspy_start_end(self):
        """
        Compute `tstart` and `tend` columns for each glitch in the DataFrame.

        `tstart` is computed from `start_time` and `start_time_ns`.
        `tend` is `tstart + duration`.

        If start_time and start_time_ns are not present, a corresponding
        Omicron df is necessary.
        """

        try:
            self.glitches.loc[:, 'tstart'] = (
                    self.glitches['start_time'] + 1e-9 * self.glitches['start_time_ns']
                )

            self.glitches.loc[:, 'tend'] = (
                    self.glitches['tstart'] + self.glitches['duration']
                )

        except TypeError as e:
            print('start_time not included, searching for data in omicron...')
            print(e)

            if not self.omicron_df:
                raise AttributeError("No omicron df attr provided while attempting to cross reference gravity spy triggers with omicron df")

            # merge gspy and omicron dfs on time to find the tstart and tend of gspy glitch
            merged_df = self.glitches.merge(
                    self.omicron_df,
                    left_on="event_time",
                    right_on="time",
                    how="inner"
                )

            self.glitches = merged_df

        assert 'tstart' in self.glitches.columns

    def query_and_condition_gspy(self):
        """
        Perform a full glitch query and augment the results with `tstart` and `tend`.

        This combines fetching glitch data and computing additional timing fields.
        """
        _ = self.fetch_gravity_spy_events()

        self.construct_gspy_start_end()

    def return_gspy_events(self):
        """
        Return glitch events with computed `tstart` and `tend` fields.

        Returns:
            pd.DataFrame: A DataFrame of glitches with full timing information.
        """
        self.query_and_condition_gspy()

        return self.glitches


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, help='GPS time of query start')
    parser.add_argument('--end', type=int, help='GPS time of query end')
    parser.add_argument('--chunk-definition-file', type=str, help='Path to file containing chunk definitions')
    parser.add_argument('--chunk', type=str)
    parser.add_argument('--ml-label', type=str, help='Label of glitch class to query, optional')
    parser.add_argument('--output-path', type=str)
    args = parser.parse_args()

    if args.chunk_definition_file and args.chunk:

        chunkparse = ChunkParse()
        start, end = chunkparse.parse_chunk_file(
                args.chunk,
                args.chunk_definition_file,
            )

    elif args.start and args.end:
        start = args.start
        end = args.end

    else:
        raise ValueError("Insufficient time arguments provided")

    gspy_events = GravitySpyHandler(
            t_start=start,
            t_end=end,
            ml_label=args.ml_label,
        )

    glitches = gspy_events.fetch_gravity_spy_events()

    glitches.to_pandas().to_csv(
            f"{args.output_path}/chunk{args.chunk}_gspy.csv"
        )


if __name__ == '__main__':
    main()
