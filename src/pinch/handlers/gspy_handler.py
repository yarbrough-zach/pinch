#!/usr/bin/env python3
from __future__ import annotations

import os
import argparse

from dataclasses import dataclass
from typing import Optional, Iterable, Tuple
import pandas as pd
import logging

from gwpy.table import GravitySpyTable

from pinch.utils.chunk_parse import ChunkParse

logger = logging.getLoger(__name__)

# authentication for gspy database
def _as_time_range_from_df(
        df: pd.DataFrame,
        col: str,
        margin: float = 10.0
    ) -> Tuple[float, float]:

        if col not in df.columns or df.empty:
            msg = f"Expected non-empty DataFrame with column '{col}'."
            logger.error(msg)
            raise ValueError(msg)

        t0 = float(df[col].min()) - margin
        t1 = float(df[col].max()) + margin

        if t0 >= t1:
            msg = f"Bad time window: start {t0} >= end {t1}"
            logger.error(msg)
            raise ValueError(msg)

        return t0, t1


@dataclass
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
    ifo: str
    t_start: int | float
    t_end: int | float
    omicron_df: Optional[pd.DataFrame] = None
    ml_label: Optional[str] = None
    confidence: float = 0.9
   
    def __post_init__(self) -> None:
        if self.t_start >= self.t_end:
            msg = "t_start must be < t_end"
            logger.error(msg)
            raise ValueError(msg)

        if not (0.0 <= self.confidence <= 1):
            msg = "confidence must be between 0 and 1"
            logger.error(msg)
            raise ValueError(msg)

    @classmethod
    def from_time_range(
            cls,
            ifo: str,
            t_start: int | float,
            t_end: int | float,
            *,
            ml_label: Optional[str] = None,
            confidence: float =  0.9,
            omicron_df: Optional[pd.DataFrame] = None,
        ) -> 'GravitySpyHandler':
            t0 = float(t_start)
            t1 = float(t_end)
            if t0 >= t1:
                msg = f"t_start must be < t_end"
                logger.error(msg)
                raise ValueError(msg)

            return cls(
                    ifo=ifo, t_start=t0, t_end=t1,
                    ml_label=ml_label, confidence=confidence, omicron_df=omicron_df)

    @classmethod
    def from_omicron_df(
            cls,
            ifo: str,
            omicron_df: pd.DataFrame,
            margin: int | float = 10,
            *,
            ml_label: Optional[str] = None,
            confidence: float = 0.9,
        ) -> 'GravitySpyHandler':
            time_col = "tstart" if "tstart" in omicron_df.columns else "time"
            t0, t1 = _as_time_range_from_df(omicron_df, time_col, margin=margin)
            return cls.from_time_range(
                    ifo=ifo, t_start=t0, t_end=t1,
                    ml_label=ml_label, confidence=confidence, omicron_df=omicron_df)

    def fetch_gravity_spy_events(self) -> pd.DataFrame:
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

        glitches = glitches.to_pandas()

        if glitches.empty:
            msg = "No glitches retured for gspy query"
            logger.error(msg)
            raise RuntimeError(msg)
        
        self.glitches = glitches

        return self.glitches

    def construct_gspy_start_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute `tstart` and `tend` columns for each glitch in the DataFrame.

        `tstart` is computed from `start_time` and `start_time_ns`.
        `tend` is `tstart + duration`.

        If start_time and start_time_ns are not present, a corresponding
        Omicron df is necessary.
        """

        out = df.copy()
        
        # try to compute directly
        has_direct = {"start_time", "start_time_ns", "duration"}.issubset(out.columns)

        if has_direct:
            out["tstart"] = out["start_time"].astype(float) + 1e-9 * out["start_time_ns"].astype(float)
            out["tend"] = out["tstart"] + out["duration"].astype(float)

            self.glitches = out
            return out
        
        # if we can't, fall back to omicron
        if self.omicron_df is None or self.omicron_df.empty:
            msg = "no gspy start time and duration, no omicron df provided to compute them"
            logger.error(msg)
            raise ValueError(msg)

        o = self.omicron_df.sort_values(by=self._omic_time_col())
        g = out.sort_values(by='event_time')
        
        merged = pd.merge_asof(
                g, o, left_on="event_time", right_on=self._omic_time_col(),
                direction="nearest", tolerance=0.05
            )

        self.glitches = merged
        return self.glitches

        if "tstart" not in merged.columns or "tend" not in merged.columns:
            msg = "Unable to produce 'tstart'/'tend' after omicron merge."
            logger.error(msg)
            raise ValueError(msg)
        
    def _omic_time_col(self) -> str:
        return "tstart" if self.omicron_df is not None and "tstart" in self.omicron_df.columns else "time"

    def query_and_condition_gspy(self):
        """
        Perform a full glitch query and augment the results with `tstart` and `tend`.

        This combines fetching glitch data and computing additional timing fields.
        """
        df = self.fetch_gravity_spy_events()

        df = self.construct_gspy_start_end(df)
        
        return df

    def return_gspy_events(self):
        """
        Return glitch events with computed `tstart` and `tend` fields.

        Returns:
            pd.DataFrame: A DataFrame of glitches with full timing information.
        """
        return self.query_and_condition_gspy()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=float, help='GPS time of query start')
    parser.add_argument('--end', type=float, help='GPS time of query end')
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
        msg = "Insufficient time arguments provided"
        logger.error(msg)
        raise ValueError(msg)

    gspy_events = GravitySpyHandler(
            t_start=start,
            t_end=end,
            ml_label=args.ml_label,
        )

    glitches = gspy_events.fetch_gravity_spy_events()

    glitches.to_csv(
            f"{args.output_path}/chunk{args.chunk}_gspy.csv"
        )


if __name__ == '__main__':
    main()
