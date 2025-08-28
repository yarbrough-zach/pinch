#!/usr/bin/env python3

import pandas as pd
import logging

from typing import Optional, Dict, List, Any, Union

from intervaltree import IntervalTree
from collections import defaultdict

logger = logging.getLogger(__name__)

class OverlapEngine:
    """
    A class to identify and annotate overlaps between pipeline triggers and glitch triggers.

    This engine compares pipeline triggers against Gravity Spy and Omicron glitch triggers
    to identify temporal overlaps. It supports two methods for overlap detection: a
    straightforward pandas-based approach (deprecated) and an efficient interval tree-based
    approach (recommended).

    Attributes:
        pipeline_triggers (pd.DataFrame): DataFrame of triggers from the search pipeline.
        gspy_triggers (pd.DataFrame or None): Gravity Spy glitch triggers (with 'tstart', 'tend', and 'gravityspy_id').
        omicron_triggers (pd.DataFrame or None): Omicron glitch triggers (with 'tstart', 'tend').
        dirty_pipeline_triggers (pd.DataFrame): Triggers with any glitch overlaps.
        clean_pipeline_triggers (pd.DataFrame): Triggers with no overlaps.
        other_pipeline_triggers (pd.DataFrame): Triggers with only Omicron overlaps.

    Methods:
        find_gspy_overlaps(): Use interval trees to find and annotate overlaps with Gravity Spy glitches.
        find_omicron_overlaps(): Use interval trees to find and annotate overlaps with Omicron glitches.
        separate_triggers(): Categorize triggers into clean, dirty, and other.
        return_separated_triggers(): Return dictionary of clean, dirty, and other triggers.
        return_pipeline_triggers(): Return the full annotated pipeline trigger DataFrame.
    """

    def __init__(
            self,
            pipeline_triggers: pd.DataFrame,
            gspy_triggers: Optional[pd.DataFrame] = None,
            omicron_triggers: Optional[pd.DataFrame] = None
        ) -> None:
            self.pipeline_triggers = pipeline_triggers
            self.gspy_triggers = gspy_triggers
            self.omicron_triggers = omicron_triggers

        for col in ['glitch_id', 'omic_id']:
            if col not in self.pipeline_triggers.columns:
                self.pipeline_triggers[col] = None

    def find_gspy_overlaps_masks(self) -> None:
        """
        Annotate pipeline triggers that overlap with Gravity Spy glitches.

        Matches are determined by checking if trigger intervals overlap with
        Gravity Spy trigger intervals using one of four standard interval overlap cases.
        """

        for idx, (window_start, window_end, glitch_id) in enumerate(
                zip(self.gspy_triggers['tstart'], self.gspy_triggers['tend'], self.gspy_triggers['gravityspy_id'])):

            if idx % 1000 == 0:
                logger.info(f"Gspy Progress: {idx} / {len(self.gspy_triggers)}")

            case1_mask = (self.pipeline_triggers['tstart'] > window_start) & (self.pipeline_triggers['tend'] < window_end)
            case2_mask = (
                    (self.pipeline_triggers['tend'] > window_start) &
                    (self.pipeline_triggers['tstart'] < window_start) &
                    (self.pipeline_triggers['tend'] < window_end)
                )
            case3_mask = (
                    (self.pipeline_triggers['tend'] > window_end) &
                    (self.pipeline_triggers['tstart'] < window_end) &
                    (self.pipeline_triggers['tstart'] > window_start)
                )
            case4_mask = (
                    (self.pipeline_triggers['tstart'] < window_start) &
                    (self.pipeline_triggers['tend'] > window_end)
                )

            combined_mask = case1_mask | case2_mask | case3_mask | case4_mask

            affected_indicies = self.pipeline_triggers.index.values[combined_mask]

            self.pipeline_triggers.loc[affected_indicies, 'glitch_id'] = glitch_id

    def find_gspy_overlaps_tree(self) -> None:
        """
        Annotate pipeline triggers with overlapping Gravity Spy glitch IDs using an interval tree.

        This method builds an interval tree from Gravity Spy glitch intervals and efficiently
        queries it for overlaps against each pipeline trigger. All overlapping Gravity Spy glitch
        IDs are recorded in a list per trigger in the 'glitch_id' column.

        This replaces the older mask-based approach and supports multiple overlaps per trigger.
        """

        self.tree = IntervalTree()

        for idx, row in self.gspy_triggers.iterrows():
            self.tree[row['tstart']:row['tend']] = row['gravityspy_id']

        trigger_glitch_map = defaultdict(list)

        for idx, row in self.pipeline_triggers.iterrows():
            overlaps = self.tree.overlap(row['tstart'], row['tend'])

            if overlaps:
                trigger_glitch_map[idx] = [iv.data for iv in overlaps]

        self.pipeline_triggers['glitch_id'] = self.pipeline_triggers.index.map(lambda i: trigger_glitch_map.get(i, []))

    def find_omicron_overlaps_tree(self) -> None:
        """
        Annotate pipeline triggers with overlapping Omicron glitch indices using an interval tree.

        This method builds an interval tree from Omicron glitch intervals and efficiently queries it
        for overlaps against each pipeline trigger. All overlapping Omicron glitch indices (i.e.,
        DataFrame row indices) are recorded in a list per trigger in the 'omic_id' column.

        This replaces the older mask-based approach and supports multiple overlaps per trigger.
        """

        self.omicron_tree = IntervalTree()

        for idx, row in self.omicron_triggers.iterrows():
            self.omicron_tree[row['tstart']:row['tend']] = idx

            if idx % 1000 == 0:
                logger.info(f"Omicron progress: {idx} / {len(self.omicron_triggers)}")

        trigger_glitch_map = defaultdict(list)

        for idx, row in self.pipeline_triggers.iterrows():

            overlaps = self.omicron_tree.overlap(row['tstart'], row['tend'])

            if overlaps:
                trigger_glitch_map[idx] = [iv.data for iv in overlaps]

        self.pipeline_triggers['omic_id'] = self.pipeline_triggers.index.map(lambda i: trigger_glitch_map.get(i, []))

    def find_omicron_overlaps_masks(self) -> None:
        """
        Annotate pipeline triggers that overlap with Omicron glitches.

        Similar to the Gravity Spy case, overlap is determined using four
        standard interval intersection conditions.
        """

        for idx, (window_start, window_end, glitch_id) in enumerate(
                zip(
                    self.omicron_triggers['tstart'],
                    self.omicron_triggers['tend'],
                    self.omicron_triggers.index.values
                )
        ):

            if idx % 1000 == 0:
                logger.info(f"Omicron progress: {idx} / {len(self.omicron_triggers)}")

            case1_mask = (
                    (self.pipeline_triggers['tstart'] > window_start) &
                    (self.pipeline_triggers['tend'] < window_end)
                )
            case2_mask = (
                    (self.pipeline_triggers['tend'] > window_start) &
                    (self.pipeline_triggers['tstart'] < window_start) &
                    (self.pipeline_triggers['tend'] < window_end)
                )
            case3_mask = (
                    (self.pipeline_triggers['tend'] > window_end) &
                    (self.pipeline_triggers['tstart'] < window_end) &
                    (self.pipeline_triggers['tstart'] > window_start)
                )
            case4_mask = (
                    (self.pipeline_triggers['tstart'] < window_start) &
                    (self.pipeline_triggers['tend'] > window_end)
                )

            combined_mask = case1_mask | case2_mask | case3_mask | case4_mask

            affected_indicies = self.pipeline_triggers.index.values[combined_mask]

            self.pipeline_triggers.loc[affected_indicies, 'omic_id'] = glitch_id

    @staticmethod
    def ensure_list(x: Any) -> List[Any]:

        if isinstance(x, list):
            return x

        if isinstance(x, np.ndarray):
            return list(x)

        if x is None or (isinstance(x, float) and pd.isna(x)):
            return []

        return [x]

    def separate_triggers(self) -> None:
        """
        Split pipeline triggers into clean, dirty, and other categories.

        - Dirty: overlaps with Gravity Spy or Omicron
        - Clean: no overlaps at all
        - Other: only overlaps with Omicron (no Gravity Spy)
        """

        # make sure glitch_id and omic_id are lists for backwards compatibility
        self.pipeline_triggers['glitch_id'] = self.pipeline_triggers['glitch_id'].apply(self.ensure_list)
        self.pipeline_triggers['omic_id'] = self.pipeline_triggers['omic_id'].apply(self.ensure_list)

        self.pipeline_triggers.loc[:, 'num_glitch_overlaps'] = self.pipeline_triggers['glitch_id'].apply(len)
        self.pipeline_triggers.loc[:, 'num_omic_overlaps'] = self.pipeline_triggers['omic_id'].apply(len)

        mask_dirty = (self.pipeline_triggers['num_glitch_overlaps'] > 0) | (self.pipeline_triggers['num_omic_overlaps'] > 0)
        mask_clean = (self.pipeline_triggers['num_glitch_overlaps'] == 0) & (self.pipeline_triggers['num_omic_overlaps'] == 0)
        mask_other = (self.pipeline_triggers['num_glitch_overlaps'] == 0) & (self.pipeline_triggers['num_omic_overlaps'] > 0)

        self.dirty_pipeline_triggers = self.pipeline_triggers[mask_dirty].copy()
        self.clean_pipeline_triggers = self.pipeline_triggers[mask_clean].copy()
        self.other_pipeline_triggers = self.pipeline_triggers[mask_other].copy()

        # duplicate gravityspy id as glitch id for merging
        # FIXME gross
        # self.gspy_triggers.loc[:, 'glitch_id'] = self.gspy_triggers['gravityspy_id']

        self.dirty_pipeline_triggers.loc[:, 'trigger_group_id'] = self.dirty_pipeline_triggers.index

        self.dirty_pipeline_triggers = self.dirty_pipeline_triggers.explode('glitch_id')

        # merge on glitch_id
        self.dirty_pipeline_triggers = self.dirty_pipeline_triggers.merge(
                self.gspy_triggers.rename(columns={'gravityspy_id': 'glitch_id'})[['glitch_id', 'ml_confidence', 'ml_label']],
                on='glitch_id',
                how='left'
            )

    def return_separated_triggers(self) -> Dict[str, pd.DataFrame]:
        """
        Return clean, dirty, and other pipeline triggers as a dictionary.

        Returns:
            dict: {'clean': DataFrame, 'dirty': DataFrame, 'other': DataFrame}
        """

        return {
            'clean': self.clean_pipeline_triggers,
            'dirty': self.dirty_pipeline_triggers,
            'other': self.other_pipeline_triggers,
        }

    def return_pipeline_triggers(self):
        """
        Return the full annotated pipeline trigger DataFrame.

        Returns:
            pd.DataFrame: Pipeline triggers with glitch/omicron annotations.
        """

        return self.pipeline_triggers
