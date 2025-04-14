#!/usr/bin/env python3

# given pipeline triggers and some glitch triggers
# find overlaps, assign values

class OverlapEngine:
    """
    A class to find and annotate overlaps between pipeline triggers and glitch triggers.

    This class compares pipeline triggers against Gravity Spy and Omicron glitch triggers
    to identify temporal overlaps. It then annotates the pipeline triggers with IDs from
    overlapping glitches and separates them into clean, dirty, and other subsets.

    Attributes:
        pipeline_triggers (pd.DataFrame): DataFrame of pipeline triggers to process.
        gspy_triggers (pd.DataFrame or None): Gravity Spy glitch triggers.
        omicron_triggers (pd.DataFrame or None): Omicron glitch triggers.
        dirty_pipeline_triggers (pd.DataFrame): Triggers with overlapping glitches.
        clean_pipeline_triggers (pd.DataFrame): Triggers with no overlaps.
        other_pipeline_triggers (pd.DataFrame): Triggers with only Omicron overlaps.

    Methods:
        find_gspy_overlaps(): Annotate pipeline triggers with overlapping Gravity Spy glitch IDs.
        find_omicron_overlaps(): Annotate pipeline triggers with overlapping Omicron glitch IDs.
        separate_triggers(): Partition triggers into clean, dirty, and other categories.
        return_separated_triggers(): Return a dict of clean, dirty, and other trigger DataFrames.
        return_pipeline_triggers(): Return the full annotated pipeline triggers DataFrame.
    """
    def __init__(self, pipeline_triggers, gspy_triggers=None, omicron_triggers=None):
        self.pipeline_triggers = pipeline_triggers
        self.gspy_triggers = gspy_triggers
        self.omicron_triggers = omicron_triggers

        for col in ['glitch_id', 'omic_id']:
            if col not in self.pipeline_triggers.columns:
                self.pipeline_triggers[col] = None

    def find_gspy_overlaps(self):
        """
        Annotate pipeline triggers that overlap with Gravity Spy glitches.

        Matches are determined by checking if trigger intervals overlap with
        Gravity Spy trigger intervals using one of four standard interval overlap cases.
        """
        for idx, (window_start, window_end, glitch_id) in enumerate(
                zip(self.gspy_triggers['tstart'], self.gspy_triggers['tend'], self.gspy_triggers['gravityspy_id'])):

            if idx % 1000 == 0:
                print(idx, '/', len(self.gspy_triggers))

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

    def find_omicron_overlaps(self):
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
                print(idx, '/', len(self.omicron_triggers))

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

    def separate_triggers(self):
        """
        Split pipeline triggers into clean, dirty, and other categories.

        - Dirty: overlaps with Gravity Spy or Omicron
        - Clean: no overlaps at all
        - Other: only overlaps with Omicron (no Gravity Spy)
        """
        mask_dirty = (self.pipeline_triggers['glitch_id'].notna()) | (self.pipeline_triggers['omic_id'].notna())
        mask_clean = (self.pipeline_triggers['glitch_id'].isna()) & (self.pipeline_triggers['omic_id'].isna())
        mask_other = (self.pipeline_triggers['glitch_id'].isna()) & (self.pipeline_triggers['omic_id'].notna())

        self.dirty_pipeline_triggers = self.pipeline_triggers[mask_dirty].copy()
        self.clean_pipeline_triggers = self.pipeline_triggers[mask_clean].copy()
        self.other_pipeline_triggers = self.pipeline_triggers[mask_other].copy()

        # duplicate gravityspy id as glitch id for merging
        # FIXME gross
        # self.gspy_triggers.loc[:, 'glitch_id'] = self.gspy_triggers['gravityspy_id']

        # merge on glitch_id
        self.dirty_pipeline_triggers = self.dirty_pipeline_triggers.merge(
                self.gspy_triggers.rename(columns={'gravityspy_id': 'glitch_id'})[['glitch_id', 'ml_confidence', 'ml_label']],
                on='glitch_id',
                how='left'
            )

    def return_separated_triggers(self):
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
