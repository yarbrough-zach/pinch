#!/usr/bin/env python3

import argparse
import logging

import pandas as pd

from pinch.pipelines.overlap_pipeline import OverlapPipeline
from pinch.pipelines.svm_pipeline import SVMPipeline

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Run glitch overlap pipeline and then train/score an SVM on the results")

    parser.add_argument('--ifos', required=True, nargs='+', help='IFOs to analyze')
    parser.add_argument('--pipeline-triggers', required=True, help='Path to pipeline trigger CSVs')
    parser.add_argument('--output-dir', required=True, help='Path to write output CSVs')

    parser.add_argument('--gspy', action='store_true', help='Enable Gravity Spy overlap')
    parser.add_argument('--omicron', action='store_true', help='Enable Omicron overlap')
    parser.add_argument(
            '--omicron-paths',
            type=str,
            help='Comma-separated list of IFO:path_to_omicron_csv pairs; e.g., H1:path/H1.csv,L1:/path/L1.csv')

    parser.add_argument('--save-model', action='store_true', help='Save the trained SVM model')
    parser.add_argument('--model-path', default='trained_svm.pkl', help='Path to save/load the SVM model')
    parser.add_argument('--score-only', action='store_true', help='Skip training and only score dirty tiggers')
    parser.add_argument('--scored-output-path', required=True, help='Base path to write SVM-scored CSVs')

    args = parser.parse_args()

    # argument cross-checks
    if args.omicron and not args.omicron_paths:
        parser.error("--omicron specified but no --omicron-paths provided")
    if args.omicron_paths and not args.omicron:
        parser.error("--omicron-paths provided without --omicron")

    return args


def main():
    """
    Entry point for the overlap pipeline CLI.

    Validates inputs, sets up per-IFO processing, and writes output CSVs.
    """
    args = parse_args()
    omicron_path_dict = {}

    if args.omicron and args.omicron_paths:
        logger.info(f"omicron paths: {args.omicron_paths}")
 
        try:
            for pair in args.omicron_paths.split(','):
 
                ifo, path = pair.split(':')
                omicron_path_dict[ifo] = path
        except ValueError:
            msg = '--omicron-paths entry must be in IFO:path format'
            logger.exception(msg)
            raise ValueError(msg)

    elif args.omicron and not args.omicron_paths:
        msg = '--omicron specified but no omicron paths provided'
        logger.error(msg)
        raise ValueError(msg)

    elif args.omicron_paths and not args.omicron:
        msg = 'omicron paths provided without specifying --omicron'
        logger.error(msg)
        raise ValueError(msg)

    for ifo in args.ifos:
        logger.info(f"Processing {ifo}...")

        omicron_path = omicron_path_dict.get(ifo) if args.omicron else None

        overlap = OverlapPipeline(
                ifo=ifo,
                pipeline_trigger_path=args.pipeline_triggers,
                output_dir=args.output_dir,
                gspy_enabled=args.gspy,
                omicron_enabled=args.omicron,
                omicron_path=omicron_path
            )

        overlap.run()
        overlap.write_output()

        clean_df = overlap.separated_triggers.get("clean")
        dirty_df = overlap.separated_triggers.get("dirty")

        logger.info(f"len clean df: {len(clean_df)}")

        svm = SVMPipeline(
                clean_df=clean_df,
                dirty_df=dirty_df,
                output_path=args.scored_output_path
            )

        if not args.score_only:
            svm.train()

        scored_df = svm.evaluate()

        # FIXME this is hack-y
        # figure out why non-numeric values being added to svm_score
        # and why there are unnamed columns
        # and maybe refactor trigger_group_id and omic_id

        # drop columns problematic for duckdb
        for column in ['trigger_group_id', 'omic_id']:
            if column in scored_df.columns:
                scored_df = scored_df.drop(columns=[column])

        # check for weird svm scores that had non numeric characters
        if 'svm_score' in scored_df.columns:
            svm_numeric = pd.to_numeric(scored_df['svm_score'], errors='coerce')
            non_numeric_mask = svm_numeric.isna() & scored_df['svm_score'].notna()
            num_non_numeric = non_numeric_mask.sum()

            if num_non_numeric > 0:
                logger.debug(f"Found {num_non_numeric} non-numeric svm_score entries.")

                if num_non_numeric < 10:
                    scored_df = scored_df[~non_numeric_mask]
                    logger.debug(f"Dropped {num_non_numeric} rows with non-numeric svm_score.")
                else:
                    logger.debug("Too many non-numeric svm_score entries (>10); file left unchanged.")
                    continue  # Skip overwriting this file

        # drop unnamed columns
        scored_df = scored_df.loc[:, ~scored_df.columns.str.startswith("Unnamed")]

        output_path = f"{args.scored_output_path}/{ifo}_scored_output.csv"
        logger.info(f"Saving output to {output_path}")
        scored_df.to_csv(f"{output_path}", index=False)


if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,  # or use: level=getattr(logging, os.getenv("LOGLEVEL", "INFO").upper(), logging.INFO)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    main()
