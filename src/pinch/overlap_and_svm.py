#!/usr/bin/env python3

import argparse

from pinch.pipelines.overlap_pipeline import OverlapPipeline
from pinch.pipelines.svm_pipeline import SVMPipeline


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

    return parser.parse_args()


def main():
    """
    Entry point for the overlap pipeline CLI.

    Validates inputs, sets up per-IFO processing, and writes output CSVs.
    """
    args = parse_args()
    omicron_path_dict = {}

    if args.omicron and args.omicron_paths:
        print('omicron paths', args.omicron_paths)
        try:
            for pair in args.omicron_paths.split(','):
                print(pair)
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

        print('len clean df:', len(clean_df))

        svm = SVMPipeline(
                clean_df=clean_df,
                dirty_df=dirty_df,
                output_path=args.scored_output_path
            )

        if not args.score_only:
            svm.train()

        scored_df = svm.evaluate()

        output_path = f"{args.scored_output_path}/{ifo}_scored_output.csv"
        print(output_path)
        scored_df.to_csv(f"{output_path}")


if __name__ == '__main__':
    main()
