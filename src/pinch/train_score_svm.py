#!/usr/bin/env python3

import argparse
import logging

from pinch.pipelines.svm_pipeline import SVMPipeline
from pinch.utils.trigger_io import TIO

logger = logging.getLogger(__name__)

def parse_args():
    """
    Parse command-line arguments for SVM training and scoring.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Train and/or apply an OC-SVM to glitch trigger data")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # --- TRAIN MODE ---
    train_parser = subparsers.add_parser("train", help="Train an SVM model on clean triggers")
    train_parser.add_argument("--clean-triggers", required=True)
    train_parser.add_argument("--save-model", action="store_true")
    train_parser.add_argument("--model-path", default="trained_svm.pkl")

    # --- SCORE MODE ---
    score_parser = subparsers.add_parser("score", help="Score dirty triggers using trained model")
    score_parser.add_argument("--dirty-triggers", required=True)
    score_parser.add_argument("--model-path", required=True)
    score_parser.add_argument("--output-path", required=True)

    # --- TRAIN AND SCORE MODE ---
    both_parser = subparsers.add_parser("train_and_score", help="Train and immediately score triggers")
    both_parser.add_argument("--clean-triggers", required=True)
    both_parser.add_argument("--dirty-triggers", required=True)
    both_parser.add_argument("--output-path", required=True)
    both_parser.add_argument("--save-model", action="store_true")
    both_parser.add_argument("--model-path", default="trained_svm.pkl")

    return parser.parse_args()


def main():
    """
    Entry point for the SVM pipeline CLI.

    Run the SVMPipeline using the selected mode.

    Modes:
        - train: Only train the model.
        - score: Only evaluate triggers using a saved model.
        - train_and_score: Train and immediately evaluate.
    """
    args = parse_args()

    clean_trigger_dict = TIO.read(args.clean_triggers)
    dirty_trigger_dict = TIO.read(args.dirty_triggers)

    assert set(clean_trigger_dict.keys()) == set(dirty_trigger_dict.keys()), "Mismatch in IFO keys"

    for ifo in clean_trigger_dict.keys():
        pipeline = SVMPipeline(
                clean_df=clean_trigger_dict[ifo],
                dirty_df=dirty_trigger_dict[ifo],
                output_path=args.output_path,
            )

        if args.mode == "train":
            pipeline.train()
        elif args.mode == "score":
            pipeline.evaluate()
        elif args.mode == "train_and_score":
            pipeline.train()
            pipeline.evaluate()
            pipeline.save_scored_data()
        else:
            msg = f"Unsupported mode: {args.mode}"
            logger.error(msg)
            raise ValueError(msg)


if __name__ == '__main__':
    main()
