#!/usr/bin/env python3

import argparse
import os
import pandas as pd

from pinch.models.one_class_svm import SVMClassifier


class SVMPipeline:
    """
    A pipeline to train and apply a one-class SVM model to glitch trigger data.

    This class can operate in three modes:
        - "train": Train a model on clean triggers.
        - "score": Apply a trained model to dirty triggers.
        - "train_and_score": Train a model and immediately apply it.

    Attributes:
        args (Namespace): Parsed command-line arguments.
        trainer (SVMClassifier or None): The trained SVM model.
        clean_df (pd.DataFrame or None): Clean glitch triggers for training.
        dirty_df (pd.DataFrame or None): Dirty glitch triggers for scoring.
        scored_df (pd.DataFrame or None): Scored triggers after evaluation.

    Methods:
        load_clean_triggers(): Load clean data from file or directory.
        load_dirty_triggers(): Load dirty data from file or directory.
        train(): Train the SVM model and optionally save it.
        evaluate(): Apply the trained model to dirty triggers.
        save_scored_data(): Write scored triggers to disk.
    """
    def __init__(self, args):
        self.args = args
        self.trainer = None
        self.clean_df = None
        self.dirty_df = None

    def load_clean_triggers(self):
        """
        Load clean trigger data from a CSV file or directory.
        """
        self.clean_df = self._load_trigger_dir(self.args.clean_triggers)

    def load_dirty_triggers(self):
        """
        Load dirty trigger data from a CSV file or directory.
        """
        self.dirty_df = self._load_trigger_dir(self.args.dirty_triggers)

    def train(self):
        """
        Train a one-class SVM model on clean data.

        Optionally saves the trained model to a file.
        """
        self.load_clean_triggers()
        self.trainer = SVMClassifier.train_from_data(self.clean_df)

        if self.args.save_model:
            os.makedirs(os.path.dirname(self.args.model_path), exist_ok=True)
            self.trainer.save_model(self.args.model_path)

    def evaluate(self):
        """
        Score dirty triggers using the trained SVM model.

        Raises:
            ValueError: If no model is provided or trained.
        """
        self.load_dirty_triggers()

        if self.trainer is None:
            if not self.args.model_path:
                raise ValueError("No model speficied for evaluation")
            self.trainer = SVMClassifier.load_model(self.args.model_path)

        scored_df = self.trainer.evaluate(self.dirty_df)

        self.scored_df = scored_df

    def save_scored_data(self):
        """
        Save scored dirty triggers to the specified output path.

        Raises:
            ValueError: If no scored data is available.
        """
        if not hasattr(self, 'scored_df') or self.scored_df is None:
            raise ValueError("No scored data available, did you forget to call evaluate()?")

        output_path = self.args.output_path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self._write_scored_df(self.scored_df, output_path)

    @staticmethod
    def _load_trigger_dir(path):
        """
        Load CSV data from a file or concatenate from a directory.

        Args:
            path (str): Path to a CSV file or directory containing CSVs.

        Returns:
            pd.DataFrame: Combined DataFrame of all loaded data.

        Raises:
            ValueError: If path is neither a file nor a directory.
        """
        if os.path.isfile(path):
            return pd.read_csv(path)

        elif os.path.isdir(path):
            return pd.concat(
                    [pd.read_csv(os.path.join(path, f)) for f in sorted(os.listdir(path)) if f.endswith('.csv')],
                    ignore_index=True
                )

        else:
            raise ValueError(f"Invalid path: {path} is neither a file nor a directory")

    @staticmethod
    def _write_scored_df(df, path):
        """
        Write a scored DataFrame to a CSV file.

        Args:
            df (pd.DataFrame): Data to write.
            path (str): Output file path.
        """
        df.to_csv(path, index=False)


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
    pipeline = SVMPipeline(args)

    if args.mode == "train":
        pipeline.train()
    elif args.mode == "score":
        pipeline.evaluate()
    elif args.mode == "train_and_score":
        pipeline.train()
        pipeline.evaluate()
        pipeline.save_scored_data()
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")


if __name__ == '__main__':
    main()
