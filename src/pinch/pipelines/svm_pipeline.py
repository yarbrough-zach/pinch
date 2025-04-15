#!/usr/bin/env python3

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
        trainer (SVMClassifier or None): The trained SVM model.
        clean_df (pd.DataFrame or None): Clean glitch triggers for training.
        dirty_df (pd.DataFrame or None): Dirty glitch triggers for scoring.
        scored_df (pd.DataFrame or None): Scored triggers after evaluation.
    """
    def __init__(
            self,
            clean_df=None,
            dirty_df=None,
            trainer=None,
            model_path=None,
            output_path=None,
    ):
        self.clean_df = clean_df
        self.dirty_df = dirty_df
        self.trainer = trainer
        self.model_path = model_path
        self.output_path = output_path
        self.scored_df = None

    def train(self, save_model=False):
        """
        Train a one-class SVM model on clean data.

        Optionally saves the trained model to a file.
        """
        if self.clean_df is None:
            raise ValueError("No clean dataframe provided for training")

        self.trainer = SVMClassifier.train_from_data(self.clean_df)

        if save_model:
            if not self.model_path:
                raise ValueError("No model_path specified to save the model")
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            self.trainer.save_model(self.model_path)

    def evaluate(self):
        """
        Score dirty triggers using the trained SVM model.

        Raises:
            ValueError: If no model is provided or trained.
        """
        if self.dirty_df is None:
            raise ValueError("No dirty dataframe provided for evaluation")

        if self.trainer is None:
            if not self.model_path:
                raise ValueError("No model speficied for evaluation")
            self.trainer = SVMClassifier.load_model(self.model_path)

        scored_df = self.trainer.evaluate(self.dirty_df)

        self.scored_df = scored_df

        return self.scored_df

    def save_scored_data(self):
        """
        Save scored dirty triggers to the specified output path.

        Raises:
            ValueError: If no scored data is available.
        """
        if self.scored_df is None:
            raise ValueError("No scored data available, did you forget to call evaluate()?")

        if not self.output_path:
            raise ValueError("No output_path specified to save scored data")

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        # FIXME probably shouldn't just be attrs, should be args
        self._write_scored_df(self.scored_df, self.output_path)

    @staticmethod
    def _load_trigger_file(path):
        return pd.read_csv(path)

    @staticmethod
    def _write_scored_df(df, path):
        df.to_csv(f"{path}/scored_df.csv", index=False)
